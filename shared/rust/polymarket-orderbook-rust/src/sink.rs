//! ClickHouse sink, structured as a single actor task.
//!
//! Owns the in-memory buffer and the HTTP client. Inputs arrive over an
//! `mpsc::Receiver<SinkItem>`. The actor's main loop multiplexes the channel
//! against a flush ticker via `tokio::select!`:
//!
//! ```text
//!   loop {
//!     select! {
//!       event = rx.recv() => buffer.push(event); maybe_flush();
//!       _     = tick     => flush();
//!       else            => break;
//!     }
//!   }
//!   flush();  // drain on shutdown
//! ```
//!
//! Because there is exactly one task touching the buffer and the HTTP
//! client, no mutex is needed (the Python version uses one to serialize
//! its flush-loop and eager-flush callers).
//!
//! ## Schema modes
//!
//! The sink supports three on-disk schemas, selected via [`SinkSchema`]:
//!
//! - [`SinkSchema::Raw`] (default) — legacy single-`data`-column layout.
//!   One JSON blob per row. Matches the Python service byte-for-byte.
//! - [`SinkSchema::Typed`] — the "recommended" typed columnar schema from
//!   `docs/data-dump-optimizations.md`. Explodes each event into ~16
//!   strongly-typed columns with `Delta + ZSTD(3)` on the timestamps and
//!   `ORDER BY (market, asset_id, timestamp_received)` for compressible
//!   layout. ~62% smaller on disk than `Raw`.
//! - [`SinkSchema::V3`] — replayable schema with socket receipt timestamps,
//!   parent/child ordering, connection epochs, raw parent messages, nullable
//!   event fields, and collector-owned idempotency identity.
//!
//! Both modes use `INSERT ... FORMAT JSONEachRow` over HTTP.
//!
//! ## Raw schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS {table} (
//!     timestamp_received DateTime64(3) DEFAULT now64(3),
//!     timestamp          DateTime64(3),
//!     market             String,
//!     event_type         String,
//!     data               String CODEC(ZSTD(3))
//! ) ENGINE = MergeTree()
//! PARTITION BY toStartOfHour(timestamp_received)
//! ORDER BY (market, timestamp_received)
//! ```
//!
//! Each row carries the per-event JSON (`serde_json::to_string(&event)`)
//! in the `data` column, with the `hash` field stripped if
//! `EXCLUDE_HASH=true`. `timestamp` is sent as an integer interpreted at
//! the column's `DateTime64(3)` precision (milliseconds since epoch).
//!
//! ## Typed schema
//!
//! See [`ensure_table_typed`] for the `CREATE TABLE` statement. Columns
//! that don't apply to a given event type are emitted with their
//! type-default (`0` / `""` / `[]`) so that every row has a complete
//! tuple; per-event population is documented in the data-dump doc.
//!
//! ## Failure mode
//!
//! V3 insert failures are retried with backoff while the upstream durable
//! Redis stream remains unacknowledged. This deliberately applies
//! backpressure instead of turning an infrastructure failure into a silent
//! archive gap.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::ValueEnum;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::json;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::events::Event;
use crate::record::EventRecord;

/// On-disk schema mode for the ClickHouse sink.
///
/// See module docs for the `CREATE TABLE` definitions of each mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "lowercase")]
pub enum SinkSchema {
    /// Legacy single-`data`-column schema. One JSON blob per row.
    Raw,
    /// Recommended typed columnar schema (see docs/data-dump-optimizations.md).
    Typed,
    /// Replayable v3 schema. This is the production Polymarket format.
    V3,
}

impl SinkSchema {
    /// Short label for structured logging.
    pub fn label(self) -> &'static str {
        match self {
            SinkSchema::Raw => "raw",
            SinkSchema::Typed => "typed",
            SinkSchema::V3 => "v3",
        }
    }
}

/// One durable-stream delivery. `delivery_id` is acknowledged only after the
/// whole ClickHouse batch commits successfully.
#[derive(Debug, Clone)]
pub struct SinkItem {
    pub record: EventRecord,
    pub delivery_id: Option<String>,
}

impl From<Event> for SinkItem {
    fn from(event: Event) -> Self {
        Self {
            record: EventRecord::synthetic(event),
            delivery_id: None,
        }
    }
}

impl From<EventRecord> for SinkItem {
    fn from(record: EventRecord) -> Self {
        Self {
            record,
            delivery_id: None,
        }
    }
}

pub struct SinkConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
    pub drop_table_on_start: bool,
    pub exclude_hash: bool,
    pub batch_size: usize,
    pub flush_interval: Duration,
    /// Row-level TTL in minutes on `timestamp_received`. 0 = no TTL.
    pub ttl_minutes: u64,
    /// On-disk schema for the target table.
    pub schema: SinkSchema,
}

pub struct Sink {
    cfg: SinkConfig,
    http: Client,
    buffer: SinkBuffer,
    // Cumulative counters for data-gap tracking. Logged on shutdown.
    total_flushed: u64,
    total_dropped: u64,
    total_failures: u64,
}

#[derive(Default)]
struct SinkBuffer(Vec<SinkItem>);

impl SinkBuffer {
    fn push<T: Into<SinkItem>>(&mut self, item: T) {
        self.0.push(item.into());
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn iter(&self) -> std::slice::Iter<'_, SinkItem> {
        self.0.iter()
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

impl Sink {
    /// Create the HTTP client, ensure the database and table exist, return
    /// a ready-to-run sink.
    pub async fn connect(cfg: SinkConfig) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("build reqwest client")?;

        let sink = Self {
            cfg,
            http,
            buffer: SinkBuffer::default(),
            total_flushed: 0,
            total_dropped: 0,
            total_failures: 0,
        };

        sink.ensure_database().await?;
        if sink.cfg.drop_table_on_start {
            sink.drop_table().await?;
        }
        sink.ensure_table().await?;

        info!(
            url = %sink.cfg.url,
            db = %sink.cfg.database,
            table = %sink.cfg.table,
            schema = sink.cfg.schema.label(),
            batch_size = sink.cfg.batch_size,
            flush_ms = sink.cfg.flush_interval.as_millis() as u64,
            "ClickHouseSink connected",
        );

        Ok(sink)
    }

    /// Drive the sink as the consumer of an event channel. Returns when
    /// `rx` is closed (all senders dropped). Drains the buffer on exit.
    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<SinkItem>,
        ack_tx: Option<mpsc::Sender<Vec<String>>>,
    ) -> Result<()> {
        let mut tick = tokio::time::interval(self.cfg.flush_interval);
        // First tick fires immediately; skip it so the first flush happens
        // at +flush_interval, not at startup.
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;

        loop {
            tokio::select! {
                maybe_event = rx.recv() => {
                    match maybe_event {
                        Some(ev) => {
                            self.buffer.push(ev);
                            if self.buffer.len() >= self.cfg.batch_size {
                                self.flush_and_ack(ack_tx.as_ref()).await?;
                            }
                        }
                        None => break, // all senders dropped → drain & exit
                    }
                }
                _ = tick.tick() => {
                    self.flush_and_ack(ack_tx.as_ref()).await?;
                }
            }
        }

        // Drain on shutdown.
        self.flush_and_ack(ack_tx.as_ref()).await?;

        if self.total_dropped > 0 {
            warn!(
                total_dropped = self.total_dropped,
                total_failures = self.total_failures,
                total_flushed = self.total_flushed,
                "[DATA-GAP] ClickHouseSink shutting down with dropped events",
            );
        } else {
            info!(
                total_flushed = self.total_flushed,
                "ClickHouseSink shut down cleanly",
            );
        }

        Ok(())
    }

    async fn flush_and_ack(&mut self, ack_tx: Option<&mpsc::Sender<Vec<String>>>) -> Result<()> {
        let delivery_ids = self.flush().await?;
        if delivery_ids.is_empty() {
            return Ok(());
        }
        if let Some(ack_tx) = ack_tx {
            ack_tx
                .send(delivery_ids)
                .await
                .map_err(|_| anyhow!("Redis acknowledgement channel closed"))?;
        }
        Ok(())
    }

    // -- flush -----------------------------------------------------------

    async fn flush(&mut self) -> Result<Vec<String>> {
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }
        let count = self.buffer.len();

        let body = self
            .serialize_batch()
            .context("serialize ClickHouse batch")?;
        let mut retry_delay = Duration::from_secs(1);
        loop {
            match self.send_insert(body.clone()).await {
                Ok(()) => break,
                Err(e) => {
                    self.total_failures += 1;
                    error!(
                        error = %e, count,
                        total_failures = self.total_failures,
                        retry_delay_ms = retry_delay.as_millis() as u64,
                        "ClickHouse insert failed; retaining batch for retry",
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
                }
            }
        }

        self.total_flushed += count as u64;
        let delivery_ids = self
            .buffer
            .iter()
            .filter_map(|item| item.delivery_id.clone())
            .collect();
        self.buffer.clear();
        Ok(delivery_ids)
    }

    /// Serialize the buffer into a JSONEachRow-formatted body.
    /// One row per event, newline-separated. Returns an error only on
    /// timestamp parse failure or serde failure (extremely unlikely).
    fn serialize_batch(&self) -> Result<Vec<u8>> {
        let mut body = Vec::with_capacity(self.buffer.len() * 256);
        for item in self.buffer.iter() {
            let ev = &item.record.event;
            let timestamp_ms: i64 = ev
                .timestamp()
                .parse()
                .map_err(|e| anyhow!("invalid event timestamp {:?}: {}", ev.timestamp(), e))?;

            match self.cfg.schema {
                SinkSchema::Raw => self.write_raw_row(&mut body, ev, timestamp_ms)?,
                SinkSchema::Typed => self.write_typed_row(&mut body, ev, timestamp_ms)?,
                SinkSchema::V3 => self.write_v3_row(&mut body, item, timestamp_ms)?,
            }
            body.push(b'\n');
        }
        Ok(body)
    }

    fn write_raw_row(&self, body: &mut Vec<u8>, ev: &Event, timestamp_ms: i64) -> Result<()> {
        // serialize the event JSON (the value of the `data` column)
        let data_json = if self.cfg.exclude_hash {
            let mut cloned = ev.clone();
            cloned.strip_hash();
            serde_json::to_string(&cloned)
        } else {
            serde_json::to_string(ev)
        }
        .context("serialize event")?;

        let row = RawRow {
            timestamp: timestamp_ms,
            market: ev.market(),
            event_type: ev.kind(),
            data: &data_json,
        };
        serde_json::to_writer(body, &row).context("serialize row")?;
        Ok(())
    }

    fn write_typed_row(&self, body: &mut Vec<u8>, ev: &Event, timestamp_ms: i64) -> Result<()> {
        let value = typed_row_value(ev, timestamp_ms);
        serde_json::to_writer(body, &value).context("serialize typed row")?;
        Ok(())
    }

    fn write_v3_row(&self, body: &mut Vec<u8>, item: &SinkItem, timestamp_ms: i64) -> Result<()> {
        let value = v3_row_value(item, timestamp_ms);
        serde_json::to_writer(body, &value).context("serialize v3 row")?;
        Ok(())
    }

    async fn send_insert(&self, body: Vec<u8>) -> Result<()> {
        let query = match self.cfg.schema {
            SinkSchema::Raw => format!(
                "INSERT INTO {} (timestamp, market, event_type, data) FORMAT JSONEachRow",
                self.cfg.table
            ),
            SinkSchema::Typed => format!(
                "INSERT INTO {} (\
                    timestamp, market, event_type, asset_id, \
                    bids, asks, price, size, side, best_bid, best_ask, \
                    fee_rate_bps, transaction_hash, old_tick_size, new_tick_size\
                ) FORMAT JSONEachRow",
                self.cfg.table
            ),
            SinkSchema::V3 => format!(
                "INSERT INTO {} (\
                    schema_version, timestamp_received, timestamp, timestamp_raw, \
                    collector_session_id, collector_session_started_at, publisher_fence, \
                    connection_id, connection_epoch, frame_sequence, receive_sequence, \
                    message_id, message_index, message_count, row_index, row_count, \
                    transport_id, market, event_type, asset_id, hash, raw_message, \
                    bids, asks, price, size, side, best_bid, best_ask, \
                    fee_rate_bps, transaction_hash, old_tick_size, new_tick_size\
                ) FORMAT JSONEachRow",
                self.cfg.table
            ),
        };
        let resp = self
            .http
            .post(&self.cfg.url)
            .basic_auth(&self.cfg.user, Some(&self.cfg.password))
            .query(&[
                ("database", self.cfg.database.as_str()),
                ("query", query.as_str()),
            ])
            .body(body)
            .send()
            .await
            .context("clickhouse insert request")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("clickhouse insert failed: {} {}", status, text);
        }
        Ok(())
    }

    // -- DDL helpers -----------------------------------------------------

    async fn ensure_database(&self) -> Result<()> {
        let sql = format!("CREATE DATABASE IF NOT EXISTS {}", self.cfg.database);
        // Note: don't pass database= here; the DB may not exist yet.
        self.exec_no_db(&sql).await.context("ensure database")?;
        info!(db = %self.cfg.database, "ensured database exists");
        Ok(())
    }

    async fn ensure_table(&self) -> Result<()> {
        let ttl_clause = if self.cfg.ttl_minutes > 0 {
            format!(
                "\nTTL timestamp_received + INTERVAL {} MINUTE",
                self.cfg.ttl_minutes
            )
        } else {
            String::new()
        };
        let sql = match self.cfg.schema {
            SinkSchema::Raw => format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    timestamp_received DateTime64(3) DEFAULT now64(3),
                    timestamp          DateTime64(3),
                    market             String,
                    event_type         String,
                    data               String CODEC(ZSTD(3))
                )
                ENGINE = MergeTree()
                PARTITION BY toStartOfHour(timestamp_received)
                ORDER BY (market, timestamp_received){ttl_clause}",
                self.cfg.table
            ),
            SinkSchema::Typed => format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    timestamp_received DateTime64(3) DEFAULT now64(3)       CODEC(Delta, ZSTD(3)),
                    timestamp          DateTime64(3)                        CODEC(Delta, ZSTD(3)),
                    market             LowCardinality(FixedString(66))      CODEC(ZSTD(3)),
                    event_type         LowCardinality(String)               CODEC(ZSTD(3)),
                    asset_id           LowCardinality(String)               CODEC(ZSTD(3)),
                    bids               Array(Tuple(Decimal32(4), Decimal64(6))) CODEC(ZSTD(3)),
                    asks               Array(Tuple(Decimal32(4), Decimal64(6))) CODEC(ZSTD(3)),
                    price              Decimal32(4)                         CODEC(ZSTD(3)),
                    size               Decimal64(6)                         CODEC(ZSTD(3)),
                    side               LowCardinality(String)               CODEC(ZSTD(3)),
                    best_bid           Decimal32(4)                         CODEC(ZSTD(3)),
                    best_ask           Decimal32(4)                         CODEC(ZSTD(3)),
                    fee_rate_bps       UInt16                               CODEC(ZSTD(3)),
                    transaction_hash   String                               CODEC(ZSTD(3)),
                    old_tick_size      Decimal32(4)                         CODEC(ZSTD(3)),
                    new_tick_size      Decimal32(4)                         CODEC(ZSTD(3))
                )
                ENGINE = MergeTree()
                PARTITION BY toStartOfHour(timestamp_received)
                ORDER BY (market, asset_id, timestamp_received){ttl_clause}",
                self.cfg.table
            ),
            SinkSchema::V3 => format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    schema_version                 UInt8,
                    timestamp_received             DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
                    timestamp                      DateTime64(3, 'UTC') CODEC(Delta, ZSTD(3)),
                    timestamp_raw                  String CODEC(ZSTD(3)),
                    collector_session_id           UUID CODEC(ZSTD(3)),
                    collector_session_started_at   DateTime64(9, 'UTC') CODEC(Delta, ZSTD(3)),
                    publisher_fence                UInt64 CODEC(Delta, ZSTD(3)),
                    connection_id                  UInt32 CODEC(Delta, ZSTD(3)),
                    connection_epoch               UInt64 CODEC(Delta, ZSTD(3)),
                    frame_sequence                 UInt64 CODEC(Delta, ZSTD(3)),
                    receive_sequence               UInt64 CODEC(Delta, ZSTD(3)),
                    message_id                     UUID,
                    message_index                  UInt32,
                    message_count                  UInt32,
                    row_index                      UInt32,
                    row_count                      UInt32,
                    transport_id                   String CODEC(ZSTD(3)),
                    market                         LowCardinality(String) CODEC(ZSTD(3)),
                    event_type                     LowCardinality(String) CODEC(ZSTD(3)),
                    asset_id                       LowCardinality(String) CODEC(ZSTD(3)),
                    hash                           Nullable(String) CODEC(ZSTD(3)),
                    raw_message                    String CODEC(ZSTD(3)),
                    bids                           Array(Tuple(Decimal32(4), Decimal64(6))) CODEC(ZSTD(3)),
                    asks                           Array(Tuple(Decimal32(4), Decimal64(6))) CODEC(ZSTD(3)),
                    price                          Nullable(Decimal32(4)) CODEC(ZSTD(3)),
                    size                           Nullable(Decimal64(6)) CODEC(ZSTD(3)),
                    side                           LowCardinality(Nullable(String)) CODEC(ZSTD(3)),
                    best_bid                       Nullable(Decimal32(4)) CODEC(ZSTD(3)),
                    best_ask                       Nullable(Decimal32(4)) CODEC(ZSTD(3)),
                    fee_rate_bps                   Nullable(UInt32) CODEC(ZSTD(3)),
                    transaction_hash               Nullable(String) CODEC(ZSTD(3)),
                    old_tick_size                  Nullable(Decimal32(4)) CODEC(ZSTD(3)),
                    new_tick_size                  Nullable(Decimal32(4)) CODEC(ZSTD(3))
                )
                ENGINE = ReplacingMergeTree()
                PARTITION BY toStartOfHour(timestamp_received)
                ORDER BY (
                    market, asset_id, collector_session_id,
                    message_id, row_index, receive_sequence
                ){ttl_clause}",
                self.cfg.table
            ),
        };
        self.exec(&sql).await.context("ensure table")?;
        if self.cfg.schema == SinkSchema::V3 {
            // Keep an already-created v3 table forward-compatible while this
            // branch is rolled out incrementally. Historical rows receive the
            // neutral fence value 0.
            self.exec(&format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS \
                 publisher_fence UInt64 DEFAULT 0 CODEC(Delta, ZSTD(3)) \
                 AFTER collector_session_started_at",
                self.cfg.table,
            ))
            .await
            .context("ensure v3 publisher fence column")?;
        }
        info!(
            table = %self.cfg.table,
            schema = self.cfg.schema.label(),
            ttl_minutes = self.cfg.ttl_minutes,
            "ensured table exists",
        );
        Ok(())
    }

    async fn drop_table(&self) -> Result<()> {
        let sql = format!("DROP TABLE IF EXISTS {}", self.cfg.table);
        self.exec(&sql).await.context("drop table")?;
        info!(table = %self.cfg.table, "dropped table");
        Ok(())
    }

    /// Execute a statement against the configured database.
    async fn exec(&self, sql: &str) -> Result<()> {
        let resp = self
            .http
            .post(&self.cfg.url)
            .basic_auth(&self.cfg.user, Some(&self.cfg.password))
            .query(&[("database", self.cfg.database.as_str())])
            .body(sql.to_string())
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("clickhouse exec failed: {} {}", status, text);
        }
        Ok(())
    }

    /// Execute a statement without binding to a database (used for
    /// `CREATE DATABASE` before the database exists).
    async fn exec_no_db(&self, sql: &str) -> Result<()> {
        let resp = self
            .http
            .post(&self.cfg.url)
            .basic_auth(&self.cfg.user, Some(&self.cfg.password))
            .body(sql.to_string())
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("clickhouse exec failed: {} {}", status, text);
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct RawRow<'a> {
    timestamp: i64,
    market: &'a str,
    event_type: &'a str,
    /// Already-serialized JSON string. Serde re-escapes it so the value is
    /// stored as a string in ClickHouse's `String` column.
    data: &'a str,
}

/// Build a `serde_json::Value` shaped for the typed-schema `INSERT` column
/// list. Columns that don't apply to the given event type are filled with
/// their column-type default (`"0"` for Decimals, `""` for Strings, `[]`
/// for Arrays, `0` for UInt16). The column population per event type is
/// documented in `docs/data-dump-optimizations.md`.
fn typed_row_value(ev: &Event, timestamp_ms: i64) -> serde_json::Value {
    // Decimal columns in ClickHouse accept numeric strings in JSONEachRow
    // — CH parses them into the column's Decimal scale. Default is "0".
    const ZERO: &str = "0";
    let dec = |d: &Decimal| d.to_string();
    let opt = |d: &Option<Decimal>| d.as_ref().map(dec).unwrap_or_else(|| ZERO.to_string());

    match ev {
        Event::Book {
            market,
            asset_id,
            bids,
            asks,
            ..
        } => json!({
            "timestamp": timestamp_ms,
            "market": market,
            "event_type": "book",
            "asset_id": asset_id,
            "bids": bids,
            "asks": asks,
            "price": ZERO,
            "size": ZERO,
            "side": "",
            "best_bid": ZERO,
            "best_ask": ZERO,
            "fee_rate_bps": 0u16,
            "transaction_hash": "",
            "old_tick_size": ZERO,
            "new_tick_size": ZERO,
        }),
        Event::PriceChange {
            market,
            asset_id,
            price,
            size,
            side,
            best_bid,
            best_ask,
            ..
        } => json!({
            "timestamp": timestamp_ms,
            "market": market,
            "event_type": "price_change",
            "asset_id": asset_id,
            "bids": [],
            "asks": [],
            "price": dec(price),
            "size": dec(size),
            "side": side,
            "best_bid": opt(best_bid),
            "best_ask": opt(best_ask),
            "fee_rate_bps": 0u16,
            "transaction_hash": "",
            "old_tick_size": ZERO,
            "new_tick_size": ZERO,
        }),
        Event::LastTradePrice {
            market,
            asset_id,
            price,
            size,
            side,
            fee_rate_bps,
            transaction_hash,
            ..
        } => json!({
            "timestamp": timestamp_ms,
            "market": market,
            "event_type": "last_trade_price",
            "asset_id": asset_id,
            "bids": [],
            "asks": [],
            "price": dec(price),
            "size": dec(size),
            "side": side,
            "best_bid": ZERO,
            "best_ask": ZERO,
            // Wire value is a numeric string; fall back to 0 on parse failure
            // so we don't drop the whole batch for one bad row. Observed
            // values are always "0" or "1000".
            "fee_rate_bps": fee_rate_bps.parse::<u16>().unwrap_or(0),
            "transaction_hash": transaction_hash,
            "old_tick_size": ZERO,
            "new_tick_size": ZERO,
        }),
        Event::TickSizeChange {
            market,
            asset_id,
            old_tick_size,
            new_tick_size,
            ..
        } => json!({
            "timestamp": timestamp_ms,
            "market": market,
            "event_type": "tick_size_change",
            "asset_id": asset_id,
            "bids": [],
            "asks": [],
            "price": ZERO,
            "size": ZERO,
            "side": "",
            "best_bid": ZERO,
            "best_ask": ZERO,
            "fee_rate_bps": 0u16,
            "transaction_hash": "",
            "old_tick_size": dec(old_tick_size),
            "new_tick_size": dec(new_tick_size),
        }),
    }
}

/// Build one replayable v3 row. Unlike the legacy typed schema, scalar fields
/// that do not belong to an event are NULL rather than overloaded zero values.
fn v3_row_value(item: &SinkItem, timestamp_ms: i64) -> serde_json::Value {
    let record = &item.record;
    let event = &record.event;
    let mut row = json!({
        "schema_version": record.schema_version,
        "timestamp_received": record.timestamp_received_ns,
        "timestamp": timestamp_ms,
        "timestamp_raw": event.timestamp(),
        "collector_session_id": record.collector_session_id.to_string(),
        "collector_session_started_at": record.collector_session_started_at_ns,
        "publisher_fence": record.publisher_fence,
        "connection_id": record.connection_id,
        "connection_epoch": record.connection_epoch,
        "frame_sequence": record.frame_sequence,
        "receive_sequence": record.receive_sequence,
        "message_id": record.message_id.to_string(),
        "message_index": record.message_index,
        "message_count": record.message_count,
        "row_index": record.row_index,
        "row_count": record.row_count,
        "transport_id": item.delivery_id.as_deref().unwrap_or(""),
        "market": event.market(),
        "event_type": event.kind(),
        "asset_id": event.asset_id(),
        "hash": serde_json::Value::Null,
        "raw_message": record.raw_message,
        "bids": [],
        "asks": [],
        "price": serde_json::Value::Null,
        "size": serde_json::Value::Null,
        "side": serde_json::Value::Null,
        "best_bid": serde_json::Value::Null,
        "best_ask": serde_json::Value::Null,
        "fee_rate_bps": serde_json::Value::Null,
        "transaction_hash": serde_json::Value::Null,
        "old_tick_size": serde_json::Value::Null,
        "new_tick_size": serde_json::Value::Null,
    });
    let object = row.as_object_mut().expect("v3 row is an object");
    let decimal = |value: &Decimal| serde_json::Value::String(value.to_string());
    let optional_decimal = |value: &Option<Decimal>| {
        value
            .as_ref()
            .map(decimal)
            .unwrap_or(serde_json::Value::Null)
    };

    match event {
        Event::Book {
            bids, asks, hash, ..
        } => {
            object.insert(
                "bids".into(),
                serde_json::to_value(bids).expect("serialize bids"),
            );
            object.insert(
                "asks".into(),
                serde_json::to_value(asks).expect("serialize asks"),
            );
            object.insert(
                "hash".into(),
                hash.as_ref()
                    .map(|value| serde_json::Value::String(value.clone()))
                    .unwrap_or(serde_json::Value::Null),
            );
        }
        Event::PriceChange {
            price,
            size,
            side,
            best_bid,
            best_ask,
            hash,
            ..
        } => {
            object.insert("price".into(), decimal(price));
            object.insert("size".into(), decimal(size));
            object.insert("side".into(), serde_json::Value::String(side.clone()));
            object.insert("best_bid".into(), optional_decimal(best_bid));
            object.insert("best_ask".into(), optional_decimal(best_ask));
            object.insert(
                "hash".into(),
                hash.as_ref()
                    .map(|value| serde_json::Value::String(value.clone()))
                    .unwrap_or(serde_json::Value::Null),
            );
        }
        Event::LastTradePrice {
            price,
            size,
            side,
            fee_rate_bps,
            transaction_hash,
            ..
        } => {
            object.insert("price".into(), decimal(price));
            object.insert("size".into(), decimal(size));
            object.insert("side".into(), serde_json::Value::String(side.clone()));
            object.insert(
                "fee_rate_bps".into(),
                fee_rate_bps
                    .parse::<u32>()
                    .map(serde_json::Value::from)
                    .unwrap_or(serde_json::Value::Null),
            );
            object.insert(
                "transaction_hash".into(),
                serde_json::Value::String(transaction_hash.clone()),
            );
        }
        Event::TickSizeChange {
            old_tick_size,
            new_tick_size,
            ..
        } => {
            object.insert("old_tick_size".into(), decimal(old_tick_size));
            object.insert("new_tick_size".into(), decimal(new_tick_size));
        }
    }
    row
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::Level;
    use crate::record::CollectorContext;
    use rust_decimal::Decimal;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn cfg() -> SinkConfig {
        SinkConfig {
            url: "http://example.invalid:8123".into(),
            user: "default".into(),
            password: String::new(),
            database: "default".into(),
            table: "test_table".into(),
            drop_table_on_start: false,
            exclude_hash: false,
            batch_size: 10,
            flush_interval: Duration::from_millis(500),
            ttl_minutes: 0,
            schema: SinkSchema::Raw,
        }
    }

    fn empty_sink(cfg: SinkConfig) -> Sink {
        Sink {
            cfg,
            http: Client::new(),
            buffer: SinkBuffer::default(),
            total_flushed: 0,
            total_dropped: 0,
            total_failures: 0,
        }
    }

    #[test]
    fn serialize_batch_emits_jsoneachrow_format() {
        let mut sink = empty_sink(cfg());
        sink.buffer.push(Event::Book {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1757908892351".into(),
            bids: vec![Level::new(dec("0.41"), dec("100"))],
            asks: vec![Level::new(dec("0.42"), dec("200"))],
            hash: Some("h".into()),
        });
        sink.buffer.push(Event::TickSizeChange {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1757908892999".into(),
            old_tick_size: dec("0.01"),
            new_tick_size: dec("0.001"),
        });

        let body = sink.serialize_batch().unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        let lines: Vec<&str> = body_str.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 2, "expected one line per event");

        // Each line is a valid standalone JSON object with our 4 columns.
        for line in &lines {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(v["timestamp"].is_i64(), "timestamp must be integer ms");
            assert!(v["market"].is_string());
            assert!(v["event_type"].is_string());
            assert!(v["data"].is_string(), "data must be a JSON string");
        }

        // Row 1: book with ms-epoch parsed correctly.
        let row1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(row1["timestamp"], 1757908892351_i64);
        assert_eq!(row1["event_type"], "book");
        // Inner data round-trips back to the bids/asks shape we serialized.
        let inner: serde_json::Value =
            serde_json::from_str(row1["data"].as_str().unwrap()).unwrap();
        assert_eq!(inner["bids"], serde_json::json!([["0.41", "100"]]));
        assert_eq!(inner["hash"], "h");
    }

    #[test]
    fn serialize_batch_excludes_hash_when_configured() {
        let mut c = cfg();
        c.exclude_hash = true;
        let mut sink = empty_sink(c);
        sink.buffer.push(Event::Book {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1".into(),
            bids: vec![],
            asks: vec![],
            hash: Some("h".into()),
        });
        let body = sink.serialize_batch().unwrap();
        let body_str = std::str::from_utf8(&body).unwrap();
        let row: serde_json::Value = serde_json::from_str(body_str.trim_end()).unwrap();
        let inner: serde_json::Value = serde_json::from_str(row["data"].as_str().unwrap()).unwrap();
        assert!(inner.get("hash").is_none(), "hash must be stripped");
    }

    #[test]
    fn serialize_batch_rejects_invalid_timestamp() {
        let mut sink = empty_sink(cfg());
        sink.buffer.push(Event::TickSizeChange {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "not-a-number".into(),
            old_tick_size: dec("0.01"),
            new_tick_size: dec("0.001"),
        });
        let err = sink.serialize_batch().unwrap_err().to_string();
        assert!(err.contains("invalid event timestamp"), "got: {err}");
    }

    // -- Typed schema ----------------------------------------------------

    fn typed_cfg() -> SinkConfig {
        let mut c = cfg();
        c.schema = SinkSchema::Typed;
        c
    }

    fn v3_cfg() -> SinkConfig {
        let mut cfg = cfg();
        cfg.schema = SinkSchema::V3;
        cfg
    }

    fn v3_item(event: Event, row_index: u32, row_count: u32) -> SinkItem {
        let collector = CollectorContext::with_publisher_fence(42);
        SinkItem {
            record: collector
                .next_message(4, 2, 9, 0, 1, 1_757_908_892_351_123_456)
                .record(
                    event,
                    row_index,
                    row_count,
                    "{\"server\":\"payload\"}".into(),
                ),
            delivery_id: Some("1757908892351-0".into()),
        }
    }

    /// Parse every line in a serialized typed-schema batch into JSON `Value`s.
    fn typed_rows(sink: &Sink) -> Vec<serde_json::Value> {
        let body = sink.serialize_batch().unwrap();
        std::str::from_utf8(&body)
            .unwrap()
            .trim_end()
            .split('\n')
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn typed_book_emits_all_columns_with_depth_populated() {
        let mut sink = empty_sink(typed_cfg());
        sink.buffer.push(Event::Book {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1757908892351".into(),
            bids: vec![Level::new(dec("0.41"), dec("100"))],
            asks: vec![Level::new(dec("0.42"), dec("200"))],
            hash: Some("h".into()),
        });
        let row = &typed_rows(&sink)[0];
        assert_eq!(row["event_type"], "book");
        assert_eq!(row["timestamp"], 1757908892351_i64);
        assert_eq!(row["market"], "0xm");
        assert_eq!(row["asset_id"], "a1");
        // bids/asks as Array(Tuple(...)) → JSONEachRow shape [[price, size]].
        assert_eq!(row["bids"], serde_json::json!([["0.41", "100"]]));
        assert_eq!(row["asks"], serde_json::json!([["0.42", "200"]]));
        // Non-book columns default to type zero.
        assert_eq!(row["price"], "0");
        assert_eq!(row["size"], "0");
        assert_eq!(row["side"], "");
        assert_eq!(row["best_bid"], "0");
        assert_eq!(row["best_ask"], "0");
        assert_eq!(row["fee_rate_bps"], 0);
        assert_eq!(row["transaction_hash"], "");
        assert_eq!(row["old_tick_size"], "0");
        assert_eq!(row["new_tick_size"], "0");
    }

    #[test]
    fn typed_price_change_populates_price_size_side_and_best_quotes() {
        let mut sink = empty_sink(typed_cfg());
        sink.buffer.push(Event::PriceChange {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1000".into(),
            best_bid: Some(dec("0.40")),
            best_ask: Some(dec("0.42")),
            hash: Some("h".into()),
            price: dec("0.41"),
            size: dec("100"),
            side: "BUY".into(),
        });
        let row = &typed_rows(&sink)[0];
        assert_eq!(row["event_type"], "price_change");
        assert_eq!(row["price"], "0.41");
        assert_eq!(row["size"], "100");
        assert_eq!(row["side"], "BUY");
        assert_eq!(row["best_bid"], "0.40");
        assert_eq!(row["best_ask"], "0.42");
        // Non-applicable columns default.
        assert_eq!(row["bids"], serde_json::json!([]));
        assert_eq!(row["asks"], serde_json::json!([]));
        assert_eq!(row["transaction_hash"], "");
        assert_eq!(row["fee_rate_bps"], 0);
        assert_eq!(row["old_tick_size"], "0");
        assert_eq!(row["new_tick_size"], "0");
    }

    #[test]
    fn typed_price_change_defaults_missing_best_quotes_to_zero() {
        let mut sink = empty_sink(typed_cfg());
        sink.buffer.push(Event::PriceChange {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            best_bid: None,
            best_ask: None,
            hash: None,
            price: dec("0.5"),
            size: dec("10"),
            side: "SELL".into(),
        });
        let row = &typed_rows(&sink)[0];
        assert_eq!(row["best_bid"], "0");
        assert_eq!(row["best_ask"], "0");
    }

    #[test]
    fn typed_last_trade_price_populates_trade_columns() {
        let mut sink = empty_sink(typed_cfg());
        sink.buffer.push(Event::LastTradePrice {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1".into(),
            price: dec("0.42"),
            size: dec("75"),
            side: "BUY".into(),
            fee_rate_bps: "1000".into(),
            transaction_hash: "0xtx".into(),
        });
        let row = &typed_rows(&sink)[0];
        assert_eq!(row["event_type"], "last_trade_price");
        assert_eq!(row["price"], "0.42");
        assert_eq!(row["size"], "75");
        assert_eq!(row["side"], "BUY");
        assert_eq!(row["fee_rate_bps"], 1000);
        assert_eq!(row["transaction_hash"], "0xtx");
        // Non-applicable columns default.
        assert_eq!(row["best_bid"], "0");
        assert_eq!(row["best_ask"], "0");
        assert_eq!(row["bids"], serde_json::json!([]));
    }

    #[test]
    fn typed_last_trade_price_parses_garbage_fee_rate_as_zero() {
        let mut sink = empty_sink(typed_cfg());
        sink.buffer.push(Event::LastTradePrice {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            price: dec("0.1"),
            size: dec("1"),
            side: "BUY".into(),
            fee_rate_bps: "not-a-number".into(),
            transaction_hash: "tx".into(),
        });
        let row = &typed_rows(&sink)[0];
        assert_eq!(row["fee_rate_bps"], 0);
    }

    #[test]
    fn typed_tick_size_change_populates_tick_columns() {
        let mut sink = empty_sink(typed_cfg());
        sink.buffer.push(Event::TickSizeChange {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1".into(),
            old_tick_size: dec("0.01"),
            new_tick_size: dec("0.001"),
        });
        let row = &typed_rows(&sink)[0];
        assert_eq!(row["event_type"], "tick_size_change");
        assert_eq!(row["old_tick_size"], "0.01");
        assert_eq!(row["new_tick_size"], "0.001");
        assert_eq!(row["price"], "0");
        assert_eq!(row["side"], "");
        assert_eq!(row["bids"], serde_json::json!([]));
    }

    #[test]
    fn typed_schema_value_from_enum_label() {
        use clap::ValueEnum;
        let parsed = SinkSchema::from_str("typed", false).unwrap();
        assert_eq!(parsed, SinkSchema::Typed);
        let parsed = SinkSchema::from_str("raw", false).unwrap();
        assert_eq!(parsed, SinkSchema::Raw);
        let parsed = SinkSchema::from_str("v3", false).unwrap();
        assert_eq!(parsed, SinkSchema::V3);
    }

    #[test]
    fn v3_trade_keeps_receive_order_and_nullable_shape() {
        let mut sink = empty_sink(v3_cfg());
        sink.buffer.push(v3_item(
            Event::LastTradePrice {
                market: "m".into(),
                asset_id: "a".into(),
                timestamp: "1757908892351".into(),
                price: dec("0.42"),
                size: dec("75"),
                side: "BUY".into(),
                fee_rate_bps: "10".into(),
                transaction_hash: "0xtx".into(),
            },
            0,
            1,
        ));

        let body = sink.serialize_batch().unwrap();
        let row: serde_json::Value =
            serde_json::from_slice(body.strip_suffix(b"\n").unwrap()).unwrap();
        assert_eq!(row["schema_version"], 3);
        assert_eq!(row["publisher_fence"], 42);
        assert_eq!(row["timestamp_received"], 1_757_908_892_351_123_456_i64);
        assert_eq!(row["connection_epoch"], 2);
        assert_eq!(row["frame_sequence"], 9);
        assert_eq!(row["row_index"], 0);
        assert_eq!(row["row_count"], 1);
        assert_eq!(row["transport_id"], "1757908892351-0");
        assert_eq!(row["raw_message"], "{\"server\":\"payload\"}");
        assert_eq!(row["price"], "0.42");
        assert_eq!(row["transaction_hash"], "0xtx");
        assert!(row["best_bid"].is_null());
        assert!(row["hash"].is_null());
    }
}
