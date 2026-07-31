//! ClickHouse sink, structured as a single actor task.
//!
//! Owns the in-memory buffer and the HTTP client. Inputs arrive over an
//! `mpsc::Receiver<Event>`. The actor's main loop multiplexes the channel
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
//! The sink supports two on-disk schemas, selected via [`SinkSchema`]:
//!
//! - [`SinkSchema::Raw`] (default) — legacy single-`data`-column layout.
//!   One JSON blob per row. Matches the Python service byte-for-byte.
//! - [`SinkSchema::Typed`] — the "recommended" typed columnar schema from
//!   `docs/data-dump-optimizations.md`. Explodes each event into ~16
//!   strongly-typed columns with `Delta + ZSTD(3)` on the timestamps and
//!   `ORDER BY (market, asset_id, timestamp_received)` for compressible
//!   layout. ~62% smaller on disk than `Raw`.
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
//! Insert failures are non-fatal: we log them with a `[DATA-GAP]` marker,
//! increment `total_dropped` and `total_failures`, and continue. Matches
//! the Python sink's behaviour.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::ValueEnum;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Serialize;
use tokio::sync::mpsc;
use serde_json::json;
use tracing::{error, info, warn};

use crate::events::Event;

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
}

impl SinkSchema {
    /// Short label for structured logging.
    pub fn label(self) -> &'static str {
        match self {
            SinkSchema::Raw => "raw",
            SinkSchema::Typed => "typed",
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
    buffer: Vec<Event>,
    // Cumulative counters for data-gap tracking. Logged on shutdown.
    total_flushed: u64,
    total_dropped: u64,
    total_failures: u64,
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
            buffer: Vec::new(),
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
    pub async fn run(mut self, mut rx: mpsc::Receiver<Event>) -> Result<()> {
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
                                self.flush().await;
                            }
                        }
                        None => break, // all senders dropped → drain & exit
                    }
                }
                _ = tick.tick() => {
                    self.flush().await;
                }
            }
        }

        // Drain on shutdown.
        self.flush().await;

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

    fn emit_data_gap_grafana(&self, reason: &str, error: &str, batch_size: u64) {
        let total = self.total_flushed + self.total_dropped;
        let drop_rate_pct = if total > 0 {
            (self.total_dropped as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        error!(
            grafana = true,
            event = "data_gap",
            meta = %json!({
                "reason": reason,
                "error": error,
                "batch_size": batch_size,
                "total_dropped": self.total_dropped,
                "total_failures": self.total_failures,
                "total_flushed": self.total_flushed,
                "drop_rate_pct": format!("{:.2}", drop_rate_pct),
            }),
            "data_gap",
        );
    }

    // -- flush -----------------------------------------------------------

    async fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let count = self.buffer.len();

        let body = match self.serialize_batch() {
            Ok(b) => b,
            Err(e) => {
                self.total_failures += 1;
                self.total_dropped += count as u64;
                error!(
                    error = %e, count,
                    total_dropped = self.total_dropped,
                    total_failures = self.total_failures,
                    "[DATA-GAP] failed to serialize batch",
                );
                self.emit_data_gap_grafana("serialize_failed", &e.to_string(), count as u64);
                self.buffer.clear();
                return;
            }
        };

        match self.send_insert(body).await {
            Ok(()) => {
                self.total_flushed += count as u64;
            }
            Err(e) => {
                self.total_failures += 1;
                self.total_dropped += count as u64;
                error!(
                    error = %e, count,
                    total_dropped = self.total_dropped,
                    total_failures = self.total_failures,
                    "[DATA-GAP] failed to flush batch to ClickHouse",
                );
                self.emit_data_gap_grafana("insert_failed", &e.to_string(), count as u64);
            }
        }
        self.buffer.clear();
    }

    /// Serialize the buffer into a JSONEachRow-formatted body.
    /// One row per event, newline-separated. Returns an error only on
    /// timestamp parse failure or serde failure (extremely unlikely).
    fn serialize_batch(&self) -> Result<Vec<u8>> {
        let mut body = Vec::with_capacity(self.buffer.len() * 256);
        for ev in &self.buffer {
            let timestamp_ms: i64 = ev
                .timestamp()
                .parse()
                .map_err(|e| anyhow!("invalid event timestamp {:?}: {}", ev.timestamp(), e))?;

            match self.cfg.schema {
                SinkSchema::Raw => self.write_raw_row(&mut body, ev, timestamp_ms)?,
                SinkSchema::Typed => self.write_typed_row(&mut body, ev, timestamp_ms)?,
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
        };
        let resp = self
            .http
            .post(&self.cfg.url)
            .basic_auth(&self.cfg.user, Some(&self.cfg.password))
            .query(&[("database", self.cfg.database.as_str()), ("query", query.as_str())])
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
        };
        self.exec(&sql).await.context("ensure table")?;
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
        Event::Book { market, asset_id, bids, asks, .. } => json!({
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
            market, asset_id, price, size, side, best_bid, best_ask, ..
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
            market, asset_id, price, size, side, fee_rate_bps, transaction_hash, ..
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
            market, asset_id, old_tick_size, new_tick_size, ..
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::Level;
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
            buffer: Vec::new(),
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
        let inner: serde_json::Value = serde_json::from_str(row1["data"].as_str().unwrap()).unwrap();
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
    }
}
