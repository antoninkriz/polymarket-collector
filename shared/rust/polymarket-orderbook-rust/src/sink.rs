//! Compact Polymarket v3 ClickHouse sink.
//!
//! The sink is a single actor that owns its buffer and HTTP client. It writes
//! the three-column raw event log, retries failed inserts without dropping the
//! batch, and returns Redis delivery IDs only after ClickHouse commits.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::Serialize;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::record::EventRecord;

/// One durable Redis Stream delivery awaiting a ClickHouse commit.
#[derive(Debug, Clone)]
pub struct SinkItem {
    pub record: EventRecord,
    pub delivery_id: String,
}

pub struct SinkConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
    pub batch_size: usize,
    pub flush_interval: Duration,
    /// Row-level TTL in minutes on `timestamp_received`. Zero disables it.
    pub ttl_minutes: u64,
}

pub struct Sink {
    cfg: SinkConfig,
    http: Client,
    buffer: Vec<SinkItem>,
    total_flushed: u64,
    total_failures: u64,
}

impl Sink {
    /// Connect to ClickHouse and ensure the compact v3 table exists.
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
            total_failures: 0,
        };
        sink.ensure_database().await?;
        sink.ensure_table().await?;

        info!(
            url = %sink.cfg.url,
            db = %sink.cfg.database,
            table = %sink.cfg.table,
            batch_size = sink.cfg.batch_size,
            flush_ms = sink.cfg.flush_interval.as_millis() as u64,
            "ClickHouse sink connected",
        );
        Ok(sink)
    }

    /// Consume records until the input closes, then flush and acknowledge all.
    pub async fn run(
        mut self,
        mut rx: mpsc::Receiver<SinkItem>,
        ack_tx: mpsc::Sender<Vec<String>>,
    ) -> Result<()> {
        let mut tick = tokio::time::interval(self.cfg.flush_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;

        loop {
            tokio::select! {
                item = rx.recv() => match item {
                    Some(item) => {
                        self.buffer.push(item);
                        if self.buffer.len() >= self.cfg.batch_size {
                            self.flush_and_ack(&ack_tx).await?;
                        }
                    }
                    None => break,
                },
                _ = tick.tick() => self.flush_and_ack(&ack_tx).await?,
            }
        }

        self.flush_and_ack(&ack_tx).await?;
        info!(
            total_flushed = self.total_flushed,
            total_failures = self.total_failures,
            "ClickHouse sink shut down cleanly",
        );
        Ok(())
    }

    async fn flush_and_ack(&mut self, ack_tx: &mpsc::Sender<Vec<String>>) -> Result<()> {
        let delivery_ids = self.flush().await?;
        if !delivery_ids.is_empty() {
            ack_tx
                .send(delivery_ids)
                .await
                .map_err(|_| anyhow!("Redis acknowledgement channel closed"))?;
        }
        Ok(())
    }

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
                Err(error) => {
                    self.total_failures += 1;
                    error!(
                        %error,
                        count,
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
            .map(|item| item.delivery_id.clone())
            .collect();
        self.buffer.clear();
        Ok(delivery_ids)
    }

    fn serialize_batch(&self) -> Result<Vec<u8>> {
        let mut body = Vec::with_capacity(self.buffer.len() * 256);
        for item in &self.buffer {
            let mut event = item.record.event.clone();
            event.strip_hash();
            let data = serde_json::to_string(&event).context("serialize v3 event")?;
            serde_json::to_writer(
                &mut body,
                &RawRow {
                    timestamp_received: item.record.timestamp_received_ns,
                    sequence: item.record.sequence,
                    data: &data,
                },
            )
            .context("serialize v3 row")?;
            body.push(b'\n');
        }
        Ok(body)
    }

    async fn send_insert(&self, body: Vec<u8>) -> Result<()> {
        let query = format!(
            "INSERT INTO {} (timestamp_received, sequence, data) FORMAT JSONEachRow",
            self.cfg.table,
        );
        let response = self
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
            .context("ClickHouse insert request")?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("ClickHouse insert failed: {status} {text}");
        }
        Ok(())
    }

    async fn ensure_database(&self) -> Result<()> {
        self.exec_no_db(&format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            self.cfg.database,
        ))
        .await
        .context("ensure database")?;
        info!(db = %self.cfg.database, "ensured database exists");
        Ok(())
    }

    async fn ensure_table(&self) -> Result<()> {
        let ttl_clause = if self.cfg.ttl_minutes > 0 {
            format!(
                "\nTTL timestamp_received + INTERVAL {} MINUTE",
                self.cfg.ttl_minutes,
            )
        } else {
            String::new()
        };
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                timestamp_received DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
                sequence           UInt64 CODEC(Delta, ZSTD(1)),
                data               String CODEC(ZSTD(1))
            )
            ENGINE = ReplacingMergeTree()
            PARTITION BY toStartOfHour(timestamp_received)
            ORDER BY sequence{ttl_clause}",
            self.cfg.table,
        );
        self.exec(&sql).await.context("ensure table")?;
        info!(
            table = %self.cfg.table,
            ttl_minutes = self.cfg.ttl_minutes,
            "ensured table exists",
        );
        Ok(())
    }

    async fn exec(&self, sql: &str) -> Result<()> {
        let response = self
            .http
            .post(&self.cfg.url)
            .basic_auth(&self.cfg.user, Some(&self.cfg.password))
            .query(&[("database", self.cfg.database.as_str())])
            .body(sql.to_owned())
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("ClickHouse statement failed: {status} {text}");
        }
        Ok(())
    }

    async fn exec_no_db(&self, sql: &str) -> Result<()> {
        let response = self
            .http
            .post(&self.cfg.url)
            .basic_auth(&self.cfg.user, Some(&self.cfg.password))
            .body(sql.to_owned())
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("ClickHouse statement failed: {status} {text}");
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct RawRow<'a> {
    timestamp_received: i64,
    sequence: u64,
    data: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{Event, Level};
    use crate::record::CollectorContext;
    use rust_decimal::Decimal;

    fn dec(value: &str) -> Decimal {
        value.parse().unwrap()
    }

    fn sink() -> Sink {
        Sink {
            cfg: SinkConfig {
                url: "http://example.invalid:8123".into(),
                user: "default".into(),
                password: String::new(),
                database: "default".into(),
                table: "test_table".into(),
                batch_size: 10,
                flush_interval: Duration::from_millis(500),
                ttl_minutes: 0,
            },
            http: Client::new(),
            buffer: Vec::new(),
            total_flushed: 0,
            total_failures: 0,
        }
    }

    fn item(event: Event, sequence: u64) -> SinkItem {
        let collector = CollectorContext::with_publisher_generation(42);
        let mut record = collector.record(event, 1_757_908_892_351_123_456);
        record.sequence += sequence;
        SinkItem {
            record,
            delivery_id: format!("{sequence}-0"),
        }
    }

    #[test]
    fn serializes_only_receive_time_sequence_and_event_json() {
        let mut sink = sink();
        sink.buffer.push(item(
            Event::LastTradePrice {
                market: "m".into(),
                asset_id: "a".into(),
                timestamp: "not-interpreted-by-the-raw-sink".into(),
                price: dec("0.42"),
                size: dec("75"),
                side: "BUY".into(),
                fee_rate_bps: "10".into(),
                transaction_hash: "0xtx".into(),
            },
            0,
        ));

        let body = sink.serialize_batch().unwrap();
        let row: serde_json::Value =
            serde_json::from_slice(body.strip_suffix(b"\n").unwrap()).unwrap();
        assert_eq!(row.as_object().unwrap().len(), 3);
        assert_eq!(row["timestamp_received"], 1_757_908_892_351_123_456_i64);
        assert_eq!(row["sequence"], 42_u64 << 48);
        let event: serde_json::Value = serde_json::from_str(row["data"].as_str().unwrap()).unwrap();
        assert_eq!(event["timestamp"], "not-interpreted-by-the-raw-sink");
        assert_eq!(event["transaction_hash"], "0xtx");
    }

    #[test]
    fn strips_non_unique_book_hash() {
        let mut sink = sink();
        sink.buffer.push(item(
            Event::Book {
                market: "m".into(),
                asset_id: "a".into(),
                timestamp: "1".into(),
                bids: vec![Level::new(dec("0.4"), dec("10"))],
                asks: Vec::new(),
                hash: Some("not-an-identity".into()),
            },
            0,
        ));

        let body = sink.serialize_batch().unwrap();
        let row: serde_json::Value =
            serde_json::from_slice(body.strip_suffix(b"\n").unwrap()).unwrap();
        let event: serde_json::Value = serde_json::from_str(row["data"].as_str().unwrap()).unwrap();
        assert!(event.get("hash").is_none());
    }

    #[test]
    fn identical_public_events_keep_distinct_sequences() {
        let event = Event::TickSizeChange {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            old_tick_size: dec("0.01"),
            new_tick_size: dec("0.001"),
        };
        let mut sink = sink();
        sink.buffer.push(item(event.clone(), 0));
        sink.buffer.push(item(event, 1));

        let body = sink.serialize_batch().unwrap();
        let rows = std::str::from_utf8(&body)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[1]["sequence"].as_u64(),
            rows[0]["sequence"].as_u64().map(|v| v + 1)
        );
    }
}
