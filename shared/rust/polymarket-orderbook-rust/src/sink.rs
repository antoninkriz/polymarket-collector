//! Compact Polymarket v3 ClickHouse sink.
//!
//! The sink owns the ClickHouse HTTP client and writes complete caller-owned
//! batches to the three-column raw event log. Failed inserts retry the same
//! serialized body so callers can retain delivery ownership until commit.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Serialize;
use tracing::{error, info};

use crate::record::EventRecord;

/// One durable Redis Stream delivery awaiting a ClickHouse commit.
#[derive(Debug)]
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
}

pub struct Sink {
    cfg: SinkConfig,
    http: Client,
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
            total_flushed: 0,
            total_failures: 0,
        };
        sink.ensure_database().await?;
        sink.ensure_table().await?;

        info!(
            url = %sink.cfg.url,
            db = %sink.cfg.database,
            table = %sink.cfg.table,
            "ClickHouse sink connected",
        );
        Ok(sink)
    }

    /// Insert one complete batch, retrying transient ClickHouse failures.
    pub async fn insert(&mut self, batch: &[SinkItem]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let count = batch.len();
        let body = Self::serialize_batch(batch).context("serialize ClickHouse batch")?;
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
        Ok(())
    }

    pub fn total_flushed(&self) -> u64 {
        self.total_flushed
    }

    pub fn total_failures(&self) -> u64 {
        self.total_failures
    }

    fn serialize_batch(batch: &[SinkItem]) -> Result<Vec<u8>> {
        let mut body = Vec::with_capacity(batch.len() * 256);
        for item in batch {
            let data = serde_json::to_string(&item.record.event).context("serialize v3 event")?;
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
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                timestamp_received DateTime64(9, 'UTC') CODEC(Delta, ZSTD(1)),
                sequence           UInt64 CODEC(Delta, ZSTD(1)),
                data               String CODEC(ZSTD(1))
            )
            ENGINE = ReplacingMergeTree()
            PARTITION BY toStartOfHour(timestamp_received)
            ORDER BY sequence",
            self.cfg.table,
        );
        self.exec(&sql).await.context("ensure table")?;
        info!(table = %self.cfg.table, "ensured table exists");
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
    use crate::events::Event;
    use crate::record::CollectorContext;
    use rust_decimal::Decimal;

    fn dec(value: &str) -> Decimal {
        value.parse().unwrap()
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
        let batch = [item(
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
        )];

        let body = Sink::serialize_batch(&batch).unwrap();
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
    fn identical_public_events_keep_distinct_sequences() {
        let event = Event::TickSizeChange {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            old_tick_size: dec("0.01"),
            new_tick_size: dec("0.001"),
        };
        let batch = [item(event.clone(), 0), item(event, 1)];

        let body = Sink::serialize_batch(&batch).unwrap();
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
