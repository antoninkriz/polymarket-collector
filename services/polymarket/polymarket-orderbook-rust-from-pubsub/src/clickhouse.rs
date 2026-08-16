//! ClickHouse storage for the raw Polymarket event stream.
//!
//! Failed inserts retry the same serialized batch, allowing the writer actor
//! to retain Redis delivery ownership until ClickHouse commits it.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Serialize;
use tracing::{error, info};

#[derive(Debug, Serialize)]
pub struct RawRow {
    pub timestamp_received: i64,
    pub sequence: u64,
    pub data: String,
}

pub struct ClickHouseConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
}

pub struct ClickHouseSink {
    cfg: ClickHouseConfig,
    http: Client,
}

impl ClickHouseSink {
    pub async fn connect(cfg: ClickHouseConfig) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("build reqwest client")?;

        let sink = Self { cfg, http };
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

    pub async fn insert<'a>(
        &mut self,
        rows: impl ExactSizeIterator<Item = &'a RawRow>,
    ) -> Result<()> {
        let count = rows.len();
        if count == 0 {
            return Ok(());
        }
        let body = Self::serialize_batch(rows).context("serialize ClickHouse batch")?;
        let mut retry_delay = Duration::from_secs(1);
        loop {
            match self.send_insert(body.clone()).await {
                Ok(()) => break,
                Err(error) => {
                    error!(
                        %error,
                        count,
                        retry_delay_ms = retry_delay.as_millis() as u64,
                        "ClickHouse insert failed; retaining batch for retry",
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
                }
            }
        }

        Ok(())
    }

    fn serialize_batch<'a>(rows: impl ExactSizeIterator<Item = &'a RawRow>) -> Result<Vec<u8>> {
        let mut body = Vec::with_capacity(rows.len() * 256);
        for row in rows {
            serde_json::to_writer(&mut body, row).context("serialize v3 row")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn row(sequence: u64, data: &str) -> RawRow {
        RawRow {
            timestamp_received: 1_757_908_892_351_123_456,
            sequence,
            data: data.into(),
        }
    }

    #[test]
    fn serializes_only_receive_time_sequence_and_event_json() {
        let batch = [row(
            42,
            r#"{"event_type":"last_trade_price","timestamp":"not-interpreted-by-the-raw-sink","transaction_hash":"0xtx"}"#,
        )];

        let body = ClickHouseSink::serialize_batch(batch.iter()).unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(body.strip_suffix(b"\n").unwrap()).unwrap();
        assert_eq!(value.as_object().unwrap().len(), 3);
        assert_eq!(value["timestamp_received"], 1_757_908_892_351_123_456_i64);
        assert_eq!(value["sequence"], 42_u64);
        let event: serde_json::Value =
            serde_json::from_str(value["data"].as_str().unwrap()).unwrap();
        assert_eq!(event["timestamp"], "not-interpreted-by-the-raw-sink");
        assert_eq!(event["transaction_hash"], "0xtx");
    }

    #[test]
    fn identical_public_events_keep_distinct_sequences() {
        let data =
            r#"{"event_type":"tick_size_change","old_tick_size":"0.01","new_tick_size":"0.001"}"#;
        let batch = [row(42, data), row(43, data)];

        let body = ClickHouseSink::serialize_batch(batch.iter()).unwrap();
        let rows = std::str::from_utf8(&body)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["sequence"], 42_u64);
        assert_eq!(rows[1]["sequence"], 43_u64);
        assert_eq!(rows[0]["data"], rows[1]["data"]);
    }
}
