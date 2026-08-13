//! Durable Redis Stream sink for v3 Polymarket records.
//!
//! Records are appended with `XADD`. Failed writes retain the batch and retry
//! with backoff. An uncertain Redis response can append a record twice, but
//! both copies carry the same collector `(message_id, row_index)` and the v3
//! ClickHouse table removes that transport retry by identity.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use tokio::sync::mpsc;
use tracing::{error, info};

use polymarket_orderbook_rust::record::EventRecord;

#[derive(Debug, Clone)]
pub struct PubSubSinkConfig {
    pub redis_url: String,
    pub stream: String,
    pub batch_max: usize,
    pub linger: Duration,
}

pub struct PubSubSink {
    cfg: PubSubSinkConfig,
    conn: ConnectionManager,
    total_published: u64,
    total_dropped: u64,
}

impl PubSubSink {
    pub async fn connect(cfg: PubSubSinkConfig) -> Result<Self> {
        let client = redis::Client::open(cfg.redis_url.as_str())
            .with_context(|| format!("invalid REDIS_URL: {}", cfg.redis_url))?;
        let conn = ConnectionManager::new(client)
            .await
            .context("connect Redis ConnectionManager")?;
        info!(stream = %cfg.stream, "polymarket durable stream sink connected");
        Ok(Self {
            cfg,
            conn,
            total_published: 0,
            total_dropped: 0,
        })
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<EventRecord>) -> Result<()> {
        let batch_max = self.cfg.batch_max;
        let linger = self.cfg.linger;
        let mut batch: Vec<EventRecord> = Vec::with_capacity(batch_max);

        loop {
            // Block for first event so we don't busy-loop when idle.
            let first = match rx.recv().await {
                Some(e) => e,
                None => break,
            };
            batch.push(first);

            // Drain up to batch_max more, bounded by linger.
            let deadline = Instant::now() + linger;
            let mut channel_closed = false;
            while batch.len() < batch_max {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Some(e)) => batch.push(e),
                    Ok(None) => {
                        channel_closed = true;
                        break;
                    }
                    Err(_) => break, // linger elapsed
                }
            }

            self.flush(&mut batch).await?;

            if channel_closed {
                break;
            }
        }

        info!(
            total_published = self.total_published,
            total_dropped = self.total_dropped,
            "Polymarket Redis stream sink shutting down",
        );
        Ok(())
    }

    async fn flush(&mut self, batch: &mut Vec<EventRecord>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let batch_size = batch.len();
        let payloads: Vec<String> = batch
            .iter()
            .map(serde_json::to_string)
            .collect::<std::result::Result<_, _>>()
            .context("serialize v3 stream batch")?;

        let mut retry_delay = Duration::from_millis(250);
        loop {
            let mut pipe = redis::pipe();
            for payload in &payloads {
                pipe.cmd("XADD")
                    .arg(&self.cfg.stream)
                    .arg("*")
                    .arg("payload")
                    .arg(payload)
                    .ignore();
            }
            let result: redis::RedisResult<()> = pipe.query_async(&mut self.conn).await;
            match result {
                Ok(()) => break,
                Err(error) => {
                error!(
                    %error,
                    batch_size,
                    retry_delay_ms = retry_delay.as_millis() as u64,
                    "Redis stream append failed; retaining batch for retry",
                );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
                }
            }
        }
        self.total_published += payloads.len() as u64;
        batch.clear();
        Ok(())
    }
}
