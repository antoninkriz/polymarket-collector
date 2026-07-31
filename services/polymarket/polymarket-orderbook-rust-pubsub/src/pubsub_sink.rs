//! Redis pub/sub sink for Polymarket events.
//!
//! Receives [`Event`]s on an mpsc channel and publishes each as a tagged
//! JSON payload on a single Redis pub/sub channel. All 4 event variants
//! (`book`, `price_change`, `last_trade_price`, `tick_size_change`) round-trip
//! through the wire so the downstream `polymarket-orderbook-rust-from-pubsub`
//! consumer can reconstruct the same `Event` enum and feed the existing
//! ClickHouse sink with full fidelity.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use tokio::sync::mpsc;
use tracing::{error, info};

use polymarket_orderbook_rust::events::Event;

#[derive(Debug, Clone)]
pub struct PubSubSinkConfig {
    pub redis_url: String,
    pub channel: String,
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
        info!(channel = %cfg.channel, "polymarket pubsub sink connected");
        Ok(Self {
            cfg,
            conn,
            total_published: 0,
            total_dropped: 0,
        })
    }

    pub async fn run(mut self, mut rx: mpsc::Receiver<Event>) -> Result<()> {
        let batch_max = self.cfg.batch_max;
        let linger = self.cfg.linger;
        let mut batch: Vec<Event> = Vec::with_capacity(batch_max);

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

            self.flush(&mut batch).await;

            if channel_closed {
                break;
            }
        }

        info!(
            total_published = self.total_published,
            total_dropped = self.total_dropped,
            "polymarket pubsub sink shutting down",
        );
        Ok(())
    }

    async fn flush(&mut self, batch: &mut Vec<Event>) {
        if batch.is_empty() {
            return;
        }
        let batch_size = batch.len();
        let mut pipe = redis::pipe();
        let mut serialized: u64 = 0;
        for event in batch.drain(..) {
            match serde_json::to_string(&event) {
                Ok(payload) => {
                    pipe.publish(&self.cfg.channel, payload).ignore();
                    serialized += 1;
                }
                Err(err) => {
                    self.total_dropped += 1;
                    error!(
                        error = %err,
                        total_dropped = self.total_dropped,
                        "polymarket pubsub event serialization failed",
                    );
                }
            }
        }
        if serialized == 0 {
            return;
        }
        let result: redis::RedisResult<()> = pipe.query_async(&mut self.conn).await;
        match result {
            Ok(()) => self.total_published += serialized,
            Err(e) => {
                self.total_dropped += serialized;
                error!(
                    error = %e,
                    batch_size,
                    total_dropped = self.total_dropped,
                    "polymarket pubsub pipelined publish failed",
                );
            }
        }
    }
}
