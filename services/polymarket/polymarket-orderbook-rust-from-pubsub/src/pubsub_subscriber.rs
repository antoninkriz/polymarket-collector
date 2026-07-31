//! Redis pub/sub source for the Polymarket orderbook pipeline.
//!
//! Subscribes to the channel published by `polymarket-orderbook-rust-pubsub`,
//! deserializes each payload back into an `Event` and forwards it into the
//! same mpsc that the ClickHouse sink reads from. Reconnects on error with
//! a configurable backoff.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tracing::{info, warn};

use polymarket_orderbook_rust::events::Event;

#[derive(Debug, Clone)]
pub struct PubSubSubscriberConfig {
    pub redis_url: String,
    pub channel: String,
    pub reconnect_delay: Duration,
}

#[derive(Default)]
pub struct SubscriberStats {
    pub events_received: AtomicU64,
    pub events_forwarded: AtomicU64,
    pub parse_failures: AtomicU64,
    pub reconnects: AtomicU64,
}

impl SubscriberStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

pub async fn run(
    cfg: PubSubSubscriberConfig,
    inbound_tx: mpsc::Sender<Event>,
    stats: Arc<SubscriberStats>,
) -> Result<()> {
    loop {
        match subscribe_once(&cfg, &inbound_tx, &stats).await {
            Ok(()) => info!(channel = %cfg.channel, "polymarket pubsub stream ended; reconnecting"),
            Err(error) => warn!(?error, channel = %cfg.channel, "polymarket pubsub subscriber error; reconnecting"),
        }
        stats.reconnects.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(cfg.reconnect_delay).await;
    }
}

async fn subscribe_once(
    cfg: &PubSubSubscriberConfig,
    inbound_tx: &mpsc::Sender<Event>,
    stats: &SubscriberStats,
) -> Result<()> {
    let client = redis::Client::open(cfg.redis_url.as_str())
        .with_context(|| format!("invalid REDIS_URL: {}", cfg.redis_url))?;
    let mut pubsub = client
        .get_async_pubsub()
        .await
        .context("open Redis pub/sub connection")?;
    pubsub
        .subscribe(&cfg.channel)
        .await
        .with_context(|| format!("subscribe to channel {}", cfg.channel))?;
    info!(channel = %cfg.channel, "polymarket pubsub subscriber connected");

    let mut stream = pubsub.on_message();
    while let Some(message) = stream.next().await {
        stats.events_received.fetch_add(1, Ordering::Relaxed);
        let payload: String = match message.get_payload() {
            Ok(payload) => payload,
            Err(error) => {
                stats.parse_failures.fetch_add(1, Ordering::Relaxed);
                warn!(?error, "polymarket pubsub payload decode failed");
                continue;
            }
        };
        let event = match serde_json::from_str::<Event>(&payload) {
            Ok(event) => event,
            Err(error) => {
                stats.parse_failures.fetch_add(1, Ordering::Relaxed);
                warn!(?error, payload_preview = preview(&payload), "polymarket pubsub JSON parse failed");
                continue;
            }
        };
        if inbound_tx.send(event).await.is_err() {
            anyhow::bail!("polymarket sink mpsc closed; subscriber exiting");
        }
        stats.events_forwarded.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

fn preview(payload: &str) -> String {
    const MAX: usize = 200;
    if payload.len() <= MAX {
        payload.to_string()
    } else {
        format!("{}…", &payload[..MAX])
    }
}
