//! Durable Redis Stream source for the v3 Polymarket pipeline.
//!
//! Entries remain pending until the ClickHouse sink confirms a successful
//! insert. On restart, the same consumer first drains its pending entries and
//! then switches to new entries. Replayed entries retain their collector
//! identity and are idempotent in the v3 `ReplacingMergeTree`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tracing::{info, warn};

use polymarket_orderbook_rust::record::EventRecord;
use polymarket_orderbook_rust::sink::SinkItem;

const READ_COUNT: usize = 1_000;
const READ_BLOCK_MS: usize = 1_000;

#[derive(Debug, Clone)]
pub struct PubSubSubscriberConfig {
    pub redis_url: String,
    pub stream: String,
    pub group: String,
    pub consumer: String,
    pub reconnect_delay: Duration,
}

#[derive(Default)]
pub struct SubscriberStats {
    pub events_received: AtomicU64,
    pub events_forwarded: AtomicU64,
    pub events_acked: AtomicU64,
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
    inbound_tx: mpsc::Sender<SinkItem>,
    mut ack_rx: mpsc::Receiver<Vec<String>>,
    stats: Arc<SubscriberStats>,
) -> Result<()> {
    let mut pending_acks = Vec::new();
    loop {
        match consume_once(
            &cfg,
            &inbound_tx,
            &mut ack_rx,
            &mut pending_acks,
            &stats,
        )
        .await
        {
            Ok(()) => info!(stream = %cfg.stream, "Redis event stream ended; reconnecting"),
            Err(error) => warn!(?error, stream = %cfg.stream, "Redis event stream error; reconnecting"),
        }
        stats.reconnects.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(cfg.reconnect_delay).await;
    }
}

async fn consume_once(
    cfg: &PubSubSubscriberConfig,
    inbound_tx: &mpsc::Sender<SinkItem>,
    ack_rx: &mut mpsc::Receiver<Vec<String>>,
    pending_acks: &mut Vec<String>,
    stats: &SubscriberStats,
) -> Result<()> {
    let client = redis::Client::open(cfg.redis_url.as_str())
        .with_context(|| format!("invalid REDIS_URL: {}", cfg.redis_url))?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("open Redis stream connection")?;
    ensure_consumer_group(&mut conn, cfg).await?;
    info!(
        stream = %cfg.stream,
        group = %cfg.group,
        consumer = %cfg.consumer,
        "Polymarket v3 stream consumer connected",
    );

    let options = StreamReadOptions::default()
        .group(&cfg.group, &cfg.consumer)
        .count(READ_COUNT)
        .block(READ_BLOCK_MS);
    let mut draining_pending = true;
    let mut last_pending_id = String::from("0");

    loop {
        while let Ok(ids) = ack_rx.try_recv() {
            pending_acks.extend(ids);
        }
        flush_acks(&mut conn, cfg, pending_acks, stats).await?;

        let read_id = if draining_pending {
            last_pending_id.as_str()
        } else {
            ">"
        };
        let reply: StreamReadReply = conn
            .xread_options(&[cfg.stream.as_str()], &[read_id], &options)
            .await
            .context("XREADGROUP Polymarket v3 events")?;

        let mut received = 0_usize;
        for stream in reply.keys {
            for entry in stream.ids {
                received += 1;
                if draining_pending {
                    last_pending_id.clone_from(&entry.id);
                }
                stats.events_received.fetch_add(1, Ordering::Relaxed);
                let payload: String = entry
                    .get("payload")
                    .ok_or_else(|| anyhow::anyhow!("stream entry {} has no payload", entry.id))?;
                let record = serde_json::from_str::<EventRecord>(&payload).map_err(|error| {
                    stats.parse_failures.fetch_add(1, Ordering::Relaxed);
                    anyhow::anyhow!(
                        "parse v3 stream entry {}: {}; payload={}",
                        entry.id,
                        error,
                        preview(&payload),
                    )
                })?;
                inbound_tx
                    .send(SinkItem {
                        record,
                        delivery_id: Some(entry.id),
                    })
                    .await
                    .map_err(|_| anyhow::anyhow!("ClickHouse sink channel closed"))?;
                stats.events_forwarded.fetch_add(1, Ordering::Relaxed);
            }
        }

        if draining_pending && received == 0 {
            draining_pending = false;
            info!(consumer = %cfg.consumer, "pending Redis entries drained");
        }
    }
}

async fn ensure_consumer_group(
    conn: &mut redis::aio::MultiplexedConnection,
    cfg: &PubSubSubscriberConfig,
) -> Result<()> {
    let result: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&cfg.stream)
        .arg(&cfg.group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    match result {
        Ok(()) => info!(group = %cfg.group, "created Redis event consumer group"),
        Err(error) if error.to_string().contains("BUSYGROUP") => {}
        Err(error) => return Err(error).context("create Redis event consumer group"),
    }
    Ok(())
}

async fn flush_acks(
    conn: &mut redis::aio::MultiplexedConnection,
    cfg: &PubSubSubscriberConfig,
    pending_acks: &mut Vec<String>,
    stats: &SubscriberStats,
) -> Result<()> {
    if pending_acks.is_empty() {
        return Ok(());
    }
    let acknowledged: i64 = redis::cmd("XACK")
        .arg(&cfg.stream)
        .arg(&cfg.group)
        .arg(pending_acks.as_slice())
        .query_async(conn)
        .await
        .context("XACK committed ClickHouse rows")?;
    stats
        .events_acked
        .fetch_add(acknowledged as u64, Ordering::Relaxed);
    pending_acks.clear();
    Ok(())
}

fn preview(payload: &str) -> String {
    const MAX: usize = 200;
    if payload.len() <= MAX {
        payload.to_string()
    } else {
        let mut end = MAX;
        while !payload.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &payload[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::preview;

    #[test]
    fn preview_does_not_split_a_utf8_character() {
        let payload = format!("{}é-tail", "a".repeat(199));
        assert_eq!(preview(&payload), format!("{}…", "a".repeat(199)));
    }
}
