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
use redis::{AsyncCommands, AsyncConnectionConfig};
use tokio::sync::mpsc;
use tracing::{info, warn};

use polymarket_orderbook_rust::record::EventRecord;
use polymarket_orderbook_rust::sink::SinkItem;

const READ_COUNT: usize = 1_000;
const READ_BLOCK_MS: usize = 1_000;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);

// XACK and XDEL must be one operation: after ClickHouse commits, either the
// entry remains pending and replayable or it is both acknowledged and removed.
// Deletion is permitted only when this is the stream's sole consumer group;
// otherwise XDEL would silently destroy data still needed by another group.
const ACK_AND_DELETE_EXCLUSIVE_SCRIPT: &str = r#"
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
if #groups ~= 1 then
  return redis.error_reply('STREAM_GROUP_OWNERSHIP_MISMATCH expected exactly one group')
end

local group_name = nil
for i = 1, #groups[1], 2 do
  if groups[1][i] == 'name' then
    group_name = groups[1][i + 1]
    break
  end
end
if group_name ~= ARGV[1] then
  return redis.error_reply('STREAM_GROUP_OWNERSHIP_MISMATCH configured group is not sole owner')
end

local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], unpack(ARGV, 2))
local deleted = redis.call('XDEL', KEYS[1], unpack(ARGV, 2))
return {acknowledged, deleted}
"#;

#[derive(Debug, Clone)]
pub struct PubSubSubscriberConfig {
    pub redis_url: String,
    pub stream: String,
    pub group: String,
    pub consumer: String,
    pub delete_acked_entries: bool,
    pub reconnect_delay: Duration,
}

#[derive(Default)]
pub struct SubscriberStats {
    pub events_received: AtomicU64,
    pub events_forwarded: AtomicU64,
    pub events_acked: AtomicU64,
    pub events_deleted: AtomicU64,
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
        match consume_once(&cfg, &inbound_tx, &mut ack_rx, &mut pending_acks, &stats).await {
            Ok(()) => info!(stream = %cfg.stream, "Redis event stream ended; reconnecting"),
            Err(error) => {
                warn!(?error, stream = %cfg.stream, "Redis event stream error; reconnecting")
            }
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
    // redis-rs 1.5 defaults to a 500 ms response timeout, which is shorter
    // than this consumer's intentional one-second blocking stream read.
    let connection_config =
        AsyncConnectionConfig::new().set_response_timeout(Some(RESPONSE_TIMEOUT));
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&connection_config)
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
                        delivery_id: entry.id,
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
    let (acknowledged, deleted) = if cfg.delete_acked_entries {
        redis::Script::new(ACK_AND_DELETE_EXCLUSIVE_SCRIPT)
            .key(&cfg.stream)
            .arg(&cfg.group)
            .arg(pending_acks.as_slice())
            .invoke_async::<(i64, i64)>(conn)
            .await
            .with_context(|| {
                format!(
                    "atomically XACK/XDEL committed rows; {} must be the stream's only consumer group",
                    cfg.group,
                )
            })?
    } else {
        let acknowledged: i64 = redis::cmd("XACK")
            .arg(&cfg.stream)
            .arg(&cfg.group)
            .arg(pending_acks.as_slice())
            .query_async(conn)
            .await
            .context("XACK committed ClickHouse rows")?;
        (acknowledged, 0)
    };
    stats
        .events_acked
        .fetch_add(acknowledged as u64, Ordering::Relaxed);
    stats
        .events_deleted
        .fetch_add(deleted as u64, Ordering::Relaxed);
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
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use anyhow::Result;

    use super::{
        ensure_consumer_group, flush_acks, preview, PubSubSubscriberConfig, SubscriberStats,
    };

    const TEST_REDIS_URL: &str = "redis://localhost:16380";

    #[test]
    fn preview_does_not_split_a_utf8_character() {
        let payload = format!("{}é-tail", "a".repeat(199));
        assert_eq!(preview(&payload), format!("{}…", "a".repeat(199)));
    }

    #[tokio::test]
    #[ignore]
    async fn committed_cleanup_requires_the_only_consumer_group() -> Result<()> {
        let suffix = format!(
            "{}:{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        );
        let cfg = PubSubSubscriberConfig {
            redis_url: TEST_REDIS_URL.into(),
            stream: format!("test:polymarket:v3:cleanup:{suffix}"),
            group: "clickhouse".into(),
            consumer: "clickhouse-1".into(),
            delete_acked_entries: true,
            reconnect_delay: Duration::from_millis(1),
        };
        let client = redis::Client::open(TEST_REDIS_URL)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        ensure_consumer_group(&mut conn, &cfg).await?;

        let first_id: String = redis::cmd("XADD")
            .arg(&cfg.stream)
            .arg("*")
            .arg("payload")
            .arg("{}")
            .query_async(&mut conn)
            .await?;
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&cfg.group)
            .arg(&cfg.consumer)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&cfg.stream)
            .arg(">")
            .query_async(&mut conn)
            .await?;

        let stats = SubscriberStats::default();
        let mut pending = vec![first_id];
        flush_acks(&mut conn, &cfg, &mut pending, &stats).await?;
        assert!(pending.is_empty());
        let length: i64 = redis::cmd("XLEN")
            .arg(&cfg.stream)
            .query_async(&mut conn)
            .await?;
        assert_eq!(length, 0);

        let _: () = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&cfg.stream)
            .arg("observer")
            .arg("0")
            .query_async(&mut conn)
            .await?;
        let second_id: String = redis::cmd("XADD")
            .arg(&cfg.stream)
            .arg("*")
            .arg("payload")
            .arg("{}")
            .query_async(&mut conn)
            .await?;
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&cfg.group)
            .arg(&cfg.consumer)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&cfg.stream)
            .arg(">")
            .query_async(&mut conn)
            .await?;

        let mut pending = vec![second_id];
        let error = flush_acks(&mut conn, &cfg, &mut pending, &stats)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("only consumer group"), "{error}");
        assert_eq!(pending.len(), 1);
        let length: i64 = redis::cmd("XLEN")
            .arg(&cfg.stream)
            .query_async(&mut conn)
            .await?;
        assert_eq!(length, 1);

        let _: i64 = redis::cmd("DEL")
            .arg(&cfg.stream)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }
}
