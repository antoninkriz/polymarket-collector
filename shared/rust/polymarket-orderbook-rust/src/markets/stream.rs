//! Redis stream consumer for market lifecycle events.
//!
//! Reads from `polymarket:market_events` (overridable) using `XREADGROUP`
//! against the configured consumer group + name. Two event types are
//! handled:
//!
//! - `new_market` — normalize the complete payload and send it to the
//!   authoritative lifecycle coordinator.
//! - `market_resolved` — normalize the complete payload and send it to the
//!   same coordinator.
//!
//! Each successfully processed message is XACK'd. With `--skip-backlog`, the
//! cursor starts at `$` (drop pending messages and only process new ones).
//!
//! The consumer drains its pending (unacknowledged) messages first, then
//! switches to `">"` for new messages.

use std::time::Duration;

use anyhow::{ensure, Context, Result};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, AsyncConnectionConfig};
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::events::{Event, MarketLifecycleObservation};
use crate::markets::lifecycle::{LifecycleRequest, LifecycleSource};
use crate::record::now_ns;

const READ_COUNT: usize = 10;
const READ_BLOCK_MS: usize = 1000;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);

pub struct StreamConfig {
    pub redis_url: String,
    pub stream_key: String,
    pub group: String,
    pub consumer: String,
    pub skip_backlog: bool,
}

/// Run the stream consumer until shutdown. An entry is acknowledged only after
/// the lifecycle coordinator confirms that its event was successfully applied.
pub async fn run(cfg: StreamConfig, lifecycle_tx: mpsc::Sender<LifecycleRequest>) -> Result<()> {
    let client = redis::Client::open(cfg.redis_url.as_str()).context("open redis client")?;
    // redis-rs 1.5 defaults to a 500 ms response timeout, which is shorter
    // than this consumer's intentional one-second blocking stream read.
    let connection_config =
        AsyncConnectionConfig::new().set_response_timeout(Some(RESPONSE_TIMEOUT));
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&connection_config)
        .await
        .context("connect redis")?;

    ensure_consumer_group(&mut conn, &cfg).await?;

    info!(
        stream = %cfg.stream_key,
        group = %cfg.group,
        consumer = %cfg.consumer,
        "stream listener started",
    );

    let opts = StreamReadOptions::default()
        .group(&cfg.group, &cfg.consumer)
        .count(READ_COUNT)
        .block(READ_BLOCK_MS);

    let mut pending_done = false;
    let mut last_pending_id = String::from("0");

    loop {
        let read_id: &str = if pending_done {
            ">"
        } else {
            last_pending_id.as_str()
        };

        let reply: StreamReadReply = conn
            .xread_options(&[cfg.stream_key.as_str()], &[read_id], &opts)
            .await
            .context("xreadgroup")?;

        if reply.keys.is_empty() {
            // Block timeout with no new entries.
            if !pending_done {
                pending_done = true;
            }
            continue;
        }

        let mut got_any = false;
        for stream_key in reply.keys {
            for entry in stream_key.ids {
                got_any = true;
                last_pending_id = entry.id.clone();
                let data = stream_entry_to_map(&entry);
                let timestamp_received_ns = now_ns();
                dispatch(&data, timestamp_received_ns, &lifecycle_tx)
                    .await
                    .with_context(|| format!("dispatch stream message {}", entry.id))?;
                let acknowledged = conn
                    .xack::<_, _, _, i64>(
                        cfg.stream_key.as_str(),
                        cfg.group.as_str(),
                        &[entry.id.as_str()],
                    )
                    .await
                    .with_context(|| format!("xack stream message {}", entry.id))?;
                ensure!(
                    acknowledged == 1,
                    "xack stream message {} acknowledged {acknowledged} entries",
                    entry.id
                );
            }
        }
        // Phase transition: when the pending pass returns no entries we
        // switch to ">" for new messages.
        if !got_any && !pending_done {
            pending_done = true;
        }
    }
}

async fn ensure_consumer_group(conn: &mut MultiplexedConnection, cfg: &StreamConfig) -> Result<()> {
    let start_id = if cfg.skip_backlog { "$" } else { "0" };

    // XGROUP CREATE stream group id MKSTREAM. Returns BUSYGROUP if it
    // already exists; that's not fatal.
    let res: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&cfg.stream_key)
        .arg(&cfg.group)
        .arg(start_id)
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    match res {
        Ok(()) => {
            info!(stream = %cfg.stream_key, group = %cfg.group, "created consumer group");
        }
        Err(e) if e.to_string().contains("BUSYGROUP") => {
            info!(group = %cfg.group, "consumer group already exists");
            if cfg.skip_backlog {
                let _: () = redis::cmd("XGROUP")
                    .arg("SETID")
                    .arg(&cfg.stream_key)
                    .arg(&cfg.group)
                    .arg(start_id)
                    .query_async(conn)
                    .await
                    .context("xgroup setid")?;
                info!(group = %cfg.group, id = start_id, "reset cursor");
            }
        }
        Err(e) => return Err(e).context("xgroup create"),
    }
    Ok(())
}

fn stream_entry_to_map(
    entry: &redis::streams::StreamId,
) -> std::collections::HashMap<String, String> {
    let mut out = std::collections::HashMap::new();
    for (k, v) in &entry.map {
        if let redis::Value::BulkString(bytes) = v {
            if let Ok(s) = std::str::from_utf8(bytes) {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

async fn dispatch(
    data: &std::collections::HashMap<String, String>,
    timestamp_received_ns: i64,
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
) -> Result<()> {
    let Some(event) = normalize_event(data)? else {
        return Ok(());
    };
    let observation = MarketLifecycleObservation {
        event,
        timestamp_received_ns,
    };
    let (completion, completed) = oneshot::channel();
    lifecycle_tx
        .send(LifecycleRequest::Observation {
            source: LifecycleSource::RedisStream,
            observation,
            completion,
        })
        .await
        .context("lifecycle coordinator channel closed")?;
    completed
        .await
        .context("lifecycle coordinator stopped before confirming dispatch")?
        .map_err(anyhow::Error::msg)
}

fn normalize_event(data: &std::collections::HashMap<String, String>) -> Result<Option<Event>> {
    let event_type = data.get("event_type").map(String::as_str).unwrap_or("");
    let payload_raw = data.get("payload").map(String::as_str).unwrap_or("{}");
    match event_type {
        "new_market" => {
            let payload: NewMarketPayload =
                serde_json::from_str(payload_raw).context("parse new_market payload")?;
            Ok(Some(Event::NewMarket {
                id: payload.id,
                market: payload.market,
                timestamp: payload.timestamp,
                assets_ids: payload.assets_ids,
                outcomes: payload.outcomes,
                question: payload.question,
                slug: payload.slug,
            }))
        }
        "market_resolved" => {
            let payload: MarketResolvedPayload =
                serde_json::from_str(payload_raw).context("parse market_resolved payload")?;
            Ok(Some(Event::MarketResolved {
                id: payload.id,
                market: payload.market,
                timestamp: payload.timestamp,
                assets_ids: payload.assets_ids,
                winning_asset_id: payload.winning_asset_id,
                winning_outcome: payload.winning_outcome,
            }))
        }
        _ => Ok(None),
    }
}

#[derive(Debug, Deserialize)]
struct NewMarketPayload {
    #[serde(default)]
    id: String,
    #[serde(default)]
    market: String,
    #[serde(default)]
    timestamp: String,
    #[serde(default)]
    assets_ids: Vec<String>,
    #[serde(default)]
    outcomes: Vec<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(default)]
    slug: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MarketResolvedPayload {
    #[serde(default)]
    id: String,
    #[serde(default)]
    market: String,
    #[serde(default)]
    timestamp: String,
    #[serde(default)]
    assets_ids: Vec<String>,
    #[serde(default)]
    winning_asset_id: Option<String>,
    #[serde(default)]
    winning_outcome: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_new_market_payload() {
        let raw = r#"{
            "market": "0xabc",
            "assets_ids": ["y", "n"],
            "outcomes": ["Yes", "No"],
            "question": "Will it rain?",
            "slug": "rain"
        }"#;
        let p: NewMarketPayload = serde_json::from_str(raw).unwrap();
        assert_eq!(p.market, "0xabc");
        assert_eq!(p.assets_ids, vec!["y", "n"]);
        assert_eq!(p.outcomes, vec!["Yes", "No"]);
    }

    #[test]
    fn normalizes_complete_new_market_event() {
        let data = std::collections::HashMap::from([
            ("event_type".into(), "new_market".into()),
            (
                "payload".into(),
                r#"{
                    "id":"7", "market":"0xabc", "timestamp":"123",
                    "assets_ids":["y","n"], "outcomes":["Yes","No"],
                    "question":"Will it rain?", "slug":"rain"
                }"#
                .into(),
            ),
        ]);
        let event = normalize_event(&data).unwrap().unwrap();
        let Event::NewMarket {
            id,
            market,
            timestamp,
            assets_ids,
            outcomes,
            question,
            slug,
        } = event
        else {
            panic!("expected new_market")
        };
        assert_eq!(id, "7");
        assert_eq!(market, "0xabc");
        assert_eq!(timestamp, "123");
        assert_eq!(assets_ids, ["y", "n"]);
        assert_eq!(outcomes, ["Yes", "No"]);
        assert_eq!(question.as_deref(), Some("Will it rain?"));
        assert_eq!(slug.as_deref(), Some("rain"));
    }

    #[test]
    fn parse_market_resolved_payload() {
        let raw = r#"{
            "market": "0xabc",
            "winning_outcome": "Yes",
            "question": "Q?"
        }"#;
        let p: MarketResolvedPayload = serde_json::from_str(raw).unwrap();
        assert_eq!(p.market, "0xabc");
        assert_eq!(p.winning_outcome.as_deref(), Some("Yes"));
    }

    #[test]
    fn normalizes_complete_market_resolved_event() {
        let data = std::collections::HashMap::from([
            ("event_type".into(), "market_resolved".into()),
            (
                "payload".into(),
                r#"{
                    "id":"7", "market":"0xabc", "timestamp":"123",
                    "assets_ids":["y","n"], "winning_asset_id":"y",
                    "winning_outcome":"Yes"
                }"#
                .into(),
            ),
        ]);
        let event = normalize_event(&data).unwrap().unwrap();
        let Event::MarketResolved {
            id,
            market,
            timestamp,
            assets_ids,
            winning_asset_id,
            winning_outcome,
        } = event
        else {
            panic!("expected market_resolved")
        };
        assert_eq!(id, "7");
        assert_eq!(market, "0xabc");
        assert_eq!(timestamp, "123");
        assert_eq!(assets_ids, ["y", "n"]);
        assert_eq!(winning_asset_id.as_deref(), Some("y"));
        assert_eq!(winning_outcome.as_deref(), Some("Yes"));
    }

    #[test]
    fn binary_market_yes_first() {
        let m = crate::markets::binary_market_from_outcomes(
            "0xabc".into(),
            &["Yes".to_string(), "No".to_string()],
            &["y".to_string(), "n".to_string()],
        )
        .unwrap();
        assert_eq!(m.yes(), "y");
        assert_eq!(m.no(), "n");
    }

    #[test]
    fn binary_market_no_first() {
        let m = crate::markets::binary_market_from_outcomes(
            "0xabc".into(),
            &["No".to_string(), "Yes".to_string()],
            &["y".to_string(), "n".to_string()],
        )
        .unwrap();
        // outcomes[0] is "No" so YES is index 1.
        assert_eq!(m.yes(), "n");
        assert_eq!(m.no(), "y");
    }

    #[test]
    fn binary_market_rejects_non_binary() {
        let result = crate::markets::binary_market_from_outcomes(
            "0xabc".into(),
            &["A".into(), "B".into(), "C".into()],
            &["1".into(), "2".into(), "3".into()],
        );
        assert!(result.is_none());
    }

    #[test]
    fn unknown_event_is_ignored() {
        let data = std::collections::HashMap::from([("event_type".into(), "future".into())]);
        assert!(normalize_event(&data).unwrap().is_none());
    }
}
