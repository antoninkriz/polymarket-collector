//! Redis stream consumer for market lifecycle events.
//!
//! Reads from `polymarket:market_events` (overridable) using `XREADGROUP`
//! against the configured consumer group + name. Two event types are
//! handled:
//!
//! - `new_market` — parse outcomes/asset IDs into a [`Market`] and call the
//!   pool's `subscribe_markets`.
//! - `market_resolved` — call the pool's `unsubscribe_markets`.
//!
//! Each successfully processed message is XACK'd. With `--skip-backlog`, the
//! cursor starts at `$` (drop pending messages and only process new ones).
//!
//! The consumer drains its pending (unacknowledged) messages first, then
//! switches to `">"` for new messages.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, AsyncConnectionConfig};
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::markets;
use crate::ws::pool::Pool;

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

/// Run the stream consumer until shutdown. Holds a shared lock on the
/// pool to dispatch subscribe / unsubscribe calls.
pub async fn run(cfg: StreamConfig, pool: Arc<Mutex<Pool>>) -> Result<()> {
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
                if let Err(e) = dispatch(&data, &pool).await {
                    error!(
                        message_id = %entry.id,
                        error = %e,
                        "stream message dispatch failed",
                    );
                }
                if let Err(e) = conn
                    .xack::<_, _, _, i64>(
                        cfg.stream_key.as_str(),
                        cfg.group.as_str(),
                        &[entry.id.as_str()],
                    )
                    .await
                {
                    warn!(message_id = %entry.id, error = %e, "xack failed");
                }
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
    pool: &Arc<Mutex<Pool>>,
) -> Result<()> {
    let event_type = data.get("event_type").map(String::as_str).unwrap_or("");
    let payload_raw = data.get("payload").map(String::as_str).unwrap_or("{}");

    match event_type {
        "new_market" => {
            let payload: NewMarketPayload =
                serde_json::from_str(payload_raw).context("parse new_market payload")?;
            let Some(market) = markets::binary_market_from_outcomes(
                payload.market.clone(),
                &payload.outcomes,
                &payload.assets_ids,
            ) else {
                return Ok(()); // skip non-binary
            };
            info!(
                market = short(&payload.market),
                question = short(&payload.question.unwrap_or_default()),
                "[MARKET_EVENT] new_market",
            );
            pool.lock().await.subscribe_markets(vec![market]).await?;
        }
        "market_resolved" => {
            let payload: MarketResolvedPayload =
                serde_json::from_str(payload_raw).context("parse market_resolved payload")?;
            if payload.market.is_empty() {
                return Ok(());
            }
            info!(
                market = short(&payload.market),
                winner = %payload.winning_outcome.unwrap_or_default(),
                "[MARKET_EVENT] market_resolved",
            );
            pool.lock()
                .await
                .unsubscribe_markets(vec![payload.market])
                .await?;
        }
        _ => {
            // Unknown event type — ignore.
        }
    }
    Ok(())
}

fn short(s: &str) -> &str {
    let mut end = s.len().min(16);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[derive(Debug, Deserialize)]
struct NewMarketPayload {
    #[serde(default)]
    market: String,
    #[serde(default)]
    assets_ids: Vec<String>,
    #[serde(default)]
    outcomes: Vec<String>,
    #[serde(default)]
    question: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MarketResolvedPayload {
    #[serde(default)]
    market: String,
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
    fn short_does_not_split_a_utf8_character() {
        let value = format!("{}é-tail", "a".repeat(15));
        assert_eq!(short(&value), "a".repeat(15));
    }
}
