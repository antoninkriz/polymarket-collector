//! Runtime configuration loaded from environment variables.
//!
//! Standalone from the upstream `polymarket-orderbook-rust` config because
//! this service publishes to Redis pub/sub and has no ClickHouse dependency.

use std::time::Duration;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    // -- Redis ---------------------------------------------------------------
    pub redis_url: String,
    pub redis_stream_market_events: String,
    pub redis_key_active_markets: String,
    pub redis_key_active_markets_count: String,
    pub stream_consumer_group: String,
    pub stream_consumer_name: String,

    // -- Pub/sub publish -----------------------------------------------------
    pub redis_event_stream: String,
    pub publish_batch_max: usize,
    pub publish_linger: Duration,

    // -- Active markets pre-load --------------------------------------------
    pub skip_active_markets_cache: bool,

    // -- Pipeline ------------------------------------------------------------
    pub queue_size: usize,
    pub max_assets_per_conn: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            redis_url: require_env("REDIS_URL")?,
            redis_stream_market_events: env_or(
                "REDIS_STREAM_MARKET_EVENTS",
                "polymarket:market_events",
            ),
            redis_key_active_markets: env_or(
                "REDIS_KEY_ACTIVE_MARKETS",
                "polymarket:active_markets:pubsub",
            ),
            redis_key_active_markets_count: env_or(
                "REDIS_KEY_ACTIVE_MARKETS_COUNT",
                "polymarket:active_markets:pubsub:count",
            ),
            stream_consumer_group: env_or("ORDERBOOK_CONSUMER_GROUP", "orderbook-rust-pubsub"),
            stream_consumer_name: env_or("ORDERBOOK_CONSUMER_NAME", "orderbook-rust-pubsub-1"),

            redis_event_stream: env_or("REDIS_EVENT_STREAM", "polymarket:events:v3"),
            publish_batch_max: env_parse("MAX_BATCH", 200_usize)?,
            publish_linger: Duration::from_millis(env_parse("LINGER_MS", 2_u64)?),

            skip_active_markets_cache: env_bool("SKIP_ACTIVE_MARKETS_CACHE"),

            queue_size: env_parse("QUEUE_SIZE", 5_000_000)?,
            max_assets_per_conn: env_parse("MAX_ASSETS_PER_CONN", 200)?,
        })
    }
}

// -- env helpers -------------------------------------------------------------

fn require_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing required env var {name}"))
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("invalid {name}={raw}: {e}")),
        Err(_) => Ok(default),
    }
}

fn env_bool(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .ok()
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("true" | "1" | "yes")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    const ALL_ENV_KEYS: &[&str] = &[
        "REDIS_URL",
        "REDIS_STREAM_MARKET_EVENTS",
        "REDIS_KEY_ACTIVE_MARKETS",
        "REDIS_KEY_ACTIVE_MARKETS_COUNT",
        "ORDERBOOK_CONSUMER_GROUP",
        "ORDERBOOK_CONSUMER_NAME",
        "REDIS_EVENT_STREAM",
        "SKIP_ACTIVE_MARKETS_CACHE",
        "QUEUE_SIZE",
        "MAX_ASSETS_PER_CONN",
        "MAX_BATCH",
        "LINGER_MS",
    ];

    fn snapshot_env() -> Vec<(String, Option<String>)> {
        ALL_ENV_KEYS
            .iter()
            .map(|k| (k.to_string(), std::env::var(k).ok()))
            .collect()
    }

    fn restore_env(snapshot: Vec<(String, Option<String>)>) {
        for (k, v) in snapshot {
            match v {
                Some(val) => std::env::set_var(&k, val),
                None => std::env::remove_var(&k),
            }
        }
    }

    fn clear_env() {
        for k in ALL_ENV_KEYS {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn from_env_requires_redis_url() {
        let _guard = ENV_LOCK.lock().unwrap();
        let snap = snapshot_env();
        clear_env();

        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("REDIS_URL"), "expected REDIS_URL in error: {err}");

        restore_env(snap);
    }

    #[test]
    fn from_env_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        let snap = snapshot_env();
        clear_env();
        std::env::set_var("REDIS_URL", "redis://localhost:6379");

        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.redis_url, "redis://localhost:6379");
        assert_eq!(cfg.redis_stream_market_events, "polymarket:market_events");
        assert_eq!(cfg.redis_key_active_markets, "polymarket:active_markets:pubsub");
        assert_eq!(
            cfg.redis_key_active_markets_count,
            "polymarket:active_markets:pubsub:count",
        );
        assert_eq!(cfg.stream_consumer_group, "orderbook-rust-pubsub");
        assert_eq!(cfg.stream_consumer_name, "orderbook-rust-pubsub-1");
        assert_eq!(cfg.redis_event_stream, "polymarket:events:v3");
        assert!(!cfg.skip_active_markets_cache);
        assert_eq!(cfg.queue_size, 5_000_000);
        assert_eq!(cfg.max_assets_per_conn, 200);
        assert_eq!(cfg.publish_batch_max, 200);
        assert_eq!(cfg.publish_linger, Duration::from_millis(2));

        restore_env(snap);
    }

    #[test]
    fn from_env_overrides() {
        let _guard = ENV_LOCK.lock().unwrap();
        let snap = snapshot_env();
        clear_env();
        std::env::set_var("REDIS_URL", "redis://r:6379");
        std::env::set_var("REDIS_EVENT_STREAM", "my:custom:stream");
        std::env::set_var("MAX_ASSETS_PER_CONN", "100");
        std::env::set_var("ORDERBOOK_CONSUMER_GROUP", "g1");
        std::env::set_var("ORDERBOOK_CONSUMER_NAME", "c1");

        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.redis_url, "redis://r:6379");
        assert_eq!(cfg.redis_event_stream, "my:custom:stream");
        assert_eq!(cfg.max_assets_per_conn, 100);
        assert_eq!(cfg.stream_consumer_group, "g1");
        assert_eq!(cfg.stream_consumer_name, "c1");

        restore_env(snap);
    }

    #[test]
    fn env_bool_parses_truthy_values() {
        let _guard = ENV_LOCK.lock().unwrap();
        for v in ["true", "True", "TRUE", "1", "yes", "YES"] {
            std::env::set_var("TEST_PUBSUB_BOOL", v);
            assert!(env_bool("TEST_PUBSUB_BOOL"), "expected truthy for {v}");
        }
        std::env::remove_var("TEST_PUBSUB_BOOL");
    }

    #[test]
    fn env_bool_parses_falsy_values() {
        let _guard = ENV_LOCK.lock().unwrap();
        for v in ["false", "0", "no", "anything-else", ""] {
            std::env::set_var("TEST_PUBSUB_BOOL2", v);
            assert!(!env_bool("TEST_PUBSUB_BOOL2"), "expected falsy for {v}");
        }
        std::env::remove_var("TEST_PUBSUB_BOOL2");
        assert!(!env_bool("TEST_PUBSUB_BOOL2"));
    }
}
