//! Runtime configuration loaded from environment variables.
//!
//! Standalone from the upstream `polymarket-orderbook-rust` config because
//! this service appends v3 records to Redis and has no ClickHouse dependency.

use std::time::Duration;

use anyhow::{Context, Result};

pub const DEFAULT_QUEUE_SIZE: usize = 250_000;

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
    pub publisher_lease_key: String,
    pub publisher_lease_generation_key: String,
    pub publisher_generation_floor: u64,
    pub publisher_generation_persist_timeout: Duration,
    pub publisher_lease_ttl: Duration,
    pub publisher_lease_renew_interval: Duration,

    // -- Durable sequence recovery -----------------------------------------
    pub clickhouse_url: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub clickhouse_database: String,
    pub clickhouse_table: String,

    // -- Active markets pre-load --------------------------------------------
    pub skip_active_markets_cache: bool,

    // -- Pipeline ------------------------------------------------------------
    pub queue_size: usize,
    pub max_assets_per_conn: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let clickhouse_host = env_or("CLICKHOUSE_HOST", "localhost");
        let clickhouse_port = env_parse("CLICKHOUSE_PORT", 8123_u16)?;
        let publisher_lease_ttl =
            Duration::from_millis(env_parse("PUBLISHER_LEASE_TTL_MS", 15_000_u64)?);
        let publisher_lease_renew_interval =
            Duration::from_millis(env_parse("PUBLISHER_LEASE_RENEW_MS", 5_000_u64)?);
        let publisher_generation_persist_timeout = Duration::from_millis(env_parse(
            "PUBLISHER_GENERATION_PERSIST_TIMEOUT_MS",
            5_000_u64,
        )?);
        let queue_size = env_parse("QUEUE_SIZE", DEFAULT_QUEUE_SIZE)?;
        anyhow::ensure!(
            !publisher_lease_ttl.is_zero(),
            "PUBLISHER_LEASE_TTL_MS must be positive",
        );
        anyhow::ensure!(
            !publisher_lease_renew_interval.is_zero()
                && publisher_lease_renew_interval < publisher_lease_ttl,
            "PUBLISHER_LEASE_RENEW_MS must be positive and less than PUBLISHER_LEASE_TTL_MS",
        );
        anyhow::ensure!(
            !publisher_generation_persist_timeout.is_zero()
                && publisher_generation_persist_timeout < publisher_lease_ttl,
            "PUBLISHER_GENERATION_PERSIST_TIMEOUT_MS must be positive and less than PUBLISHER_LEASE_TTL_MS",
        );
        anyhow::ensure!(queue_size > 0, "QUEUE_SIZE must be positive");

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
            publisher_lease_key: env_or("PUBLISHER_LEASE_KEY", "polymarket:publisher:v3:lease"),
            publisher_lease_generation_key: env_or(
                "PUBLISHER_LEASE_GENERATION_KEY",
                "polymarket:publisher:v3:generation",
            ),
            publisher_generation_floor: env_parse("PUBLISHER_GENERATION_FLOOR", 0_u64)?,
            publisher_generation_persist_timeout,
            publisher_lease_ttl,
            publisher_lease_renew_interval,

            clickhouse_url: format!("http://{clickhouse_host}:{clickhouse_port}"),
            clickhouse_user: env_or("CLICKHOUSE_USER", "default"),
            clickhouse_password: env_or("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database: env_or("CLICKHOUSE_DATABASE", "default"),
            clickhouse_table: env_or("CLICKHOUSE_TABLE", "polymarket_orderbook_v3"),

            skip_active_markets_cache: env_bool("SKIP_ACTIVE_MARKETS_CACHE"),

            queue_size,
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
        "PUBLISHER_LEASE_KEY",
        "PUBLISHER_LEASE_GENERATION_KEY",
        "PUBLISHER_GENERATION_FLOOR",
        "PUBLISHER_GENERATION_PERSIST_TIMEOUT_MS",
        "PUBLISHER_LEASE_TTL_MS",
        "PUBLISHER_LEASE_RENEW_MS",
        "CLICKHOUSE_HOST",
        "CLICKHOUSE_PORT",
        "CLICKHOUSE_USER",
        "CLICKHOUSE_PASSWORD",
        "CLICKHOUSE_DATABASE",
        "CLICKHOUSE_TABLE",
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
        assert!(
            err.contains("REDIS_URL"),
            "expected REDIS_URL in error: {err}"
        );

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
        assert_eq!(
            cfg.redis_key_active_markets,
            "polymarket:active_markets:pubsub"
        );
        assert_eq!(
            cfg.redis_key_active_markets_count,
            "polymarket:active_markets:pubsub:count",
        );
        assert_eq!(cfg.stream_consumer_group, "orderbook-rust-pubsub");
        assert_eq!(cfg.stream_consumer_name, "orderbook-rust-pubsub-1");
        assert_eq!(cfg.redis_event_stream, "polymarket:events:v3");
        assert!(!cfg.skip_active_markets_cache);
        assert_eq!(cfg.queue_size, DEFAULT_QUEUE_SIZE);
        assert_eq!(cfg.max_assets_per_conn, 200);
        assert_eq!(cfg.publish_batch_max, 200);
        assert_eq!(cfg.publish_linger, Duration::from_millis(2));
        assert_eq!(cfg.publisher_lease_key, "polymarket:publisher:v3:lease");
        assert_eq!(
            cfg.publisher_lease_generation_key,
            "polymarket:publisher:v3:generation",
        );
        assert_eq!(cfg.publisher_lease_ttl, Duration::from_secs(15));
        assert_eq!(cfg.publisher_lease_renew_interval, Duration::from_secs(5),);
        assert_eq!(cfg.publisher_generation_floor, 0);
        assert_eq!(
            cfg.publisher_generation_persist_timeout,
            Duration::from_secs(5),
        );
        assert_eq!(cfg.clickhouse_url, "http://localhost:8123");
        assert_eq!(cfg.clickhouse_database, "default");
        assert_eq!(cfg.clickhouse_table, "polymarket_orderbook_v3");

        restore_env(snap);
    }

    #[test]
    fn from_env_overrides() {
        let _guard = ENV_LOCK.lock().unwrap();
        let snap = snapshot_env();
        clear_env();
        std::env::set_var("REDIS_URL", "redis://r:6379");
        std::env::set_var("REDIS_EVENT_STREAM", "my:custom:stream");
        std::env::set_var("QUEUE_SIZE", "1234");
        std::env::set_var("MAX_ASSETS_PER_CONN", "100");
        std::env::set_var("ORDERBOOK_CONSUMER_GROUP", "g1");
        std::env::set_var("ORDERBOOK_CONSUMER_NAME", "c1");
        std::env::set_var("PUBLISHER_LEASE_KEY", "custom:lease");
        std::env::set_var("PUBLISHER_LEASE_GENERATION_KEY", "custom:generation");
        std::env::set_var("PUBLISHER_LEASE_TTL_MS", "9000");
        std::env::set_var("PUBLISHER_LEASE_RENEW_MS", "2000");
        std::env::set_var("PUBLISHER_GENERATION_FLOOR", "23");
        std::env::set_var("PUBLISHER_GENERATION_PERSIST_TIMEOUT_MS", "3000");
        std::env::set_var("CLICKHOUSE_HOST", "clickhouse");
        std::env::set_var("CLICKHOUSE_PORT", "8124");
        std::env::set_var("CLICKHOUSE_DATABASE", "marketdata");
        std::env::set_var("CLICKHOUSE_TABLE", "events_v3");

        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.redis_url, "redis://r:6379");
        assert_eq!(cfg.redis_event_stream, "my:custom:stream");
        assert_eq!(cfg.queue_size, 1234);
        assert_eq!(cfg.max_assets_per_conn, 100);
        assert_eq!(cfg.stream_consumer_group, "g1");
        assert_eq!(cfg.stream_consumer_name, "c1");
        assert_eq!(cfg.publisher_lease_key, "custom:lease");
        assert_eq!(cfg.publisher_lease_generation_key, "custom:generation");
        assert_eq!(cfg.publisher_lease_ttl, Duration::from_secs(9));
        assert_eq!(cfg.publisher_lease_renew_interval, Duration::from_secs(2),);
        assert_eq!(cfg.publisher_generation_floor, 23);
        assert_eq!(
            cfg.publisher_generation_persist_timeout,
            Duration::from_secs(3),
        );
        assert_eq!(cfg.clickhouse_url, "http://clickhouse:8124");
        assert_eq!(cfg.clickhouse_database, "marketdata");
        assert_eq!(cfg.clickhouse_table, "events_v3");

        restore_env(snap);
    }

    #[test]
    fn from_env_rejects_zero_queue_size() {
        let _guard = ENV_LOCK.lock().unwrap();
        let snap = snapshot_env();
        clear_env();
        std::env::set_var("REDIS_URL", "redis://localhost:6379");
        std::env::set_var("QUEUE_SIZE", "0");

        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("QUEUE_SIZE must be positive"), "{err}");

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
