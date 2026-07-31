//! Runtime configuration loaded from environment variables.
//!
//! All tunables that were previously scattered as module-level constants in
//! Python live here as a single struct.

use std::time::Duration;

use anyhow::{Context, Result};

/// Default ClickHouse insert batch size. Matches `sink.py::FLUSH_BATCH_SIZE`.
pub const DEFAULT_FLUSH_BATCH_SIZE: usize = 5000;
/// Default ClickHouse flush interval. Matches `sink.py::FLUSH_INTERVAL_SECONDS`.
pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Clone)]
pub struct Config {
    // -- Redis ---------------------------------------------------------------
    pub redis_url: String,
    pub redis_stream_market_events: String,
    pub redis_key_active_markets: String,
    pub redis_key_active_markets_count: String,
    pub stream_consumer_group: String,
    pub stream_consumer_name: String,

    // -- ClickHouse (HTTP, port 8123 by default) -----------------------------
    /// Base URL e.g. `http://obdata-clickhouse:8123`. Built from
    /// `CLICKHOUSE_HOST` + `CLICKHOUSE_PORT`.
    pub clickhouse_url: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub clickhouse_database: String,
    pub clickhouse_table: String,
    pub drop_table_on_start: bool,
    pub exclude_hash: bool,

    // -- Active markets pre-load ---------------------------------------------
    pub skip_active_markets_cache: bool,

    // -- Pipeline ------------------------------------------------------------
    pub queue_size: usize,
    pub max_assets_per_conn: usize,
    pub flush_batch_size: usize,
    pub flush_interval: Duration,

    // -- Redundant connections ------------------------------------------------
    /// Time-to-live for the global dedup cache. Entries survive at least
    /// `dedup_ttl / 2` and at most `dedup_ttl`. Must be comfortably larger
    /// than the expected inter-arrival skew between two connections
    /// receiving the same event (sub-second typically).
    pub dedup_ttl: Duration,

    // -- ClickHouse TTL ------------------------------------------------------
    /// Optional row-level TTL in minutes applied to `timestamp_received`.
    /// When set, ClickHouse automatically drops rows older than this.
    /// 0 means no TTL (default).
    pub ttl_minutes: u64,
}

impl Config {
    /// Load configuration from environment variables.
    ///
    /// Required: `REDIS_URL`. Everything else has a default that matches the
    /// Python service.
    pub fn from_env() -> Result<Self> {
        let clickhouse_host = env_or("CLICKHOUSE_HOST", "localhost");
        let clickhouse_port: u16 = env_parse("CLICKHOUSE_PORT", 8123)?;

        Ok(Self {
            // Redis
            redis_url: require_env("REDIS_URL")?,
            redis_stream_market_events: env_or(
                "REDIS_STREAM_MARKET_EVENTS",
                "polymarket:market_events",
            ),
            redis_key_active_markets: env_or(
                "REDIS_KEY_ACTIVE_MARKETS",
                "polymarket:active_markets",
            ),
            redis_key_active_markets_count: env_or(
                "REDIS_KEY_ACTIVE_MARKETS_COUNT",
                "polymarket:active_markets:count",
            ),
            stream_consumer_group: env_or("ORDERBOOK_CONSUMER_GROUP", "orderbook"),
            stream_consumer_name: env_or("ORDERBOOK_CONSUMER_NAME", "orderbook-1"),

            // ClickHouse
            clickhouse_url: format!("http://{clickhouse_host}:{clickhouse_port}"),
            clickhouse_user: env_or("CLICKHOUSE_USER", "default"),
            clickhouse_password: env_or("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database: env_or("CLICKHOUSE_DATABASE", "default"),
            clickhouse_table: env_or("CLICKHOUSE_TABLE", "polymarket_orderbook"),
            drop_table_on_start: env_bool("DROP_TABLE_ON_START"),
            exclude_hash: env_bool("EXCLUDE_HASH"),

            // Pre-load
            skip_active_markets_cache: env_bool("SKIP_ACTIVE_MARKETS_CACHE"),

            // Pipeline
            queue_size: env_parse("QUEUE_SIZE", 5_000_000)?,
            max_assets_per_conn: env_parse("MAX_ASSETS_PER_CONN", 200)?,
            flush_batch_size: DEFAULT_FLUSH_BATCH_SIZE,
            flush_interval: DEFAULT_FLUSH_INTERVAL,

            // Redundant connections
            dedup_ttl: Duration::from_secs(env_parse("DEDUP_TTL_SECONDS", 10_u64)?),

            // ClickHouse TTL
            ttl_minutes: env_parse("TTL_MINUTES", 0_u64)?,
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

/// Parse a boolean env var. Truthy values: `true`, `1`, `yes` (case-insensitive).
/// Anything else (including absent) is `false`. Matches Python's behaviour.
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

    /// Serialize env mutations across tests so they can't race.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn env_bool_truthy() {
        let _guard = ENV_LOCK.lock().unwrap();
        for v in ["true", "True", "TRUE", "1", "yes", "YES"] {
            std::env::set_var("TEST_BOOL_VAR", v);
            assert!(env_bool("TEST_BOOL_VAR"), "expected truthy for {v}");
        }
        std::env::remove_var("TEST_BOOL_VAR");
    }

    #[test]
    fn env_bool_falsy() {
        let _guard = ENV_LOCK.lock().unwrap();
        for v in ["false", "0", "no", "anything-else", ""] {
            std::env::set_var("TEST_BOOL_VAR2", v);
            assert!(!env_bool("TEST_BOOL_VAR2"), "expected falsy for {v}");
        }
        std::env::remove_var("TEST_BOOL_VAR2");
        assert!(!env_bool("TEST_BOOL_VAR2"));
    }

    #[test]
    fn from_env_requires_redis_url() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("REDIS_URL");
        let err = Config::from_env().unwrap_err().to_string();
        assert!(err.contains("REDIS_URL"), "expected REDIS_URL in error: {err}");
    }

    #[test]
    fn from_env_defaults_match_python() {
        let _guard = ENV_LOCK.lock().unwrap();
        // Snapshot existing env so we can restore.
        let snapshot: Vec<(String, Option<String>)> = [
            "REDIS_URL",
            "CLICKHOUSE_HOST",
            "CLICKHOUSE_PORT",
            "CLICKHOUSE_USER",
            "CLICKHOUSE_PASSWORD",
            "CLICKHOUSE_DATABASE",
            "CLICKHOUSE_TABLE",
            "DROP_TABLE_ON_START",
            "EXCLUDE_HASH",
            "SKIP_ACTIVE_MARKETS_CACHE",
            "QUEUE_SIZE",
            "MAX_ASSETS_PER_CONN",
            "REDIS_STREAM_MARKET_EVENTS",
            "REDIS_KEY_ACTIVE_MARKETS",
            "REDIS_KEY_ACTIVE_MARKETS_COUNT",
            "ORDERBOOK_CONSUMER_GROUP",
            "ORDERBOOK_CONSUMER_NAME",
        ]
        .iter()
        .map(|k| (k.to_string(), std::env::var(k).ok()))
        .collect();

        // Clear and set just REDIS_URL.
        for (k, _) in &snapshot {
            std::env::remove_var(k);
        }
        std::env::set_var("REDIS_URL", "redis://localhost:6379");

        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.redis_url, "redis://localhost:6379");
        assert_eq!(cfg.clickhouse_url, "http://localhost:8123");
        assert_eq!(cfg.clickhouse_user, "default");
        assert_eq!(cfg.clickhouse_database, "default");
        assert_eq!(cfg.clickhouse_table, "polymarket_orderbook");
        assert!(!cfg.drop_table_on_start);
        assert!(!cfg.exclude_hash);
        assert!(!cfg.skip_active_markets_cache);
        assert_eq!(cfg.queue_size, 5_000_000);
        assert_eq!(cfg.max_assets_per_conn, 200);
        assert_eq!(cfg.stream_consumer_group, "orderbook");
        assert_eq!(cfg.stream_consumer_name, "orderbook-1");
        assert_eq!(cfg.redis_stream_market_events, "polymarket:market_events");
        assert_eq!(cfg.redis_key_active_markets, "polymarket:active_markets");
        assert_eq!(cfg.redis_key_active_markets_count, "polymarket:active_markets:count");
        assert_eq!(cfg.flush_batch_size, 5000);
        assert_eq!(cfg.flush_interval, Duration::from_millis(500));

        // Restore.
        for (k, v) in snapshot {
            match v {
                Some(val) => std::env::set_var(&k, val),
                None => std::env::remove_var(&k),
            }
        }
    }

    #[test]
    fn from_env_overrides() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("REDIS_URL", "redis://r:6379");
        std::env::set_var("CLICKHOUSE_HOST", "ch.internal");
        std::env::set_var("CLICKHOUSE_PORT", "9000");
        std::env::set_var("EXCLUDE_HASH", "true");
        std::env::set_var("MAX_ASSETS_PER_CONN", "100");

        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.clickhouse_url, "http://ch.internal:9000");
        assert!(cfg.exclude_hash);
        assert_eq!(cfg.max_assets_per_conn, 100);

        std::env::remove_var("REDIS_URL");
        std::env::remove_var("CLICKHOUSE_HOST");
        std::env::remove_var("CLICKHOUSE_PORT");
        std::env::remove_var("EXCLUDE_HASH");
        std::env::remove_var("MAX_ASSETS_PER_CONN");
    }
}
