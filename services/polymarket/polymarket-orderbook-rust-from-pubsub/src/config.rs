//! Runtime configuration for the Polymarket pub/sub → ClickHouse writer.
//!
//! No WS pool, no market lifecycle, no shards: just the Redis pub/sub source
//! and the existing ClickHouse sink knobs.

use std::time::Duration;

use anyhow::{Context, Result};

pub const DEFAULT_FLUSH_BATCH_SIZE: usize = 5_000;
pub const DEFAULT_FLUSH_INTERVAL_MS: u64 = 500;

#[derive(Debug, Clone)]
pub struct Config {
    pub redis_url: String,
    pub redis_event_stream: String,
    pub stream_consumer_group: String,
    pub stream_consumer_name: String,
    pub pubsub_reconnect_delay: Duration,
    pub clickhouse_url: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub clickhouse_database: String,
    pub clickhouse_table: String,
    pub drop_table_on_start: bool,
    pub exclude_hash: bool,
    pub flush_batch_size: usize,
    pub flush_interval: Duration,
    pub ttl_minutes: u64,
    pub queue_size: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let clickhouse_host = env_or("CLICKHOUSE_HOST", "localhost");
        let clickhouse_port = env_u16_or("CLICKHOUSE_PORT", 8123)?;

        Ok(Self {
            redis_url: require_env("REDIS_URL")?,
            redis_event_stream: env_or("REDIS_EVENT_STREAM", "polymarket:events:v3"),
            stream_consumer_group: env_or("EVENT_CONSUMER_GROUP", "polymarket-clickhouse-v3"),
            stream_consumer_name: env_or("EVENT_CONSUMER_NAME", "polymarket-clickhouse-v3-1"),
            pubsub_reconnect_delay: Duration::from_millis(env_u64_or(
                "PUBSUB_RECONNECT_DELAY_MS",
                2_000,
            )?),
            clickhouse_url: format!("http://{clickhouse_host}:{clickhouse_port}"),
            clickhouse_user: env_or("CLICKHOUSE_USER", "default"),
            clickhouse_password: env_or("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database: env_or("CLICKHOUSE_DATABASE", "default"),
            clickhouse_table: env_or("CLICKHOUSE_TABLE", "polymarket_orderbook_v3"),
            drop_table_on_start: env_bool("DROP_TABLE_ON_START"),
            exclude_hash: env_bool_default("EXCLUDE_HASH", true),
            flush_batch_size: env_usize_or("FLUSH_BATCH_SIZE", DEFAULT_FLUSH_BATCH_SIZE)?,
            flush_interval: Duration::from_millis(env_u64_or(
                "FLUSH_INTERVAL_MS",
                DEFAULT_FLUSH_INTERVAL_MS,
            )?),
            ttl_minutes: env_u64_or("TTL_MINUTES", 0)?,
            queue_size: env_usize_or("QUEUE_SIZE", 1_000_000)?,
        })
    }
}

fn require_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing required env var {name}"))
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_u16_or(name: &str, default: u16) -> Result<u16> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("parse env var {name} as u16")),
        Err(_) => Ok(default),
    }
}

fn env_u64_or(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("parse env var {name} as u64")),
        Err(_) => Ok(default),
    }
}

fn env_usize_or(name: &str, default: usize) -> Result<usize> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("parse env var {name} as usize")),
        Err(_) => Ok(default),
    }
}

fn env_bool(name: &str) -> bool {
    env_bool_default(name, false)
}

fn env_bool_default(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(
            value.to_ascii_lowercase().as_str(),
            "true" | "1" | "yes"
        ),
        Err(_) => default,
    }
}
