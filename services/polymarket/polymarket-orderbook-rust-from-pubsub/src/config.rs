//! Runtime configuration for the Polymarket Redis Stream → ClickHouse writer.
//!
//! No WS pool, no market lifecycle, no shards: just the Redis Stream source
//! and the existing ClickHouse sink knobs.

use std::time::Duration;

use anyhow::{Context, Result};

pub const DEFAULT_FLUSH_BATCH_SIZE: usize = 5_000;
pub const DEFAULT_FLUSH_INTERVAL_MS: u64 = 500;
pub const DEFAULT_QUEUE_SIZE: usize = 50_000;
pub const DEFAULT_ACK_QUEUE_SIZE: usize = 32;

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
    pub flush_batch_size: usize,
    pub flush_interval: Duration,
    pub queue_size: usize,
    pub ack_queue_size: usize,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let clickhouse_host = env_or("CLICKHOUSE_HOST", "localhost");
        let clickhouse_port = env_u16_or("CLICKHOUSE_PORT", 8123)?;
        let queue_size = env_usize_or("QUEUE_SIZE", DEFAULT_QUEUE_SIZE)?;
        let ack_queue_size = env_usize_or("ACK_QUEUE_SIZE", DEFAULT_ACK_QUEUE_SIZE)?;
        validate_queue_sizes(queue_size, ack_queue_size)?;

        Ok(Self {
            redis_url: require_env("REDIS_URL")?,
            redis_event_stream: env_or("REDIS_EVENT_STREAM", "polymarket:events:v3"),
            stream_consumer_group: env_or("EVENT_CONSUMER_GROUP", "polymarket-clickhouse-v3"),
            stream_consumer_name: env_or("EVENT_CONSUMER_NAME", "polymarket-clickhouse-v3-1"),
            pubsub_reconnect_delay: Duration::from_millis(env_u64_or(
                "STREAM_RECONNECT_DELAY_MS",
                2_000,
            )?),
            clickhouse_url: format!("http://{clickhouse_host}:{clickhouse_port}"),
            clickhouse_user: env_or("CLICKHOUSE_USER", "default"),
            clickhouse_password: env_or("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database: env_or("CLICKHOUSE_DATABASE", "default"),
            clickhouse_table: env_or("CLICKHOUSE_TABLE", "polymarket_orderbook_v3"),
            flush_batch_size: env_usize_or("FLUSH_BATCH_SIZE", DEFAULT_FLUSH_BATCH_SIZE)?,
            flush_interval: Duration::from_millis(env_u64_or(
                "FLUSH_INTERVAL_MS",
                DEFAULT_FLUSH_INTERVAL_MS,
            )?),
            queue_size,
            ack_queue_size,
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

fn validate_queue_sizes(queue_size: usize, ack_queue_size: usize) -> Result<()> {
    anyhow::ensure!(queue_size > 0, "QUEUE_SIZE must be positive");
    anyhow::ensure!(ack_queue_size > 0, "ACK_QUEUE_SIZE must be positive");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{env_usize_or, validate_queue_sizes, DEFAULT_ACK_QUEUE_SIZE, DEFAULT_QUEUE_SIZE};

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    const QUEUE_ENV: &str = "TEST_POLYMARKET_WRITER_QUEUE_SIZE";
    const ACK_QUEUE_ENV: &str = "TEST_POLYMARKET_WRITER_ACK_QUEUE_SIZE";

    #[test]
    fn queue_defaults_and_overrides_are_valid() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var(QUEUE_ENV);
        std::env::remove_var(ACK_QUEUE_ENV);
        assert_eq!(
            env_usize_or(QUEUE_ENV, DEFAULT_QUEUE_SIZE).unwrap(),
            DEFAULT_QUEUE_SIZE,
        );
        assert_eq!(
            env_usize_or(ACK_QUEUE_ENV, DEFAULT_ACK_QUEUE_SIZE).unwrap(),
            DEFAULT_ACK_QUEUE_SIZE,
        );

        std::env::set_var(QUEUE_ENV, "1234");
        std::env::set_var(ACK_QUEUE_ENV, "7");
        assert_eq!(env_usize_or(QUEUE_ENV, DEFAULT_QUEUE_SIZE).unwrap(), 1234);
        assert_eq!(
            env_usize_or(ACK_QUEUE_ENV, DEFAULT_ACK_QUEUE_SIZE).unwrap(),
            7,
        );
        std::env::remove_var(QUEUE_ENV);
        std::env::remove_var(ACK_QUEUE_ENV);
    }

    #[test]
    fn zero_sized_queues_are_rejected() {
        assert!(validate_queue_sizes(0, DEFAULT_ACK_QUEUE_SIZE).is_err());
        assert!(validate_queue_sizes(DEFAULT_QUEUE_SIZE, 0).is_err());
        assert!(validate_queue_sizes(DEFAULT_QUEUE_SIZE, DEFAULT_ACK_QUEUE_SIZE).is_ok());
    }
}
