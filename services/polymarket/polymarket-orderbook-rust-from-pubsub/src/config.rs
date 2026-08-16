//! Runtime configuration for the Polymarket Redis Stream → ClickHouse writer.
//!
//! No WS pool, no market lifecycle, no shards: just the Redis Stream source
//! and the existing ClickHouse sink knobs.

use std::time::Duration;

use anyhow::{Context, Result};

pub const DEFAULT_FLUSH_BATCH_SIZE: usize = 5_000;
pub const DEFAULT_FLUSH_INTERVAL_MS: u64 = 500;

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
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let clickhouse_host = env_or("CLICKHOUSE_HOST", "localhost");
        let clickhouse_port = env_parse("CLICKHOUSE_PORT", 8123_u16)?;
        let flush_batch_size = env_parse("FLUSH_BATCH_SIZE", DEFAULT_FLUSH_BATCH_SIZE)?;
        let flush_interval =
            Duration::from_millis(env_parse("FLUSH_INTERVAL_MS", DEFAULT_FLUSH_INTERVAL_MS)?);
        validate_writer_settings(flush_batch_size, flush_interval)?;

        Ok(Self {
            redis_url: require_env("REDIS_URL")?,
            redis_event_stream: env_or("REDIS_EVENT_STREAM", "polymarket:events:v3"),
            stream_consumer_group: env_or("EVENT_CONSUMER_GROUP", "polymarket-clickhouse-v3"),
            stream_consumer_name: env_or("EVENT_CONSUMER_NAME", "polymarket-clickhouse-v3-1"),
            pubsub_reconnect_delay: Duration::from_millis(env_parse(
                "STREAM_RECONNECT_DELAY_MS",
                2_000,
            )?),
            clickhouse_url: format!("http://{clickhouse_host}:{clickhouse_port}"),
            clickhouse_user: env_or("CLICKHOUSE_USER", "default"),
            clickhouse_password: env_or("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database: env_or("CLICKHOUSE_DATABASE", "default"),
            clickhouse_table: env_or("CLICKHOUSE_TABLE", "polymarket_orderbook_v3"),
            flush_batch_size,
            flush_interval,
        })
    }
}

fn require_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing required env var {name}"))
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    std::env::var(name).map_or(Ok(default), |value| {
        value
            .parse()
            .with_context(|| format!("parse env var {name}"))
    })
}

fn validate_writer_settings(batch_size: usize, flush_interval: Duration) -> Result<()> {
    anyhow::ensure!(batch_size > 0, "FLUSH_BATCH_SIZE must be positive");
    anyhow::ensure!(
        !flush_interval.is_zero(),
        "FLUSH_INTERVAL_MS must be positive"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::validate_writer_settings;

    #[test]
    fn writer_batch_and_interval_must_be_positive() {
        assert!(validate_writer_settings(0, Duration::from_millis(500)).is_err());
        assert!(validate_writer_settings(5_000, Duration::ZERO).is_err());
        assert!(validate_writer_settings(5_000, Duration::from_millis(500)).is_ok());
    }
}
