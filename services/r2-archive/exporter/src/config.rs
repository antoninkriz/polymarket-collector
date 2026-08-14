use std::env;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};

pub const DEFAULT_EXPORT_DELAY_MINUTES: u32 = 5;
pub const DEFAULT_EXPORT_LAG_HOURS: i64 = 1;
pub const DEFAULT_LOOP_CHECK_INTERVAL_SECONDS: u64 = 60;
pub const DEFAULT_QUERY_MAX_RETRIES: usize = 10;
pub const DEFAULT_QUERY_RETRY_DELAY_SECONDS: u64 = 1;

#[derive(Clone, Debug)]
pub struct Config {
    pub clickhouse_url: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,
    pub clickhouse_database: String,
    pub clickhouse_table: String,
    pub local_export_dir: PathBuf,
    pub export_once: bool,
    pub export_delay_minutes: u32,
    pub export_lag_hours: i64,
    pub loop_check_interval: Duration,
    pub query_max_retries: usize,
    pub query_retry_delay: Duration,
    pub metadata_query_timeout: Duration,
    pub event_query_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let host = value_or(&mut lookup, "CLICKHOUSE_HOST", "localhost");
        ensure!(!host.trim().is_empty(), "CLICKHOUSE_HOST must not be empty");
        let port = parse_or::<u16>(&mut lookup, "CLICKHOUSE_PORT", 8123)?;
        let database = value_or(&mut lookup, "CLICKHOUSE_DATABASE", "default");
        let table = value_or(&mut lookup, "CLICKHOUSE_TABLE", "polymarket_orderbook_v3");
        validate_identifier("CLICKHOUSE_DATABASE", &database)?;
        validate_identifier("CLICKHOUSE_TABLE", &table)?;

        let backend = value_or(&mut lookup, "EXPORT_BACKEND", "local");
        if !backend.eq_ignore_ascii_case("local") {
            bail!("this exporter build currently supports only EXPORT_BACKEND=local");
        }

        let export_delay_minutes = parse_or(
            &mut lookup,
            "EXPORT_DELAY_MINUTES",
            DEFAULT_EXPORT_DELAY_MINUTES,
        )?;
        ensure!(
            export_delay_minutes < 60,
            "EXPORT_DELAY_MINUTES must be less than 60"
        );
        let export_lag_hours = parse_or(&mut lookup, "EXPORT_LAG_HOURS", DEFAULT_EXPORT_LAG_HOURS)?;
        ensure!(export_lag_hours >= 1, "EXPORT_LAG_HOURS must be positive");
        let query_max_retries =
            parse_or(&mut lookup, "QUERY_MAX_RETRIES", DEFAULT_QUERY_MAX_RETRIES)?;
        ensure!(query_max_retries > 0, "QUERY_MAX_RETRIES must be positive");

        let local_export_dir = value_or(&mut lookup, "LOCAL_EXPORT_DIR", "/exports");
        ensure!(
            !local_export_dir.trim().is_empty(),
            "LOCAL_EXPORT_DIR must not be empty"
        );
        let loop_check_interval_seconds = parse_or(
            &mut lookup,
            "LOOP_CHECK_INTERVAL_SECONDS",
            DEFAULT_LOOP_CHECK_INTERVAL_SECONDS,
        )?;
        ensure!(
            loop_check_interval_seconds > 0,
            "LOOP_CHECK_INTERVAL_SECONDS must be positive"
        );
        let query_retry_delay_seconds = parse_or(
            &mut lookup,
            "QUERY_RETRY_DELAY_SECONDS",
            DEFAULT_QUERY_RETRY_DELAY_SECONDS,
        )?;
        ensure!(
            query_retry_delay_seconds > 0,
            "QUERY_RETRY_DELAY_SECONDS must be positive"
        );
        let metadata_query_timeout_seconds =
            parse_or(&mut lookup, "METADATA_QUERY_TIMEOUT_SECONDS", 60_u64)?;
        ensure!(
            metadata_query_timeout_seconds > 0,
            "METADATA_QUERY_TIMEOUT_SECONDS must be positive"
        );
        let event_query_timeout_seconds =
            parse_or(&mut lookup, "EVENT_QUERY_TIMEOUT_SECONDS", 600_u64)?;
        ensure!(
            event_query_timeout_seconds > 0,
            "EVENT_QUERY_TIMEOUT_SECONDS must be positive"
        );

        Ok(Self {
            clickhouse_url: format!("http://{host}:{port}/"),
            clickhouse_user: value_or(&mut lookup, "CLICKHOUSE_USER", "default"),
            clickhouse_password: value_or(&mut lookup, "CLICKHOUSE_PASSWORD", ""),
            clickhouse_database: database,
            clickhouse_table: table,
            local_export_dir: PathBuf::from(local_export_dir),
            export_once: parse_bool(&mut lookup, "EXPORT_ONCE", false)?,
            export_delay_minutes,
            export_lag_hours,
            loop_check_interval: Duration::from_secs(loop_check_interval_seconds),
            query_max_retries,
            query_retry_delay: Duration::from_secs(query_retry_delay_seconds),
            metadata_query_timeout: Duration::from_secs(metadata_query_timeout_seconds),
            event_query_timeout: Duration::from_secs(event_query_timeout_seconds),
        })
    }
}

fn value_or(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str, default: &str) -> String {
    lookup(name).unwrap_or_else(|| default.to_owned())
}

fn parse_or<T>(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match lookup(name) {
        Some(value) => value
            .parse()
            .with_context(|| format!("parse {name}={value:?}")),
        None => Ok(default),
    }
}

fn parse_bool(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: bool,
) -> Result<bool> {
    let Some(value) = lookup(name) else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => bail!("parse {name}={value:?} as boolean"),
    }
}

pub fn validate_identifier(name: &str, value: &str) -> Result<()> {
    let mut characters = value.chars();
    let valid_start = characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic());
    let valid_tail =
        characters.all(|character| character == '_' || character.is_ascii_alphanumeric());
    ensure!(
        valid_start && valid_tail,
        "{name} must be one unquoted ClickHouse identifier, got {value:?}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn config(values: &[(&str, &str)]) -> Result<Config> {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>();
        Config::from_lookup(|name| values.get(name).cloned())
    }

    #[test]
    fn local_defaults_and_overrides_are_typed() {
        let defaults = config(&[]).unwrap();
        assert_eq!(defaults.clickhouse_url, "http://localhost:8123/");
        assert_eq!(defaults.clickhouse_database, "default");
        assert_eq!(defaults.clickhouse_table, "polymarket_orderbook_v3");
        assert_eq!(defaults.local_export_dir, PathBuf::from("/exports"));
        assert!(!defaults.export_once);
        assert_eq!(defaults.export_delay_minutes, 5);
        assert_eq!(defaults.export_lag_hours, 1);
        assert_eq!(defaults.query_max_retries, 10);

        let overridden = config(&[
            ("CLICKHOUSE_HOST", "clickhouse"),
            ("CLICKHOUSE_PORT", "9000"),
            ("CLICKHOUSE_DATABASE", "archive"),
            ("CLICKHOUSE_TABLE", "events_v3"),
            ("LOCAL_EXPORT_DIR", "/tmp/archive"),
            ("EXPORT_ONCE", "yes"),
            ("QUERY_MAX_RETRIES", "3"),
        ])
        .unwrap();
        assert_eq!(overridden.clickhouse_url, "http://clickhouse:9000/");
        assert_eq!(overridden.clickhouse_database, "archive");
        assert_eq!(overridden.clickhouse_table, "events_v3");
        assert_eq!(overridden.local_export_dir, PathBuf::from("/tmp/archive"));
        assert!(overridden.export_once);
        assert_eq!(overridden.query_max_retries, 3);
    }

    #[test]
    fn unsupported_backend_and_unsafe_identifiers_are_rejected() {
        assert!(config(&[("EXPORT_BACKEND", "r2")]).is_err());
        assert!(config(&[("CLICKHOUSE_TABLE", "events; DROP TABLE events")]).is_err());
        assert!(config(&[("CLICKHOUSE_TABLE", "default.events")]).is_err());
        assert!(config(&[("CLICKHOUSE_DATABASE", "9default")]).is_err());
        assert!(config(&[("EXPORT_DELAY_MINUTES", "60")]).is_err());
        assert!(config(&[("QUERY_MAX_RETRIES", "0")]).is_err());
        assert!(config(&[("QUERY_RETRY_DELAY_SECONDS", "0")]).is_err());
        assert!(config(&[("LOOP_CHECK_INTERVAL_SECONDS", "0")]).is_err());
        assert!(config(&[("METADATA_QUERY_TIMEOUT_SECONDS", "0")]).is_err());
        assert!(config(&[("EVENT_QUERY_TIMEOUT_SECONDS", "0")]).is_err());
        assert!(config(&[("LOCAL_EXPORT_DIR", "")]).is_err());
        assert!(config(&[("EXPORT_ONCE", "sometimes")]).is_err());
    }
}
