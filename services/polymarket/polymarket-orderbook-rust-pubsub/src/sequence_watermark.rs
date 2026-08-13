//! Recover the publisher generation floor from durable ClickHouse data.
//!
//! Redis normally preserves and increments the generation. ClickHouse is the
//! recovery authority if Redis is restored without that key: the next lease
//! must use a generation greater than every sequence already committed.

use std::time::Duration;

use anyhow::{ensure, Context, Result};
use reqwest::Client;

use polymarket_orderbook_rust::record::sequence_generation;

use crate::config::Config;

const QUERY_TIMEOUT: Duration = Duration::from_secs(15);

pub async fn clickhouse_generation_floor(cfg: &Config) -> Result<u64> {
    validate_identifier(&cfg.clickhouse_table).context("invalid ClickHouse table name")?;
    let client = Client::builder()
        .timeout(QUERY_TIMEOUT)
        .build()
        .context("build ClickHouse watermark client")?;
    let table = format!("`{}`", cfg.clickhouse_table);

    let exists = query_text(&client, cfg, &format!("EXISTS TABLE {table}"))
        .await
        .context("check ClickHouse sequence table")?;
    match exists.trim() {
        "0" => return Ok(0),
        "1" => {}
        value => anyhow::bail!("unexpected ClickHouse EXISTS response: {value:?}"),
    }

    let maximum = query_text(&client, cfg, &format!("SELECT max(sequence) FROM {table}"))
        .await
        .context("query ClickHouse sequence watermark")?;
    let sequence = maximum
        .trim()
        .parse::<u64>()
        .with_context(|| format!("invalid ClickHouse max sequence: {maximum:?}"))?;
    Ok(sequence_generation(sequence))
}

async fn query_text(client: &Client, cfg: &Config, sql: &str) -> Result<String> {
    let response = client
        .post(&cfg.clickhouse_url)
        .basic_auth(&cfg.clickhouse_user, Some(&cfg.clickhouse_password))
        .query(&[("database", cfg.clickhouse_database.as_str())])
        .body(format!("{sql} FORMAT TabSeparated"))
        .send()
        .await
        .context("send ClickHouse watermark query")?;
    let status = response.status();
    let body = response.text().await.context("read ClickHouse response")?;
    ensure!(
        status.is_success(),
        "ClickHouse watermark query failed: {status} {body}"
    );
    Ok(body)
}

fn validate_identifier(value: &str) -> Result<()> {
    ensure!(!value.is_empty(), "identifier is empty");
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
        "identifier must contain only ASCII letters, digits, and underscores",
    );
    ensure!(
        value.as_bytes()[0].is_ascii_alphabetic() || value.as_bytes()[0] == b'_',
        "identifier must start with an ASCII letter or underscore",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_floor_uses_high_sequence_bits() {
        assert_eq!(sequence_generation(0), 0);
        assert_eq!(sequence_generation((42_u64 << 48) + 123), 42);
    }

    #[test]
    fn clickhouse_identifier_validation_is_strict() {
        for valid in ["events", "_events", "events_v3", "Events123"] {
            validate_identifier(valid).unwrap();
        }
        for invalid in ["", "3events", "db.events", "events-v3", "events` FINAL"] {
            assert!(
                validate_identifier(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }
}
