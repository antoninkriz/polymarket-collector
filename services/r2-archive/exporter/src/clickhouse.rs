use std::io::{Read, Take};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use arrow_ipc::reader::StreamReader;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use reqwest::StatusCode;
use reqwest::blocking::{Client, Response};
use tracing::warn;

use crate::archive::Archive;
use crate::config::Config;
use crate::event::{EventType, build_event_query, ensure_utc_hour};
use crate::parquet_file::{StagedArtifact, write_arrow_batches};

const MAX_ERROR_BODY_BYTES: u64 = 64 * 1024;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

pub trait EventSource: Send + Sync {
    fn earliest_hour(&self) -> Result<Option<DateTime<Utc>>>;
    fn latest_received_hour(&self) -> Result<Option<DateTime<Utc>>>;
    fn export_event(
        &self,
        archive: &dyn Archive,
        key: &str,
        hour: DateTime<Utc>,
        event: EventType,
    ) -> Result<StagedArtifact>;
}

#[derive(Clone)]
pub struct ClickHouseSource {
    cfg: Config,
    http: Client,
}

impl ClickHouseSource {
    pub fn new(cfg: Config) -> Result<Self> {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .context("build ClickHouse HTTP client")?;
        Ok(Self { cfg, http })
    }

    fn query_hour(&self, aggregate: &str) -> Result<Option<DateTime<Utc>>> {
        let query = format!(
            "SELECT toStartOfHour({aggregate}(timestamp_received)) FROM {} FORMAT TabSeparated",
            self.cfg.clickhouse_table
        );
        let text = self.query_text_with_retry(&query)?;
        let text = text.trim();
        if text.is_empty() || text.starts_with("1970") {
            return Ok(None);
        }
        let naive = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
            .with_context(|| format!("parse ClickHouse hour {text:?}"))?;
        let hour = Utc.from_utc_datetime(&naive);
        ensure_utc_hour(hour)?;
        Ok(Some(hour))
    }

    fn query_text_with_retry(&self, query: &str) -> Result<String> {
        retry(
            self.cfg.query_max_retries,
            self.cfg.query_retry_delay,
            "ClickHouse metadata query",
            || {
                let response = self.send(query, self.cfg.metadata_query_timeout)?;
                read_response_text(response)
            },
        )
    }

    fn export_event_once(
        &self,
        archive: &dyn Archive,
        key: &str,
        hour: DateTime<Utc>,
        event: EventType,
    ) -> Result<StagedArtifact> {
        let query = build_event_query(hour, event, &self.cfg.clickhouse_table)?;
        let response = self.send(&query, self.cfg.event_query_timeout)?;
        let reader = StreamReader::try_new_buffered(response, None)
            .with_context(|| format!("open {event} ClickHouse Arrow stream"))?;
        let staged = archive.stage(key)?;
        write_arrow_batches(event, staged, reader)
    }

    fn send(&self, query: &str, timeout: Duration) -> Result<Response> {
        let response = self
            .http
            .post(&self.cfg.clickhouse_url)
            .basic_auth(
                &self.cfg.clickhouse_user,
                Some(&self.cfg.clickhouse_password),
            )
            .query(&[("database", self.cfg.clickhouse_database.as_str())])
            .timeout(timeout)
            .body(query.to_owned())
            .send()
            .context("send ClickHouse query")?;
        if response.status().is_success() {
            return Ok(response);
        }
        response_error(response)
    }
}

impl EventSource for ClickHouseSource {
    fn earliest_hour(&self) -> Result<Option<DateTime<Utc>>> {
        self.query_hour("min")
    }

    fn latest_received_hour(&self) -> Result<Option<DateTime<Utc>>> {
        self.query_hour("max")
    }

    fn export_event(
        &self,
        archive: &dyn Archive,
        key: &str,
        hour: DateTime<Utc>,
        event: EventType,
    ) -> Result<StagedArtifact> {
        retry(
            self.cfg.query_max_retries,
            self.cfg.query_retry_delay,
            &format!("ClickHouse {event} export"),
            || self.export_event_once(archive, key, hour, event),
        )
    }
}

fn retry<T>(
    attempts: usize,
    initial_delay: Duration,
    operation: &str,
    mut callback: impl FnMut() -> Result<T>,
) -> Result<T> {
    let mut delay = initial_delay;
    for attempt in 1..=attempts {
        match callback() {
            Ok(value) => return Ok(value),
            Err(error) if attempt < attempts => {
                warn!(
                    %error,
                    attempt,
                    attempts,
                    retry_delay_ms = delay.as_millis() as u64,
                    operation,
                    "operation failed; retrying",
                );
                thread::sleep(delay);
                delay = delay.saturating_mul(2).min(MAX_RETRY_DELAY);
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("{operation} failed after {attempts} attempts"));
            }
        }
    }
    unreachable!("positive retry count is validated by Config")
}

fn response_error(response: Response) -> Result<Response> {
    let status = response.status();
    let body = read_limited(response.take(MAX_ERROR_BODY_BYTES)).unwrap_or_default();
    bail!("ClickHouse query failed: {status} {}", body.trim())
}

fn read_response_text(response: Response) -> Result<String> {
    let status = response.status();
    if status != StatusCode::OK {
        return response_error(response).map(|_| unreachable!());
    }
    read_limited(response.take(MAX_ERROR_BODY_BYTES)).context("read ClickHouse text response")
}

fn read_limited(mut response: Take<Response>) -> std::io::Result<String> {
    let mut bytes = Vec::new();
    response.read_to_end(&mut bytes)?;
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn retry_is_bounded_and_recovers() {
        let count = AtomicUsize::new(0);
        let value = retry(3, Duration::from_nanos(1), "test", || {
            let attempt = count.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                bail!("temporary")
            }
            Ok(42)
        })
        .unwrap();
        assert_eq!(value, 42);
        assert_eq!(count.load(Ordering::SeqCst), 3);

        let count = AtomicUsize::new(0);
        let error = retry::<()>(2, Duration::from_nanos(1), "test", || {
            count.fetch_add(1, Ordering::SeqCst);
            bail!("permanent")
        })
        .unwrap_err();
        assert!(error.to_string().contains("after 2 attempts"));
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }
}
