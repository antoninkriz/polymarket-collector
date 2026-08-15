use std::io::{Read, Take};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use arrow_ipc::reader::StreamReader;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use reqwest::StatusCode;
use reqwest::blocking::{Client, Response};
use tracing::warn;

use crate::archive::Archive;
use crate::config::{Config, validate_identifier};
use crate::event::{EventType, build_event_query, ensure_utc_hour};
use crate::parquet_file::{StagedArtifact, write_arrow_batches};

const MAX_ERROR_BODY_BYTES: u64 = 64 * 1024;
const MAX_METADATA_BODY_BYTES: u64 = 2 * 1024 * 1024;
const QUERY_MAX_ATTEMPTS: usize = 10;
const QUERY_INITIAL_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
const METADATA_QUERY_TIMEOUT: Duration = Duration::from_secs(60);
const EVENT_QUERY_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivePartition {
    pub hour: DateTime<Utc>,
    pub partition_id: String,
}

pub trait EventSource: Send + Sync {
    fn earliest_hour(&self) -> Result<Option<DateTime<Utc>>>;
    fn latest_received_hour(&self) -> Result<Option<DateTime<Utc>>>;
    fn active_partitions(&self) -> Result<Vec<ActivePartition>>;
    fn drop_partition(&self, partition: &ActivePartition) -> Result<()>;
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
}

impl ClickHouseSource {
    pub fn new(cfg: Config) -> Result<Self> {
        Ok(Self { cfg })
    }

    fn query_hour(&self, aggregate: &str) -> Result<Option<DateTime<Utc>>> {
        let query = format!(
            "SELECT toStartOfHour({aggregate}(timestamp_received)) FROM {} FORMAT TabSeparated",
            self.cfg.clickhouse_table
        );
        let text = self.query_text_with_retry(&query, "ClickHouse hour query")?;
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

    fn query_text_with_retry(&self, query: &str, operation: &str) -> Result<String> {
        retry(
            QUERY_MAX_ATTEMPTS,
            QUERY_INITIAL_RETRY_DELAY,
            operation,
            || {
                let response = self.send(query, METADATA_QUERY_TIMEOUT)?;
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
        let response = self.send(&query, EVENT_QUERY_TIMEOUT)?;
        let reader = StreamReader::try_new_buffered(response, None)
            .with_context(|| format!("open {event} ClickHouse Arrow stream"))?;
        let staged = archive.stage(key)?;
        write_arrow_batches(event, staged, reader)
    }

    fn send(&self, query: &str, timeout: Duration) -> Result<Response> {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .context("build ClickHouse HTTP client")?;
        let response = http
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

    fn active_partitions(&self) -> Result<Vec<ActivePartition>> {
        let query = build_active_partitions_query(
            &self.cfg.clickhouse_database,
            &self.cfg.clickhouse_table,
        )?;
        let text = self.query_text_with_retry(&query, "ClickHouse active partition query")?;
        parse_active_partitions(&text)
    }

    fn drop_partition(&self, partition: &ActivePartition) -> Result<()> {
        let query = build_drop_partition_query(&self.cfg.clickhouse_table, partition)?;
        self.query_text_with_retry(&query, "ClickHouse partition drop")?;
        Ok(())
    }

    fn export_event(
        &self,
        archive: &dyn Archive,
        key: &str,
        hour: DateTime<Utc>,
        event: EventType,
    ) -> Result<StagedArtifact> {
        retry(
            QUERY_MAX_ATTEMPTS,
            QUERY_INITIAL_RETRY_DELAY,
            &format!("ClickHouse {event} export"),
            || self.export_event_once(archive, key, hour, event),
        )
    }
}

fn build_active_partitions_query(database: &str, table: &str) -> Result<String> {
    validate_identifier("CLICKHOUSE_DATABASE", database)?;
    validate_identifier("CLICKHOUSE_TABLE", table)?;
    Ok(format!(
        "SELECT partition, partition_id FROM system.parts \
         WHERE active = 1 AND database = '{database}' AND table = '{table}' \
         GROUP BY partition, partition_id ORDER BY partition FORMAT TabSeparated"
    ))
}

fn parse_active_partitions(text: &str) -> Result<Vec<ActivePartition>> {
    let mut partitions = Vec::new();
    for line in text.lines() {
        ensure!(!line.is_empty(), "empty ClickHouse partition row");
        let (partition, partition_id) = line
            .split_once('\t')
            .with_context(|| format!("partition row has no tab separator: {line:?}"))?;
        ensure!(
            !partition_id.contains('\t'),
            "partition row has extra columns: {line:?}"
        );
        let naive = NaiveDateTime::parse_from_str(partition, "%Y-%m-%d %H:%M:%S")
            .with_context(|| format!("parse ClickHouse UTC partition {partition:?}"))?;
        let hour = Utc.from_utc_datetime(&naive);
        ensure!(
            hour.format("%Y-%m-%d %H:%M:%S").to_string() == partition,
            "ClickHouse partition is not a canonical UTC hour: {partition:?}"
        );
        validate_partition_identity(hour, partition_id)?;
        partitions.push(ActivePartition {
            hour,
            partition_id: partition_id.to_owned(),
        });
    }
    Ok(partitions)
}

fn validate_partition_identity(hour: DateTime<Utc>, partition_id: &str) -> Result<()> {
    ensure_utc_hour(hour)?;
    ensure!(
        !partition_id.is_empty() && partition_id.bytes().all(|byte| byte.is_ascii_digit()),
        "ClickHouse partition ID must contain decimal digits only: {partition_id:?}"
    );
    let timestamp =
        u64::try_from(hour.timestamp()).context("partition hour predates Unix epoch")?;
    ensure!(
        partition_id == timestamp.to_string(),
        "ClickHouse partition ID {partition_id:?} does not equal UTC hour Unix seconds {timestamp}"
    );
    Ok(())
}

fn build_drop_partition_query(table: &str, partition: &ActivePartition) -> Result<String> {
    validate_identifier("CLICKHOUSE_TABLE", table)?;
    validate_partition_identity(partition.hour, &partition.partition_id)?;
    Ok(format!(
        "ALTER TABLE {table} DROP PARTITION ID '{}'",
        partition.partition_id
    ))
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
    read_limited(response.take(MAX_METADATA_BODY_BYTES)).context("read ClickHouse text response")
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
    use crate::config::ExportBackend;

    #[tokio::test(flavor = "multi_thread")]
    async fn source_can_be_constructed_and_dropped_on_async_worker() {
        let source = ClickHouseSource::new(Config {
            clickhouse_url: "http://example.invalid:8123/".to_owned(),
            clickhouse_user: "default".to_owned(),
            clickhouse_password: String::new(),
            clickhouse_database: "default".to_owned(),
            clickhouse_table: "polymarket_orderbook_v3".to_owned(),
            export_backend: ExportBackend::Local {
                root: "/var/lib/archive".into(),
            },
            export_once: true,
            clickhouse_retention_hours: 3,
        })
        .unwrap();
        tokio::task::yield_now().await;
        drop(source);
    }

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

    #[test]
    fn active_partition_query_is_scoped_to_validated_table() {
        let query = build_active_partitions_query("market_data", "events_v3").unwrap();
        assert!(query.contains("FROM system.parts"));
        assert!(query.contains("active = 1"));
        assert!(query.contains("database = 'market_data'"));
        assert!(query.contains("table = 'events_v3'"));
        assert!(query.contains("GROUP BY partition, partition_id"));
        assert!(build_active_partitions_query("default; DROP", "events").is_err());
        assert!(build_active_partitions_query("default", "events' OR 1=1").is_err());
    }

    #[test]
    fn partition_rows_and_drop_queries_require_canonical_unix_hour_ids() {
        let hour = Utc.with_ymd_and_hms(2026, 8, 14, 14, 0, 0).unwrap();
        let partition_id = hour.timestamp().to_string();
        let partitions =
            parse_active_partitions(&format!("2026-08-14 14:00:00\t{partition_id}\n")).unwrap();
        assert_eq!(
            partitions,
            [ActivePartition {
                hour,
                partition_id: partition_id.clone(),
            }]
        );
        assert_eq!(
            build_drop_partition_query("events_v3", &partitions[0]).unwrap(),
            format!("ALTER TABLE events_v3 DROP PARTITION ID '{partition_id}'")
        );

        assert!(parse_active_partitions("2026-08-14 14:00:00\t123\n").is_err());
        assert!(parse_active_partitions("2026-08-14 14:00:00\t1786716000x\n").is_err());
        assert!(parse_active_partitions("2026-08-14 14:00:00\t01786716000\n").is_err());
        assert!(parse_active_partitions("2026-08-14 14:00:01\t1786716001\n").is_err());
        assert!(parse_active_partitions("2026-8-14 14:00:00\t1786716000\n").is_err());
        assert!(parse_active_partitions("2026-08-14 14:00:00\t1786716000\textra\n").is_err());

        let invalid = ActivePartition {
            hour,
            partition_id: "1786716000' OR 1=1".to_owned(),
        };
        assert!(build_drop_partition_query("events_v3", &invalid).is_err());
        assert!(build_drop_partition_query("events; DROP", &partitions[0]).is_err());
    }
}
