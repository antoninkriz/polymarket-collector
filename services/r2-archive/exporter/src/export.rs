use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use tokio::task;
use tracing::{error, info, warn};

use crate::archive::{Archive, LocalArchive};
use crate::clickhouse::{ClickHouseSource, EventSource};
use crate::config::Config;
use crate::event::{EventType, ensure_utc_hour};
use crate::parquet_file::FileStats;

#[derive(Clone)]
pub struct Exporter {
    cfg: Config,
    source: Arc<dyn EventSource>,
    archive: Arc<dyn Archive>,
}

impl Exporter {
    pub fn new(cfg: Config, source: Arc<dyn EventSource>, archive: Arc<dyn Archive>) -> Self {
        Self {
            cfg,
            source,
            archive,
        }
    }

    pub fn export_hour(&self, hour: DateTime<Utc>) -> Result<()> {
        self.export_hour_at(hour, Utc::now())
    }

    fn export_hour_at(&self, hour: DateTime<Utc>, created_at: DateTime<Utc>) -> Result<()> {
        ensure_utc_hour(hour)?;
        let prefix = hour_to_prefix(hour)?;
        info!(%prefix, "exporting receive-time hour");
        let mut files = BTreeMap::new();
        let mut total_rows = 0_u64;
        let mut hour_min_sequence = None;
        let mut hour_max_sequence = None;

        for event in EventType::ALL {
            let key = event_to_key(hour, event)?;
            let artifact = self
                .source
                .export_event(self.archive.as_ref(), &key, hour, event)
                .with_context(|| format!("export {key}"))?;
            let stats = artifact.stats.clone();
            self.archive
                .commit(&key, artifact.into_file())
                .with_context(|| format!("publish {key}"))?;
            update_hour_stats(
                &stats,
                &mut total_rows,
                &mut hour_min_sequence,
                &mut hour_max_sequence,
            )?;
            files.insert(
                event.as_str().to_owned(),
                ManifestFile {
                    file: key.clone(),
                    row_count: stats.row_count,
                    byte_size: stats.byte_size,
                    sha256: stats.sha256,
                    min_sequence: stats.min_sequence,
                    max_sequence: stats.max_sequence,
                    columns: stats.columns,
                    order_by: event
                        .sort_columns()
                        .iter()
                        .map(|column| (*column).to_owned())
                        .collect(),
                },
            );
            info!(
                %key,
                rows = stats.row_count,
                bytes = stats.byte_size,
                "published event file",
            );
        }

        let manifest = Manifest {
            hour_utc: format_datetime(hour),
            row_count: total_rows,
            min_sequence: hour_min_sequence,
            max_sequence: hour_max_sequence,
            files,
            source_table: self.cfg.clickhouse_table.clone(),
            created_at: format_datetime(created_at),
        };
        let manifest_data = serde_json::to_vec(&manifest).context("serialize hour manifest")?;
        let completion_key = hour_to_completion_key(hour)?;
        self.archive
            .put_bytes(&completion_key, &manifest_data)
            .with_context(|| format!("publish {completion_key}"))?;
        info!(
            %prefix,
            rows = total_rows,
            min_sequence = ?hour_min_sequence,
            max_sequence = ?hour_max_sequence,
            "completed receive-time hour",
        );
        Ok(())
    }

    pub fn backfill(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
        let Some(earliest) = self.source.earliest_hour()? else {
            warn!("no data in ClickHouse yet");
            return Ok(None);
        };
        let Some(latest) = self.latest_exportable_hour(now)? else {
            info!("no complete ClickHouse hour is exportable yet");
            return Ok(Some(earliest));
        };
        if latest < earliest {
            info!("no complete ClickHouse hour is exportable yet");
            return Ok(Some(earliest));
        }

        let mut hour = earliest;
        let mut exported = 0_u64;
        let mut existing = 0_u64;
        while hour <= latest {
            let completion_key = hour_to_completion_key(hour)?;
            if self.archive.exists(&completion_key)? {
                existing += 1;
            } else {
                self.export_hour(hour)?;
                exported += 1;
            }
            hour = add_hour(hour)?;
        }
        info!(
            earliest = %earliest,
            latest = %latest,
            exported,
            existing,
            "backfill complete",
        );
        Ok(Some(add_hour(latest)?))
    }

    pub fn steady_state_step(
        &self,
        now: DateTime<Utc>,
        next_hour: Option<DateTime<Utc>>,
    ) -> Result<Option<DateTime<Utc>>> {
        if now.minute() < self.cfg.export_delay_minutes {
            return Ok(next_hour);
        }
        let Some(latest) = self.latest_exportable_hour(now)? else {
            return Ok(next_hour);
        };
        let mut next_hour = match next_hour {
            Some(hour) => hour,
            None => match self.source.earliest_hour()? {
                Some(hour) => hour,
                None => return Ok(None),
            },
        };
        while next_hour <= latest {
            let completion_key = hour_to_completion_key(next_hour)?;
            if !self.archive.exists(&completion_key)? {
                self.export_hour(next_hour)?;
            }
            next_hour = add_hour(next_hour)?;
        }
        Ok(Some(next_hour))
    }

    fn latest_exportable_hour(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
        let Some(latest_received) = self.source.latest_received_hour()? else {
            return Ok(None);
        };
        latest_exportable_hour(now, latest_received, self.cfg.export_lag_hours).map(Some)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Manifest {
    pub hour_utc: String,
    pub row_count: u64,
    pub min_sequence: Option<u64>,
    pub max_sequence: Option<u64>,
    pub files: BTreeMap<String, ManifestFile>,
    pub source_table: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ManifestFile {
    pub file: String,
    pub row_count: u64,
    pub byte_size: u64,
    pub sha256: String,
    pub min_sequence: Option<u64>,
    pub max_sequence: Option<u64>,
    pub columns: Vec<String>,
    pub order_by: Vec<String>,
}

pub fn hour_to_prefix(hour: DateTime<Utc>) -> Result<String> {
    ensure_utc_hour(hour)?;
    Ok(hour.format("%Y-%m-%d/%H").to_string())
}

pub fn event_to_key(hour: DateTime<Utc>, event: EventType) -> Result<String> {
    Ok(format!("{}/{}.parquet", hour_to_prefix(hour)?, event))
}

pub fn hour_to_completion_key(hour: DateTime<Utc>) -> Result<String> {
    Ok(format!("{}/manifest.json", hour_to_prefix(hour)?))
}

fn format_datetime(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, false)
}

fn add_hour(hour: DateTime<Utc>) -> Result<DateTime<Utc>> {
    hour.checked_add_signed(ChronoDuration::hours(1))
        .context("UTC hour overflow")
}

fn latest_exportable_hour(
    now: DateTime<Utc>,
    latest_received: DateTime<Utc>,
    lag_hours: i64,
) -> Result<DateTime<Utc>> {
    let now_hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .context("truncate current UTC time to hour")?;
    ensure_utc_hour(latest_received)?;
    let wall_bound = now_hour
        .checked_sub_signed(ChronoDuration::hours(lag_hours))
        .context("wall-clock export bound overflow")?;
    let watermark_bound = latest_received
        .checked_sub_signed(ChronoDuration::hours(1))
        .context("ClickHouse watermark export bound overflow")?;
    Ok(wall_bound.min(watermark_bound))
}

fn update_hour_stats(
    stats: &FileStats,
    total_rows: &mut u64,
    hour_min_sequence: &mut Option<u64>,
    hour_max_sequence: &mut Option<u64>,
) -> Result<()> {
    *total_rows = total_rows
        .checked_add(stats.row_count)
        .context("hour row count overflow")?;
    if let Some(minimum) = stats.min_sequence {
        *hour_min_sequence = Some(hour_min_sequence.map_or(minimum, |value| value.min(minimum)));
    }
    if let Some(maximum) = stats.max_sequence {
        *hour_max_sequence = Some(hour_max_sequence.map_or(maximum, |value| value.max(maximum)));
    }
    Ok(())
}

pub async fn run(cfg: Config) -> Result<()> {
    let archive: Arc<dyn Archive> = Arc::new(LocalArchive::new(&cfg.local_export_dir)?);
    let source: Arc<dyn EventSource> = Arc::new(ClickHouseSource::new(cfg.clone())?);
    let exporter = Exporter::new(cfg.clone(), source, archive);

    info!(
        directory = %cfg.local_export_dir.display(),
        source_table = %cfg.clickhouse_table,
        "using local archive backend",
    );
    let initial = exporter.clone();
    let initial_result = task::spawn_blocking(move || initial.backfill(Utc::now())).await;
    let mut next_hour = match initial_result {
        Ok(Ok(next)) => next,
        Ok(Err(error)) if cfg.export_once => return Err(error),
        Err(error) if cfg.export_once => {
            return Err(error).context("join initial exporter task");
        }
        Ok(Err(error)) => {
            error!(%error, "initial backfill failed; daemon will retry");
            None
        }
        Err(error) => {
            error!(%error, "initial exporter task failed; daemon will retry");
            None
        }
    };
    if cfg.export_once {
        info!("one-shot export complete");
        return Ok(());
    }

    info!(
        check_interval_seconds = cfg.loop_check_interval.as_secs(),
        "entering steady-state export loop",
    );
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = tokio::time::sleep(cfg.loop_check_interval) => {
                let step = exporter.clone();
                let attempted = next_hour;
                match task::spawn_blocking(move || step.steady_state_step(Utc::now(), attempted)).await {
                    Ok(Ok(next)) => next_hour = next,
                    Ok(Err(error)) => error!(%error, "steady-state export failed; will retry"),
                    Err(error) => error!(%error, "steady-state exporter task failed; will retry"),
                }
            }
            signal = &mut shutdown => {
                signal?;
                info!("exporter shut down cleanly");
                return Ok(());
            }
        }
    }
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("listen for SIGTERM")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("listen for Ctrl-C")?,
            _ = terminate.recv() => {}
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("listen for Ctrl-C")
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Read, Write};
    use std::sync::Mutex;

    use anyhow::bail;
    use chrono::TimeZone;
    use sha2::{Digest, Sha256};
    use tempfile::{NamedTempFile, TempDir};

    use super::*;
    use crate::parquet_file::StagedArtifact;

    fn hour(value: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 13, value, 0, 0).unwrap()
    }

    fn test_config(root: &std::path::Path) -> Config {
        Config {
            clickhouse_url: "http://example.invalid:8123/".to_owned(),
            clickhouse_user: "default".to_owned(),
            clickhouse_password: String::new(),
            clickhouse_database: "default".to_owned(),
            clickhouse_table: "polymarket_orderbook_v3".to_owned(),
            local_export_dir: root.to_path_buf(),
            export_once: true,
            export_delay_minutes: 5,
            export_lag_hours: 1,
            loop_check_interval: std::time::Duration::from_secs(60),
            query_max_retries: 1,
            query_retry_delay: std::time::Duration::from_secs(1),
            metadata_query_timeout: std::time::Duration::from_secs(1),
            event_query_timeout: std::time::Duration::from_secs(1),
        }
    }

    struct MemoryArchive {
        directory: TempDir,
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        order: Mutex<Vec<String>>,
    }

    impl MemoryArchive {
        fn new() -> Self {
            Self {
                directory: TempDir::new().unwrap(),
                objects: Mutex::new(BTreeMap::new()),
                order: Mutex::new(Vec::new()),
            }
        }
    }

    impl Archive for MemoryArchive {
        fn stage(&self, _key: &str) -> Result<NamedTempFile> {
            Ok(NamedTempFile::new_in(self.directory.path())?)
        }

        fn commit(&self, key: &str, file: NamedTempFile) -> Result<()> {
            let mut data = Vec::new();
            File::open(file.path())?.read_to_end(&mut data)?;
            self.objects.lock().unwrap().insert(key.to_owned(), data);
            self.order.lock().unwrap().push(key.to_owned());
            Ok(())
        }

        fn put_bytes(&self, key: &str, data: &[u8]) -> Result<()> {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_owned(), data.to_vec());
            self.order.lock().unwrap().push(key.to_owned());
            Ok(())
        }

        fn exists(&self, key: &str) -> Result<bool> {
            Ok(self.objects.lock().unwrap().contains_key(key))
        }
    }

    struct FakeSource {
        earliest: Option<DateTime<Utc>>,
        latest: Option<DateTime<Utc>>,
        rows_per_event: u64,
    }

    impl EventSource for FakeSource {
        fn earliest_hour(&self) -> Result<Option<DateTime<Utc>>> {
            Ok(self.earliest)
        }

        fn latest_received_hour(&self) -> Result<Option<DateTime<Utc>>> {
            Ok(self.latest)
        }

        fn export_event(
            &self,
            archive: &dyn Archive,
            key: &str,
            _hour: DateTime<Utc>,
            event: EventType,
        ) -> Result<StagedArtifact> {
            let mut file = archive.stage(key)?;
            let data = event.as_str().as_bytes();
            file.write_all(data)?;
            file.flush()?;
            let minimum = (event as u64) * 10;
            Ok(StagedArtifact::from_parts(
                file,
                FileStats {
                    row_count: self.rows_per_event,
                    byte_size: data.len() as u64,
                    sha256: format!("{:x}", Sha256::digest(data)),
                    min_sequence: (self.rows_per_event > 0).then_some(minimum),
                    max_sequence: (self.rows_per_event > 0).then_some(minimum + 1),
                    columns: event
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().to_owned())
                        .collect(),
                },
            ))
        }
    }

    #[test]
    fn paths_are_utc_sorted_and_require_exact_hours() {
        assert_eq!(hour_to_prefix(hour(4)).unwrap(), "2026-08-13/04");
        assert_eq!(
            event_to_key(hour(4), EventType::PriceChange).unwrap(),
            "2026-08-13/04/price_change.parquet"
        );
        assert_eq!(
            hour_to_completion_key(hour(4)).unwrap(),
            "2026-08-13/04/manifest.json"
        );
        let invalid = hour(4) + ChronoDuration::minutes(1);
        assert!(hour_to_prefix(invalid).is_err());
    }

    #[test]
    fn manifest_is_last_and_records_every_file_including_empty_types() {
        let archive = Arc::new(MemoryArchive::new());
        let source = Arc::new(FakeSource {
            earliest: Some(hour(14)),
            latest: Some(hour(15)),
            rows_per_event: 0,
        });
        let exporter = Exporter::new(
            test_config(archive.directory.path()),
            source,
            archive.clone(),
        );
        exporter
            .export_hour_at(hour(14), hour(15) + ChronoDuration::minutes(1))
            .unwrap();

        let order = archive.order.lock().unwrap();
        assert_eq!(order.last().unwrap(), "2026-08-13/14/manifest.json");
        assert_eq!(order.len(), EventType::ALL.len() + 1);
        drop(order);
        let objects = archive.objects.lock().unwrap();
        let manifest: Manifest =
            serde_json::from_slice(&objects["2026-08-13/14/manifest.json"]).unwrap();
        assert_eq!(manifest.hour_utc, "2026-08-13T14:00:00+00:00");
        assert_eq!(manifest.created_at, "2026-08-13T15:01:00+00:00");
        assert_eq!(manifest.row_count, 0);
        assert_eq!(manifest.min_sequence, None);
        assert_eq!(manifest.max_sequence, None);
        assert_eq!(manifest.files.len(), 7);
        assert_eq!(
            manifest.files["new_market"].order_by,
            ["market", "sequence"]
        );
        assert_eq!(
            manifest.files["book"].order_by,
            ["market", "asset_id", "sequence"]
        );
        for event in EventType::ALL {
            assert!(objects.contains_key(&event_to_key(hour(14), event).unwrap()));
        }
    }

    #[test]
    fn backfill_checks_exact_manifests_and_preserves_next_hour() {
        let archive = Arc::new(MemoryArchive::new());
        archive
            .put_bytes(
                &hour_to_completion_key(hour(10)).unwrap(),
                br#"{"complete":true}"#,
            )
            .unwrap();
        let source = Arc::new(FakeSource {
            earliest: Some(hour(10)),
            latest: Some(hour(13)),
            rows_per_event: 1,
        });
        let exporter = Exporter::new(
            test_config(archive.directory.path()),
            source,
            archive.clone(),
        );
        // At 14:10 the wall and receive-watermark bounds both allow hour 12.
        let next = exporter
            .backfill(hour(14) + ChronoDuration::minutes(10))
            .unwrap();
        assert_eq!(next, Some(hour(13)));
        assert!(
            archive
                .objects
                .lock()
                .unwrap()
                .contains_key("2026-08-13/12/manifest.json")
        );
        assert_eq!(
            archive
                .order
                .lock()
                .unwrap()
                .iter()
                .filter(|key| key.as_str() == "2026-08-13/10/manifest.json")
                .count(),
            1
        );
    }

    #[test]
    fn failed_event_never_publishes_manifest() {
        struct FailingSource;
        impl EventSource for FailingSource {
            fn earliest_hour(&self) -> Result<Option<DateTime<Utc>>> {
                Ok(Some(hour(1)))
            }
            fn latest_received_hour(&self) -> Result<Option<DateTime<Utc>>> {
                Ok(Some(hour(2)))
            }
            fn export_event(
                &self,
                _archive: &dyn Archive,
                _key: &str,
                _hour: DateTime<Utc>,
                _event: EventType,
            ) -> Result<StagedArtifact> {
                bail!("failed query")
            }
        }

        let archive = Arc::new(MemoryArchive::new());
        let exporter = Exporter::new(
            test_config(archive.directory.path()),
            Arc::new(FailingSource),
            archive.clone(),
        );
        assert!(exporter.export_hour(hour(1)).is_err());
        assert!(
            !archive
                .objects
                .lock()
                .unwrap()
                .contains_key("2026-08-13/01/manifest.json")
        );
    }
}
