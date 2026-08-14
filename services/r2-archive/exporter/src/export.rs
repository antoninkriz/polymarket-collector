use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Duration as ChronoDuration, Timelike, Utc};
use serde::{Deserialize, Serialize};
use tokio::task;
use tracing::{error, info, warn};

use crate::archive::{Archive, build_archive};
use crate::clickhouse::{ActivePartition, ClickHouseSource, EventSource};
use crate::config::{Config, ExportBackend};
use crate::event::{EventType, ensure_utc_hour};
use crate::parquet_file::FileStats;

const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

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
        let result = self.backfill_inner(now);
        if result.is_ok() {
            self.prune_archived_partitions_best_effort(now);
        }
        result
    }

    fn backfill_inner(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
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
        let result = self.steady_state_step_inner(now, next_hour);
        if result.is_ok() {
            self.prune_archived_partitions_best_effort(now);
        }
        result
    }

    fn steady_state_step_inner(
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

    fn prune_archived_partitions_best_effort(&self, now: DateTime<Utc>) {
        if let Err(error) = self.prune_archived_partitions(now) {
            warn!(%error, "ClickHouse partition cleanup failed; will retry");
        }
    }

    fn prune_archived_partitions(&self, now: DateTime<Utc>) -> Result<()> {
        let retention_hours = self.cfg.clickhouse_retention_hours;
        if retention_hours == 0 {
            return Ok(());
        }

        let partitions = self.source.active_partitions()?;
        let Some(newest_hour) = partitions.iter().map(|partition| partition.hour).max() else {
            return Ok(());
        };
        let cutoff = now
            .checked_sub_signed(ChronoDuration::hours(i64::from(retention_hours)))
            .context("ClickHouse retention cutoff overflow")?;

        for partition in partitions {
            if partition.hour == newest_hour {
                continue;
            }
            let partition_end = add_hour(partition.hour)?;
            if partition_end > cutoff {
                continue;
            }
            self.prune_partition_if_archived(&partition);
        }
        Ok(())
    }

    fn prune_partition_if_archived(&self, partition: &ActivePartition) {
        let completion_key = match hour_to_completion_key(partition.hour) {
            Ok(key) => key,
            Err(error) => {
                warn!(
                    %error,
                    partition_id = %partition.partition_id,
                    "retaining ClickHouse partition with invalid UTC hour",
                );
                return;
            }
        };
        let manifest_data = match self.archive.get(&completion_key, MAX_MANIFEST_BYTES) {
            Ok(Some(data)) => data,
            Ok(None) => {
                warn!(
                    %completion_key,
                    partition_id = %partition.partition_id,
                    "retaining ClickHouse partition without archive manifest",
                );
                return;
            }
            Err(error) => {
                warn!(
                    %error,
                    %completion_key,
                    partition_id = %partition.partition_id,
                    "retaining ClickHouse partition after manifest read failure",
                );
                return;
            }
        };
        let manifest = match serde_json::from_slice::<Manifest>(&manifest_data) {
            Ok(manifest) => manifest,
            Err(error) => {
                warn!(
                    %error,
                    %completion_key,
                    partition_id = %partition.partition_id,
                    "retaining ClickHouse partition with malformed manifest",
                );
                return;
            }
        };
        if let Err(error) = validate_manifest(&manifest, partition.hour, &self.cfg.clickhouse_table)
        {
            warn!(
                %error,
                %completion_key,
                partition_id = %partition.partition_id,
                "retaining ClickHouse partition with mismatched manifest",
            );
            return;
        }
        if let Err(error) = self.validate_manifest_objects(&manifest) {
            warn!(
                %error,
                %completion_key,
                partition_id = %partition.partition_id,
                "retaining ClickHouse partition with incomplete archive objects",
            );
            return;
        }
        if let Err(error) = self.source.drop_partition(partition) {
            warn!(
                %error,
                %completion_key,
                partition_id = %partition.partition_id,
                "failed to drop archived ClickHouse partition; will retry",
            );
            return;
        }
        info!(
            %completion_key,
            partition_id = %partition.partition_id,
            partition_hour = %partition.hour,
            "dropped archived ClickHouse partition",
        );
    }

    fn validate_manifest_objects(&self, manifest: &Manifest) -> Result<()> {
        for event in EventType::ALL {
            let file = manifest
                .files
                .get(event.as_str())
                .context("validated manifest file disappeared")?;
            ensure!(
                self.archive
                    .exists(&file.file)
                    .with_context(|| format!("check archive object {}", file.file))?,
                "archive object {} is missing",
                file.file
            );
        }
        Ok(())
    }

    fn latest_exportable_hour(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
        let Some(latest_received) = self.source.latest_received_hour()? else {
            return Ok(None);
        };
        latest_exportable_hour(now, latest_received, self.cfg.export_lag_hours).map(Some)
    }
}

fn validate_manifest(manifest: &Manifest, hour: DateTime<Utc>, source_table: &str) -> Result<()> {
    ensure_utc_hour(hour)?;
    let expected_hour = format_datetime(hour);
    ensure!(
        manifest.hour_utc == expected_hour,
        "manifest hour_utc {:?} does not match {expected_hour:?}",
        manifest.hour_utc
    );
    ensure!(
        manifest.source_table == source_table,
        "manifest source_table {:?} does not match {source_table:?}",
        manifest.source_table
    );
    ensure!(
        manifest.files.len() == EventType::ALL.len(),
        "manifest has {} event files instead of {}",
        manifest.files.len(),
        EventType::ALL.len()
    );
    let mut total_rows = 0_u64;
    let mut min_sequence = None;
    let mut max_sequence = None;
    for event in EventType::ALL {
        let event_name = event.as_str();
        let file = manifest
            .files
            .get(event_name)
            .with_context(|| format!("manifest is missing {event_name} file"))?;
        let expected_key = event_to_key(hour, event)?;
        ensure!(
            file.file == expected_key,
            "manifest {event_name} file {:?} does not match {expected_key:?}",
            file.file
        );
        let expected_columns = event
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect::<Vec<_>>();
        ensure!(
            file.columns == expected_columns,
            "manifest {event_name} columns do not match current schema"
        );
        let expected_order = event
            .sort_columns()
            .iter()
            .map(|column| (*column).to_owned())
            .collect::<Vec<_>>();
        ensure!(
            file.order_by == expected_order,
            "manifest {event_name} order_by does not match current export order"
        );
        ensure!(
            file.byte_size > 0,
            "manifest {event_name} file has zero byte_size"
        );
        ensure!(
            file.sha256.len() == 64
                && file
                    .sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "manifest {event_name} sha256 is not 64 lowercase hexadecimal characters"
        );
        match (file.row_count, file.min_sequence, file.max_sequence) {
            (0, None, None) => {}
            (0, _, _) => {
                anyhow::bail!("manifest {event_name} empty file has sequence bounds")
            }
            (_, Some(minimum), Some(maximum)) if minimum <= maximum => {
                min_sequence = Some(min_sequence.map_or(minimum, |value: u64| value.min(minimum)));
                max_sequence = Some(max_sequence.map_or(maximum, |value: u64| value.max(maximum)));
            }
            _ => anyhow::bail!("manifest {event_name} rows have invalid sequence bounds"),
        }
        total_rows = total_rows
            .checked_add(file.row_count)
            .context("manifest file row count overflow")?;
    }
    ensure!(
        manifest.row_count == total_rows,
        "manifest row_count {} does not match file total {total_rows}",
        manifest.row_count
    );
    ensure!(
        manifest.min_sequence == min_sequence,
        "manifest min_sequence does not match file bounds"
    );
    ensure!(
        manifest.max_sequence == max_sequence,
        "manifest max_sequence does not match file bounds"
    );
    Ok(())
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
    let archive = build_archive(cfg.export_backend.clone()).await?;
    let source: Arc<dyn EventSource> = Arc::new(ClickHouseSource::new(cfg.clone())?);
    let exporter = Exporter::new(cfg.clone(), source, archive);

    info!(
        clickhouse_retention_hours = cfg.clickhouse_retention_hours,
        "configured manifest-gated ClickHouse retention",
    );

    match &cfg.export_backend {
        ExportBackend::Local { root } => info!(
            directory = %root.display(),
            source_table = %cfg.clickhouse_table,
            "using local archive backend",
        ),
        ExportBackend::R2 {
            endpoint, bucket, ..
        } => info!(
            %endpoint,
            %bucket,
            source_table = %cfg.clickhouse_table,
            "using R2 archive backend",
        ),
    }
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::{bail, ensure};
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
            export_backend: ExportBackend::Local {
                root: root.to_path_buf(),
            },
            export_once: true,
            export_delay_minutes: 5,
            export_lag_hours: 1,
            clickhouse_retention_hours: 3,
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
        get_error: Mutex<Option<String>>,
        exists_error: Mutex<Option<String>>,
        get_limits: Mutex<Vec<usize>>,
    }

    impl MemoryArchive {
        fn new() -> Self {
            Self {
                directory: TempDir::new().unwrap(),
                objects: Mutex::new(BTreeMap::new()),
                order: Mutex::new(Vec::new()),
                get_error: Mutex::new(None),
                exists_error: Mutex::new(None),
                get_limits: Mutex::new(Vec::new()),
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
            if let Some(error) = self.exists_error.lock().unwrap().as_ref() {
                bail!(error.clone());
            }
            Ok(self.objects.lock().unwrap().contains_key(key))
        }

        fn get(&self, key: &str, max_bytes: usize) -> Result<Option<Vec<u8>>> {
            self.get_limits.lock().unwrap().push(max_bytes);
            if let Some(error) = self.get_error.lock().unwrap().as_ref() {
                bail!(error.clone());
            }
            let objects = self.objects.lock().unwrap();
            let Some(data) = objects.get(key) else {
                return Ok(None);
            };
            ensure!(data.len() <= max_bytes, "object exceeds read limit");
            Ok(Some(data.clone()))
        }
    }

    struct FakeSource {
        earliest: Option<DateTime<Utc>>,
        latest: Option<DateTime<Utc>>,
        rows_per_event: u64,
        partitions: Vec<ActivePartition>,
        active_partition_calls: AtomicUsize,
        dropped: Mutex<Vec<String>>,
        list_error: bool,
        drop_error: bool,
    }

    impl FakeSource {
        fn new(
            earliest: Option<DateTime<Utc>>,
            latest: Option<DateTime<Utc>>,
            rows_per_event: u64,
        ) -> Self {
            Self {
                earliest,
                latest,
                rows_per_event,
                partitions: Vec::new(),
                active_partition_calls: AtomicUsize::new(0),
                dropped: Mutex::new(Vec::new()),
                list_error: false,
                drop_error: false,
            }
        }

        fn with_partitions(mut self, partitions: impl IntoIterator<Item = DateTime<Utc>>) -> Self {
            self.partitions = partitions
                .into_iter()
                .map(|hour| ActivePartition {
                    hour,
                    partition_id: hour.timestamp().to_string(),
                })
                .collect();
            self
        }
    }

    impl EventSource for FakeSource {
        fn earliest_hour(&self) -> Result<Option<DateTime<Utc>>> {
            Ok(self.earliest)
        }

        fn latest_received_hour(&self) -> Result<Option<DateTime<Utc>>> {
            Ok(self.latest)
        }

        fn active_partitions(&self) -> Result<Vec<ActivePartition>> {
            self.active_partition_calls.fetch_add(1, Ordering::SeqCst);
            if self.list_error {
                bail!("partition listing failed");
            }
            Ok(self.partitions.clone())
        }

        fn drop_partition(&self, partition: &ActivePartition) -> Result<()> {
            if self.drop_error {
                bail!("partition drop failed");
            }
            self.dropped
                .lock()
                .unwrap()
                .push(partition.partition_id.clone());
            Ok(())
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

    fn valid_manifest(target_hour: DateTime<Utc>, source_table: &str) -> Manifest {
        let files = EventType::ALL
            .into_iter()
            .map(|event| {
                (
                    event.as_str().to_owned(),
                    ManifestFile {
                        file: event_to_key(target_hour, event).unwrap(),
                        row_count: 0,
                        byte_size: 1,
                        sha256: "0".repeat(64),
                        min_sequence: None,
                        max_sequence: None,
                        columns: event
                            .schema()
                            .fields()
                            .iter()
                            .map(|field| field.name().to_owned())
                            .collect(),
                        order_by: event
                            .sort_columns()
                            .iter()
                            .map(|column| (*column).to_owned())
                            .collect(),
                    },
                )
            })
            .collect();
        Manifest {
            hour_utc: format_datetime(target_hour),
            row_count: 0,
            min_sequence: None,
            max_sequence: None,
            files,
            source_table: source_table.to_owned(),
            created_at: format_datetime(target_hour + ChronoDuration::hours(1)),
        }
    }

    fn put_manifest(archive: &MemoryArchive, manifest_hour: DateTime<Utc>, manifest: &Manifest) {
        archive
            .put_bytes(
                &hour_to_completion_key(manifest_hour).unwrap(),
                &serde_json::to_vec(manifest).unwrap(),
            )
            .unwrap();
    }

    fn put_manifest_with_files(
        archive: &MemoryArchive,
        manifest_hour: DateTime<Utc>,
        manifest: &Manifest,
    ) {
        for file in manifest.files.values() {
            archive.put_bytes(&file.file, b"x").unwrap();
        }
        put_manifest(archive, manifest_hour, manifest);
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
        let source = Arc::new(FakeSource::new(Some(hour(14)), Some(hour(15)), 0));
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
        let source = Arc::new(FakeSource::new(Some(hour(10)), Some(hour(13)), 1));
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
    fn retention_zero_skips_partition_queries() {
        let archive = Arc::new(MemoryArchive::new());
        let source = Arc::new(FakeSource {
            list_error: true,
            ..FakeSource::new(None, None, 0)
        });
        let mut cfg = test_config(archive.directory.path());
        cfg.clickhouse_retention_hours = 0;
        let exporter = Exporter::new(cfg, source.clone(), archive);

        exporter.prune_archived_partitions(hour(12)).unwrap();

        assert_eq!(source.active_partition_calls.load(Ordering::SeqCst), 0);
        assert!(source.dropped.lock().unwrap().is_empty());
    }

    #[test]
    fn retention_uses_partition_end_boundary_and_preserves_newest() {
        let archive = Arc::new(MemoryArchive::new());
        for partition_hour in [hour(7), hour(8), hour(9)] {
            put_manifest_with_files(
                archive.as_ref(),
                partition_hour,
                &valid_manifest(partition_hour, "polymarket_orderbook_v3"),
            );
        }
        let source =
            Arc::new(FakeSource::new(None, None, 0).with_partitions([hour(7), hour(8), hour(9)]));
        let exporter = Exporter::new(
            test_config(archive.directory.path()),
            source.clone(),
            archive.clone(),
        );

        // With three-hour retention at 12:00, partitions ending at or before
        // 09:00 are eligible. Hour 8 is exactly on that boundary, while hour 9
        // is also protected as the newest active partition.
        exporter.prune_archived_partitions(hour(12)).unwrap();

        assert_eq!(
            *source.dropped.lock().unwrap(),
            [
                hour(7).timestamp().to_string(),
                hour(8).timestamp().to_string()
            ]
        );
        assert!(
            archive
                .get_limits
                .lock()
                .unwrap()
                .iter()
                .all(|limit| *limit == MAX_MANIFEST_BYTES)
        );
    }

    #[test]
    fn newest_active_partition_is_retained_even_when_old() {
        let archive = Arc::new(MemoryArchive::new());
        for partition_hour in [hour(1), hour(2)] {
            put_manifest_with_files(
                archive.as_ref(),
                partition_hour,
                &valid_manifest(partition_hour, "polymarket_orderbook_v3"),
            );
        }
        let source = Arc::new(FakeSource::new(None, None, 0).with_partitions([hour(1), hour(2)]));
        let exporter = Exporter::new(
            test_config(archive.directory.path()),
            source.clone(),
            archive,
        );

        exporter.prune_archived_partitions(hour(20)).unwrap();

        assert_eq!(
            *source.dropped.lock().unwrap(),
            [hour(1).timestamp().to_string()]
        );
    }

    #[test]
    fn retention_requires_exact_complete_and_consistent_manifests() {
        let archive = Arc::new(MemoryArchive::new());

        archive
            .put_bytes(&hour_to_completion_key(hour(2)).unwrap(), b"not json")
            .unwrap();

        let mut wrong_hour = valid_manifest(hour(3), "polymarket_orderbook_v3");
        wrong_hour.hour_utc = format_datetime(hour(4));
        put_manifest(archive.as_ref(), hour(3), &wrong_hour);

        let wrong_table = valid_manifest(hour(4), "another_table");
        put_manifest(archive.as_ref(), hour(4), &wrong_table);

        let mut missing_event = valid_manifest(hour(5), "polymarket_orderbook_v3");
        missing_event.files.remove("book");
        put_manifest(archive.as_ref(), hour(5), &missing_event);

        let mut wrong_key = valid_manifest(hour(6), "polymarket_orderbook_v3");
        wrong_key.files.get_mut("book").unwrap().file = "wrong.parquet".to_owned();
        put_manifest(archive.as_ref(), hour(6), &wrong_key);

        let mut bad_hash = valid_manifest(hour(7), "polymarket_orderbook_v3");
        bad_hash.files.get_mut("book").unwrap().sha256 = "ABC".repeat(21) + "A";
        put_manifest(archive.as_ref(), hour(7), &bad_hash);

        let mut bad_stats = valid_manifest(hour(8), "polymarket_orderbook_v3");
        let book = bad_stats.files.get_mut("book").unwrap();
        book.row_count = 1;
        book.min_sequence = Some(10);
        book.max_sequence = Some(11);
        put_manifest(archive.as_ref(), hour(8), &bad_stats);

        let valid = valid_manifest(hour(9), "polymarket_orderbook_v3");
        put_manifest_with_files(archive.as_ref(), hour(9), &valid);
        archive
            .objects
            .lock()
            .unwrap()
            .remove(&event_to_key(hour(9), EventType::Book).unwrap());

        let valid = valid_manifest(hour(10), "polymarket_orderbook_v3");
        put_manifest_with_files(archive.as_ref(), hour(10), &valid);

        let mut wrong_columns = valid_manifest(hour(11), "polymarket_orderbook_v3");
        wrong_columns.files.get_mut("book").unwrap().columns = vec!["wrong".to_owned()];
        put_manifest(archive.as_ref(), hour(11), &wrong_columns);

        let mut wrong_order = valid_manifest(hour(12), "polymarket_orderbook_v3");
        wrong_order.files.get_mut("book").unwrap().order_by = vec!["sequence".to_owned()];
        put_manifest(archive.as_ref(), hour(12), &wrong_order);

        let valid = valid_manifest(hour(13), "polymarket_orderbook_v3");
        put_manifest_with_files(archive.as_ref(), hour(13), &valid);

        let valid_newest = valid_manifest(hour(14), "polymarket_orderbook_v3");
        put_manifest_with_files(archive.as_ref(), hour(14), &valid_newest);

        let mut wrong_global_bounds = valid_manifest(hour(15), "polymarket_orderbook_v3");
        let book = wrong_global_bounds.files.get_mut("book").unwrap();
        book.row_count = 1;
        book.min_sequence = Some(10);
        book.max_sequence = Some(11);
        wrong_global_bounds.row_count = 1;
        wrong_global_bounds.min_sequence = Some(9);
        wrong_global_bounds.max_sequence = Some(11);
        put_manifest(archive.as_ref(), hour(15), &wrong_global_bounds);

        let valid_newest = valid_manifest(hour(16), "polymarket_orderbook_v3");
        put_manifest_with_files(archive.as_ref(), hour(16), &valid_newest);

        let source = Arc::new(FakeSource::new(None, None, 0).with_partitions((1..=16).map(hour)));
        let exporter = Exporter::new(
            test_config(archive.directory.path()),
            source.clone(),
            archive,
        );

        exporter.prune_archived_partitions(hour(20)).unwrap();

        assert_eq!(
            *source.dropped.lock().unwrap(),
            [
                hour(10).timestamp().to_string(),
                hour(13).timestamp().to_string(),
                hour(14).timestamp().to_string()
            ]
        );
    }

    #[test]
    fn cleanup_failures_retain_data_and_do_not_fail_export_checks() {
        let archive = Arc::new(MemoryArchive::new());
        let list_failing_source = Arc::new(FakeSource {
            list_error: true,
            ..FakeSource::new(None, None, 0)
        });
        let exporter = Exporter::new(
            test_config(archive.directory.path()),
            list_failing_source.clone(),
            archive.clone(),
        );
        assert_eq!(exporter.backfill(hour(12)).unwrap(), None);
        assert_eq!(
            list_failing_source
                .active_partition_calls
                .load(Ordering::SeqCst),
            1
        );

        let source = Arc::new(FakeSource::new(None, None, 0).with_partitions([hour(1), hour(2)]));
        put_manifest_with_files(
            archive.as_ref(),
            hour(1),
            &valid_manifest(hour(1), "polymarket_orderbook_v3"),
        );
        *archive.get_error.lock().unwrap() = Some("archive unavailable".to_owned());
        let exporter = Exporter::new(
            test_config(archive.directory.path()),
            source.clone(),
            archive.clone(),
        );
        exporter.prune_archived_partitions(hour(20)).unwrap();
        assert!(source.dropped.lock().unwrap().is_empty());
        *archive.get_error.lock().unwrap() = None;

        *archive.exists_error.lock().unwrap() = Some("archive HEAD failed".to_owned());
        exporter.prune_archived_partitions(hour(20)).unwrap();
        assert!(source.dropped.lock().unwrap().is_empty());
        *archive.exists_error.lock().unwrap() = None;

        let drop_failing_source = Arc::new(FakeSource {
            drop_error: true,
            ..FakeSource::new(None, None, 0).with_partitions([hour(1), hour(2)])
        });
        let exporter = Exporter::new(
            test_config(archive.directory.path()),
            drop_failing_source.clone(),
            archive,
        );
        exporter.prune_archived_partitions(hour(20)).unwrap();
        assert!(drop_failing_source.dropped.lock().unwrap().is_empty());
    }

    #[test]
    fn successful_steady_state_checks_attempt_cleanup() {
        let archive = Arc::new(MemoryArchive::new());
        let source = Arc::new(FakeSource::new(None, None, 0));
        let exporter = Exporter::new(
            test_config(archive.directory.path()),
            source.clone(),
            archive,
        );

        let now = hour(12) + ChronoDuration::minutes(1);
        assert_eq!(
            exporter.steady_state_step(now, Some(hour(11))).unwrap(),
            Some(hour(11))
        );
        assert_eq!(source.active_partition_calls.load(Ordering::SeqCst), 1);
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
            fn active_partitions(&self) -> Result<Vec<ActivePartition>> {
                Ok(Vec::new())
            }
            fn drop_partition(&self, _partition: &ActivePartition) -> Result<()> {
                Ok(())
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
