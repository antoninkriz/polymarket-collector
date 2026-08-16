//! Single-owner Redis Stream to ClickHouse writer for the v3 pipeline.
//!
//! The actor owns one uncommitted batch and its committed-but-unacknowledged
//! Redis IDs. It never acknowledges an entry before ClickHouse commits it. On
//! restart, the same consumer drains its pending entries before reading new
//! ones; replays retain their sequence and remain logically idempotent.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use redis::streams::{StreamId, StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, AsyncConnectionConfig, Value};
use serde_json::value::RawValue;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::clickhouse::{ClickHouseSink, RawRow};

const READ_COUNT: usize = 1_000;
const READ_BLOCK_MS: usize = 1_000;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(3);
const ACK_DELETE_CHUNK_SIZE: usize = READ_COUNT;
const STATS_INTERVAL: Duration = Duration::from_secs(60);

pub struct WriterConfig {
    pub stream: String,
    pub group: String,
    pub consumer: String,
    pub reconnect_delay: Duration,
    pub batch_size: usize,
    pub flush_interval: Duration,
}

#[derive(Default)]
struct WriterStats {
    events_read: u64,
    events_committed: u64,
    events_acked: u64,
    events_deleted: u64,
    parse_failures: u64,
    reconnects: u64,
}

#[derive(Debug)]
struct PendingEntry {
    row: RawRow,
    delivery_id: String,
}

pub struct Writer {
    cfg: WriterConfig,
    redis: redis::Client,
    sink: ClickHouseSink,
    batch: Vec<PendingEntry>,
    committed_unacked: Vec<String>,
    batch_deadline: Option<Instant>,
    batch_high_water: usize,
    stats: WriterStats,
    last_stats_report: Instant,
}

impl Writer {
    pub fn new(redis_url: &str, cfg: WriterConfig, sink: ClickHouseSink) -> Result<Self> {
        anyhow::ensure!(cfg.batch_size > 0, "writer batch size must be positive");
        anyhow::ensure!(
            !cfg.flush_interval.is_zero(),
            "writer flush interval must be positive"
        );
        let redis = redis::Client::open(redis_url)
            .with_context(|| format!("invalid REDIS_URL: {redis_url}"))?;
        let batch_size = cfg.batch_size;
        Ok(Self {
            cfg,
            redis,
            sink,
            batch: Vec::with_capacity(batch_size),
            committed_unacked: Vec::with_capacity(batch_size),
            batch_deadline: None,
            batch_high_water: 0,
            stats: WriterStats::default(),
            last_stats_report: Instant::now(),
        })
    }

    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        loop {
            if *shutdown.borrow() {
                return self.shutdown(None).await;
            }
            match self.consume_once(&shutdown).await {
                Ok(connection) => return self.shutdown(Some(connection)).await,
                Err(error) => {
                    warn!(?error, stream = %self.cfg.stream, "Redis event stream error; reconnecting");
                }
            }
            self.stats.reconnects += 1;
            // Entries already returned by XREADGROUP remain in this actor's
            // PEL. Commit the partial batch before resetting the pending
            // cursor so a Redis reconnect cannot duplicate it in-process.
            self.commit_batch().await?;
            self.report_stats(false);
            if wait_for_reconnect_or_shutdown(self.cfg.reconnect_delay, &mut shutdown).await {
                return self.shutdown(None).await;
            }
        }
    }

    async fn consume_once(
        &mut self,
        shutdown: &watch::Receiver<bool>,
    ) -> Result<redis::aio::MultiplexedConnection> {
        let mut conn = self.connect().await?;
        ensure_consumer_group(&mut conn, &self.cfg).await?;
        flush_acks(
            &mut conn,
            &self.cfg,
            &mut self.committed_unacked,
            &mut self.stats,
        )
        .await?;
        info!(
            stream = %self.cfg.stream,
            group = %self.cfg.group,
            consumer = %self.cfg.consumer,
            "Polymarket v3 stream consumer connected",
        );

        let mut draining_pending = true;
        let mut last_pending_id = String::from("0");

        loop {
            if *shutdown.borrow() {
                return Ok(conn);
            }
            self.flush_if_due(&mut conn).await?;
            self.report_stats(false);

            let read_id = if draining_pending {
                last_pending_id.as_str()
            } else {
                ">"
            };
            let reply: StreamReadReply = {
                let options = StreamReadOptions::default()
                    .group(&self.cfg.group, &self.cfg.consumer)
                    .count(self.read_count())
                    .block(self.read_block_ms());
                conn.xread_options(&[self.cfg.stream.as_str()], &[read_id], &options)
                    .await
                    .context("XREADGROUP Polymarket v3 events")?
            };

            let mut received = 0_usize;
            for stream in reply.keys {
                for entry in stream.ids {
                    received += 1;
                    if draining_pending {
                        last_pending_id.clone_from(&entry.id);
                    }
                    let item = parse_stream_entry(&entry, &mut self.stats)?;
                    if self.batch.is_empty() {
                        self.batch_deadline = Some(
                            Instant::now()
                                .checked_add(self.cfg.flush_interval)
                                .context("ClickHouse flush deadline overflow")?,
                        );
                    }
                    self.batch.push(item);
                    self.batch_high_water = self.batch_high_water.max(self.batch.len());
                    self.stats.events_read += 1;
                }
            }

            if self.batch.len() >= self.cfg.batch_size {
                self.commit_and_ack(&mut conn).await?;
            }
            if draining_pending && received == 0 {
                draining_pending = false;
                info!(consumer = %self.cfg.consumer, "pending Redis entries drained");
            }
        }
    }

    async fn connect(&self) -> Result<redis::aio::MultiplexedConnection> {
        // redis-rs 1.5 defaults to a 500 ms response timeout, which is shorter
        // than this actor's intentional one-second blocking stream read.
        let connection_config =
            AsyncConnectionConfig::new().set_response_timeout(Some(RESPONSE_TIMEOUT));
        self.redis
            .get_multiplexed_async_connection_with_config(&connection_config)
            .await
            .context("open Redis stream connection")
    }

    fn read_count(&self) -> usize {
        READ_COUNT.min(self.cfg.batch_size - self.batch.len())
    }

    fn read_block_ms(&self) -> usize {
        let Some(deadline) = self.batch_deadline else {
            return READ_BLOCK_MS;
        };
        deadline
            .saturating_duration_since(Instant::now())
            .as_millis()
            .clamp(1, READ_BLOCK_MS as u128) as usize
    }

    async fn flush_if_due(&mut self, conn: &mut redis::aio::MultiplexedConnection) -> Result<()> {
        let due = self
            .batch_deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        if due || self.batch.len() >= self.cfg.batch_size {
            self.commit_and_ack(conn).await?;
        }
        Ok(())
    }

    async fn commit_and_ack(&mut self, conn: &mut redis::aio::MultiplexedConnection) -> Result<()> {
        self.commit_batch().await?;
        flush_acks(
            conn,
            &self.cfg,
            &mut self.committed_unacked,
            &mut self.stats,
        )
        .await
    }

    async fn commit_batch(&mut self) -> Result<()> {
        if self.batch.is_empty() {
            return Ok(());
        }
        anyhow::ensure!(
            self.committed_unacked.is_empty(),
            "cannot commit a new batch while Redis acknowledgements are pending",
        );
        let committed = self.batch.len() as u64;
        self.sink
            .insert(self.batch.iter().map(|entry| &entry.row))
            .await?;
        self.stats.events_committed += committed;
        self.committed_unacked
            .extend(self.batch.drain(..).map(|item| item.delivery_id));
        self.batch_deadline = None;
        Ok(())
    }

    async fn shutdown(
        &mut self,
        mut conn: Option<redis::aio::MultiplexedConnection>,
    ) -> Result<()> {
        self.commit_batch().await?;
        while !self.committed_unacked.is_empty() {
            if conn.is_none() {
                match self.connect().await {
                    Ok(mut connection) => {
                        if let Err(error) = ensure_consumer_group(&mut connection, &self.cfg).await
                        {
                            warn!(?error, "Redis connection failed during writer shutdown");
                            tokio::time::sleep(self.cfg.reconnect_delay).await;
                            continue;
                        }
                        conn = Some(connection);
                    }
                    Err(error) => {
                        warn!(?error, "Redis connection failed during writer shutdown");
                        tokio::time::sleep(self.cfg.reconnect_delay).await;
                        continue;
                    }
                }
            }
            let connection = conn.as_mut().expect("Redis connection is present");
            if let Err(error) = flush_acks(
                connection,
                &self.cfg,
                &mut self.committed_unacked,
                &mut self.stats,
            )
            .await
            {
                warn!(
                    ?error,
                    "Redis acknowledgement failed during writer shutdown"
                );
                conn = None;
                tokio::time::sleep(self.cfg.reconnect_delay).await;
            }
        }

        self.report_stats(true);
        info!("Redis to ClickHouse writer shut down cleanly");
        Ok(())
    }

    fn report_stats(&mut self, force: bool) {
        if !force && self.last_stats_report.elapsed() < STATS_INTERVAL {
            return;
        }
        info!(
            batch_records = self.batch.len(),
            batch_high_water = self.batch_high_water,
            batch_max = self.cfg.batch_size,
            committed_unacked = self.committed_unacked.len(),
            events_read = self.stats.events_read,
            events_committed = self.stats.events_committed,
            events_acked = self.stats.events_acked,
            events_deleted = self.stats.events_deleted,
            parse_failures = self.stats.parse_failures,
            reconnects = self.stats.reconnects,
            "[POLYMARKET-FROM-PUBSUB-STATS]",
        );
        self.batch_high_water = self.batch.len();
        self.last_stats_report = Instant::now();
    }
}

async fn wait_for_reconnect_or_shutdown(
    delay: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    if *shutdown.borrow() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

async fn ensure_consumer_group(
    conn: &mut redis::aio::MultiplexedConnection,
    cfg: &WriterConfig,
) -> Result<()> {
    let result: redis::RedisResult<()> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&cfg.stream)
        .arg(&cfg.group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(conn)
        .await;
    match result {
        Ok(()) => info!(group = %cfg.group, "created Redis event consumer group"),
        Err(error) if error.to_string().contains("BUSYGROUP") => {}
        Err(error) => return Err(error).context("create Redis event consumer group"),
    }
    Ok(())
}

async fn flush_acks(
    conn: &mut redis::aio::MultiplexedConnection,
    cfg: &WriterConfig,
    pending_acks: &mut Vec<String>,
    stats: &mut WriterStats,
) -> Result<()> {
    if pending_acks.is_empty() {
        return Ok(());
    }
    // Keep each Redis command bounded during recovery.
    let mut processed = 0;
    while processed < pending_acks.len() {
        let end = (processed + ACK_DELETE_CHUNK_SIZE).min(pending_acks.len());
        let result: redis::RedisResult<Vec<i64>> = redis::cmd("XACKDEL")
            .arg(&cfg.stream)
            .arg(&cfg.group)
            .arg("ACKED")
            .arg("IDS")
            .arg(end - processed)
            .arg(&pending_acks[processed..end])
            .query_async(conn)
            .await;
        let statuses = match result {
            Ok(statuses) => statuses,
            Err(error) => {
                pending_acks.drain(..processed);
                return Err(error).context("XACKDEL committed ClickHouse rows");
            }
        };
        let (acknowledged, deleted) = match ack_delete_counts(&statuses, end - processed) {
            Ok(counts) => counts,
            Err(error) => {
                pending_acks.drain(..processed);
                return Err(error);
            }
        };
        stats.events_acked += acknowledged;
        stats.events_deleted += deleted;
        processed = end;
    }
    pending_acks.clear();
    Ok(())
}

fn ack_delete_counts(statuses: &[i64], expected: usize) -> Result<(u64, u64)> {
    anyhow::ensure!(
        statuses.len() == expected,
        "XACKDEL returned {} statuses for {expected} IDs",
        statuses.len()
    );
    let mut acknowledged = 0;
    let mut deleted = 0;
    for status in statuses {
        match status {
            1 => {
                acknowledged += 1;
                deleted += 1;
            }
            2 => acknowledged += 1,
            -1 => {}
            status => anyhow::bail!("XACKDEL returned unknown status {status}"),
        }
    }
    Ok((acknowledged, deleted))
}

fn parse_stream_entry(entry: &StreamId, stats: &mut WriterStats) -> Result<PendingEntry> {
    let result = parse_stream_entry_fields(entry).with_context(|| {
        format!(
            "parse v3 stream entry {}; fields={}",
            entry.id,
            stream_entry_preview(entry),
        )
    });
    if result.is_err() {
        stats.parse_failures += 1;
    }
    result
}

fn parse_stream_entry_fields(entry: &StreamId) -> Result<PendingEntry> {
    let has_raw_fields = entry.map.len() == 3
        && entry.map.contains_key("timestamp_received")
        && entry.map.contains_key("sequence")
        && entry.map.contains_key("data");
    anyhow::ensure!(
        has_raw_fields,
        "expected exactly fields `timestamp_received`, `sequence`, `data`"
    );

    let timestamp_received = stream_field(entry, "timestamp_received")?
        .parse::<i64>()
        .context("parse timestamp_received as i64 epoch nanoseconds")?;
    anyhow::ensure!(
        timestamp_received >= 0,
        "timestamp_received must be a non-negative i64 epoch nanosecond value"
    );
    let sequence = stream_field(entry, "sequence")?
        .parse::<u64>()
        .context("parse sequence as u64")?;
    let data = stream_field(entry, "data")?;
    let raw = serde_json::from_str::<&RawValue>(data).context("parse data as JSON")?;
    anyhow::ensure!(
        raw.get().trim_start().starts_with('{'),
        "data must be a JSON object"
    );
    Ok(PendingEntry {
        row: RawRow {
            timestamp_received,
            sequence,
            data: data.to_owned(),
        },
        delivery_id: entry.id.clone(),
    })
}

fn stream_field<'a>(entry: &'a StreamId, name: &str) -> Result<&'a str> {
    let value = entry
        .map
        .get(name)
        .with_context(|| format!("missing field {name:?}"))?;
    let Value::BulkString(bytes) = value else {
        anyhow::bail!(
            "field {name:?} must be a bulk string, got {}",
            redis_value_kind(value)
        );
    };
    std::str::from_utf8(bytes).with_context(|| format!("field {name:?} is not valid UTF-8"))
}

fn redis_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Nil => "nil",
        Value::Int(_) => "integer",
        Value::BulkString(_) => "bulk string",
        Value::Array(_) => "array",
        Value::SimpleString(_) => "simple string",
        Value::Okay => "okay",
        Value::Map(_) => "map",
        Value::Attribute { .. } => "attribute",
        Value::Set(_) => "set",
        Value::Double(_) => "double",
        Value::Boolean(_) => "boolean",
        Value::VerbatimString { .. } => "verbatim string",
        Value::Push { .. } => "push",
        Value::ServerError(_) => "server error",
        _ => "unknown Redis value",
    }
}

fn stream_entry_preview(entry: &StreamId) -> String {
    let mut names = entry.map.keys().map(String::as_str).collect::<Vec<_>>();
    names.sort_unstable();
    let fields = names
        .into_iter()
        .map(|name| match entry.map.get(name) {
            Some(Value::BulkString(bytes)) => match std::str::from_utf8(bytes) {
                Ok(value) => format!("{name}={:?}", preview(value)),
                Err(_) => format!("{name}=<invalid UTF-8 bulk string>"),
            },
            Some(value) => format!("{name}=<{}>", redis_value_kind(value)),
            None => unreachable!("field name came from the same map"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    preview(&fields)
}

fn preview(payload: &str) -> String {
    const MAX: usize = 200;
    if payload.len() <= MAX {
        payload.to_string()
    } else {
        let mut end = MAX;
        while !payload.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &payload[..end])
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use anyhow::Result;
    use redis::streams::StreamId;
    use redis::Value;

    use super::{
        ack_delete_counts, ensure_consumer_group, flush_acks, parse_stream_entry, preview,
        WriterConfig, WriterStats,
    };

    const TEST_REDIS_URL: &str = "redis://localhost:16380";

    fn stream_entry(fields: &[(&str, &str)]) -> StreamId {
        StreamId {
            id: "1-0".into(),
            map: fields
                .iter()
                .map(|(name, value)| {
                    (
                        (*name).to_owned(),
                        Value::BulkString(value.as_bytes().to_vec()),
                    )
                })
                .collect(),
            ..StreamId::default()
        }
    }

    #[test]
    fn raw_entry_accepts_any_json_object_and_preserves_its_text() {
        let data = "  {\"future_event\":[1,true],\"unknown\":null} \n";
        let entry = stream_entry(&[
            ("timestamp_received", "1757908892351123456"),
            ("sequence", "42"),
            ("data", data),
        ]);
        let mut stats = WriterStats::default();

        let item = parse_stream_entry(&entry, &mut stats).unwrap();

        assert_eq!(item.row.timestamp_received, 1_757_908_892_351_123_456);
        assert_eq!(item.row.sequence, 42);
        assert_eq!(item.row.data, data);
        assert_eq!(stats.parse_failures, 0);
    }

    #[test]
    fn stream_fields_must_be_utf8_bulk_strings_without_numeric_coercion() {
        let mut integer = stream_entry(&[
            ("timestamp_received", "1"),
            ("sequence", "2"),
            ("data", "{}"),
        ]);
        integer
            .map
            .insert("timestamp_received".into(), Value::Int(1));
        let mut invalid_utf8 = stream_entry(&[
            ("timestamp_received", "1"),
            ("sequence", "2"),
            ("data", "{}"),
        ]);
        invalid_utf8
            .map
            .insert("data".into(), Value::BulkString(vec![0xff]));
        let mut stats = WriterStats::default();

        let integer_error = parse_stream_entry(&integer, &mut stats).unwrap_err();
        let integer_message = format!("{integer_error:#}");
        assert!(
            integer_message.contains("must be a bulk string, got integer"),
            "{integer_message}"
        );
        assert!(
            integer_message.contains("timestamp_received=<integer>"),
            "{integer_message}"
        );

        let utf8_error = parse_stream_entry(&invalid_utf8, &mut stats).unwrap_err();
        let utf8_message = format!("{utf8_error:#}");
        assert!(utf8_message.contains("not valid UTF-8"), "{utf8_message}");
        assert!(
            utf8_message.contains("data=<invalid UTF-8 bulk string>"),
            "{utf8_message}"
        );
        assert_eq!(stats.parse_failures, 2);
    }

    #[test]
    fn negative_received_timestamp_is_rejected() {
        let entry = stream_entry(&[
            ("timestamp_received", "-1"),
            ("sequence", "2"),
            ("data", "{}"),
        ]);
        let mut stats = WriterStats::default();

        let error = parse_stream_entry(&entry, &mut stats).unwrap_err();
        let message = format!("{error:#}");

        assert!(message.contains("must be a non-negative i64"), "{message}");
        assert_eq!(stats.parse_failures, 1);
    }

    #[test]
    fn overflowing_i64_timestamp_and_u64_sequence_are_rejected() {
        let entries = [
            (
                stream_entry(&[
                    ("timestamp_received", "9223372036854775808"),
                    ("sequence", "2"),
                    ("data", "{}"),
                ]),
                "number too large",
            ),
            (
                stream_entry(&[
                    ("timestamp_received", "1"),
                    ("sequence", "18446744073709551616"),
                    ("data", "{}"),
                ]),
                "number too large",
            ),
        ];
        let mut stats = WriterStats::default();

        for (entry, expected) in &entries {
            let error = parse_stream_entry(entry, &mut stats).unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains(expected), "{message}");
        }
        assert_eq!(stats.parse_failures, entries.len() as u64);
    }

    #[test]
    fn unsupported_incomplete_and_extra_field_sets_are_rejected() {
        let entries = [
            stream_entry(&[("payload", r#"{"sequence":2}"#)]),
            stream_entry(&[("timestamp_received", "1"), ("sequence", "2")]),
            stream_entry(&[
                ("payload", r#"{"sequence":2}"#),
                ("timestamp_received", "1"),
                ("sequence", "2"),
                ("data", "{}"),
            ]),
            stream_entry(&[
                ("timestamp_received", "1"),
                ("sequence", "2"),
                ("data", "{}"),
                ("extra", "value"),
            ]),
        ];
        let mut stats = WriterStats::default();

        for entry in &entries {
            let error = parse_stream_entry(entry, &mut stats).unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("expected exactly"), "{message}");
            assert!(message.contains("fields="), "{message}");
        }
        assert_eq!(stats.parse_failures, entries.len() as u64);
    }

    #[test]
    fn malformed_numeric_and_json_fields_are_rejected() {
        let entries = [
            (
                stream_entry(&[
                    ("timestamp_received", "later"),
                    ("sequence", "2"),
                    ("data", "{}"),
                ]),
                "later",
            ),
            (
                stream_entry(&[
                    ("timestamp_received", "1"),
                    ("sequence", "-2"),
                    ("data", "{}"),
                ]),
                "-2",
            ),
            (
                stream_entry(&[
                    ("timestamp_received", "1"),
                    ("sequence", "2"),
                    ("data", "{\"broken\":"),
                ]),
                "broken",
            ),
            (
                stream_entry(&[
                    ("timestamp_received", "1"),
                    ("sequence", "2"),
                    ("data", "[]"),
                ]),
                "[]",
            ),
        ];
        let mut stats = WriterStats::default();

        for (entry, expected_preview) in &entries {
            let error = parse_stream_entry(entry, &mut stats).unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("fields="), "{message}");
            assert!(message.contains(expected_preview), "{message}");
        }
        assert_eq!(stats.parse_failures, entries.len() as u64);
    }

    #[test]
    fn preview_does_not_split_a_utf8_character() {
        let payload = format!("{}é-tail", "a".repeat(199));
        assert_eq!(preview(&payload), format!("{}…", "a".repeat(199)));
    }

    #[test]
    fn ack_delete_statuses_are_counted_strictly() {
        assert_eq!(ack_delete_counts(&[1, 2, -1], 3).unwrap(), (2, 1));
        assert!(ack_delete_counts(&[1], 2).is_err());
        assert!(ack_delete_counts(&[0], 1).is_err());
    }

    async fn xadd_raw_entry(
        conn: &mut redis::aio::MultiplexedConnection,
        stream: &str,
        sequence: usize,
    ) -> Result<String> {
        Ok(redis::cmd("XADD")
            .arg(stream)
            .arg("*")
            .arg("timestamp_received")
            .arg("1")
            .arg("sequence")
            .arg(sequence)
            .arg("data")
            .arg("{}")
            .query_async(conn)
            .await?)
    }

    #[tokio::test]
    #[ignore]
    async fn committed_cleanup_is_exact_and_group_safe() -> Result<()> {
        let suffix = format!(
            "{}:{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        );
        let cfg = WriterConfig {
            stream: format!("test:polymarket:v3:cleanup:{suffix}"),
            group: "clickhouse".into(),
            consumer: "clickhouse-1".into(),
            reconnect_delay: Duration::from_millis(1),
            batch_size: 5_000,
            flush_interval: Duration::from_millis(500),
        };
        let client = redis::Client::open(TEST_REDIS_URL)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        ensure_consumer_group(&mut conn, &cfg).await?;

        const BULK_ENTRY_COUNT: usize = 2_000;
        let mut first_ids = Vec::with_capacity(BULK_ENTRY_COUNT);
        for sequence in 0..BULK_ENTRY_COUNT {
            let id = xadd_raw_entry(&mut conn, &cfg.stream, sequence).await?;
            first_ids.push(id);
        }
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&cfg.group)
            .arg(&cfg.consumer)
            .arg("COUNT")
            .arg(BULK_ENTRY_COUNT)
            .arg("STREAMS")
            .arg(&cfg.stream)
            .arg(">")
            .query_async(&mut conn)
            .await?;

        let mut stats = WriterStats::default();
        // Send enough IDs to cross the native-command chunk boundary.
        let missing_id = first_ids[0].clone();
        let mut pending = first_ids;
        flush_acks(&mut conn, &cfg, &mut pending, &mut stats).await?;
        assert!(pending.is_empty());
        let length: i64 = redis::cmd("XLEN")
            .arg(&cfg.stream)
            .query_async(&mut conn)
            .await?;
        assert_eq!(length, 0);
        assert_eq!(stats.events_acked, BULK_ENTRY_COUNT as u64);
        assert_eq!(stats.events_deleted, BULK_ENTRY_COUNT as u64);

        // Simulate a lost response: an already-applied ID is locally complete
        // and safe to discard when the acknowledgement is retried.
        let mut missing_ack = vec![missing_id];
        flush_acks(&mut conn, &cfg, &mut missing_ack, &mut stats).await?;
        assert!(missing_ack.is_empty());
        assert_eq!(stats.events_acked, BULK_ENTRY_COUNT as u64);

        let mut ordered_ids = Vec::new();
        for sequence in 0..3 {
            ordered_ids.push(xadd_raw_entry(&mut conn, &cfg.stream, sequence).await?);
        }
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&cfg.group)
            .arg(&cfg.consumer)
            .arg("COUNT")
            .arg(3)
            .arg("STREAMS")
            .arg(&cfg.stream)
            .arg(">")
            .query_async(&mut conn)
            .await?;

        // Exact deletion can remove newer committed entries while preserving
        // an older pending entry.
        let mut newer_acks = ordered_ids[1..].to_vec();
        flush_acks(&mut conn, &cfg, &mut newer_acks, &mut stats).await?;
        let length: i64 = redis::cmd("XLEN")
            .arg(&cfg.stream)
            .query_async(&mut conn)
            .await?;
        assert_eq!(length, 1);

        let mut oldest_ack = vec![ordered_ids[0].clone()];
        flush_acks(&mut conn, &cfg, &mut oldest_ack, &mut stats).await?;
        let length: i64 = redis::cmd("XLEN")
            .arg(&cfg.stream)
            .query_async(&mut conn)
            .await?;
        assert_eq!(length, 0);

        let _: () = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&cfg.stream)
            .arg("observer")
            .arg("0")
            .query_async(&mut conn)
            .await?;
        let second_id = xadd_raw_entry(&mut conn, &cfg.stream, 0).await?;
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg(&cfg.group)
            .arg(&cfg.consumer)
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&cfg.stream)
            .arg(">")
            .query_async(&mut conn)
            .await?;
        let _: redis::Value = redis::cmd("XREADGROUP")
            .arg("GROUP")
            .arg("observer")
            .arg("observer-1")
            .arg("COUNT")
            .arg(1)
            .arg("STREAMS")
            .arg(&cfg.stream)
            .arg(">")
            .query_async(&mut conn)
            .await?;

        // The first group is acknowledged, but the entry remains because the
        // observer group still has it pending.
        let mut primary_ack = vec![second_id.clone()];
        flush_acks(&mut conn, &cfg, &mut primary_ack, &mut stats).await?;
        assert!(primary_ack.is_empty());
        let length: i64 = redis::cmd("XLEN")
            .arg(&cfg.stream)
            .query_async(&mut conn)
            .await?;
        assert_eq!(length, 1);

        let acknowledged_before_retry = stats.events_acked;
        let deleted_before_retry = stats.events_deleted;
        let mut primary_retry = vec![second_id.clone()];
        flush_acks(&mut conn, &cfg, &mut primary_retry, &mut stats).await?;
        assert_eq!(stats.events_acked, acknowledged_before_retry);
        assert_eq!(stats.events_deleted, deleted_before_retry);

        let observer_cfg = WriterConfig {
            stream: cfg.stream.clone(),
            group: "observer".into(),
            consumer: "observer-1".into(),
            reconnect_delay: cfg.reconnect_delay,
            batch_size: cfg.batch_size,
            flush_interval: cfg.flush_interval,
        };
        let mut observer_stats = WriterStats::default();
        let mut observer_ack = vec![second_id];
        flush_acks(
            &mut conn,
            &observer_cfg,
            &mut observer_ack,
            &mut observer_stats,
        )
        .await?;
        assert_eq!(observer_stats.events_acked, 1);
        assert_eq!(observer_stats.events_deleted, 1);
        let length: i64 = redis::cmd("XLEN")
            .arg(&cfg.stream)
            .query_async(&mut conn)
            .await?;
        assert_eq!(length, 0);

        let _: i64 = redis::cmd("DEL")
            .arg(&cfg.stream)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }
}
