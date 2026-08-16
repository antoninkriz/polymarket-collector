//! End-to-end writer actor test against disposable Redis and ClickHouse instances.
//!
//! Run with Redis on `localhost:16380` and ClickHouse on `localhost:8124`:
//!
//! ```bash
//! cargo test --test writer_integration -- --ignored --nocapture
//! ```

use std::fmt::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use polymarket_clickhouse_writer::clickhouse::{ClickHouseConfig, ClickHouseSink};
use polymarket_clickhouse_writer::writer::{Writer, WriterConfig};
use reqwest::Client;
use tokio::sync::watch;

const REDIS_URL: &str = "redis://localhost:16380";
const CLICKHOUSE_URL: &str = "http://localhost:8124";
const CLICKHOUSE_DATABASE: &str = "default";

fn clickhouse_password() -> String {
    std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default()
}

fn timestamp_received(sequence: u64) -> i64 {
    1_757_908_892_351_123_456_i64 + sequence as i64
}

fn data() -> String {
    serde_json::json!({
        "event_type": "tick_size_change",
        "market": "0xmarket",
        "asset_id": "asset",
        "timestamp": "1757908892351",
        "old_tick_size": "0.01",
        "new_tick_size": "0.001",
    })
    .to_string()
}

fn hex(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        write!(encoded, "{byte:02X}").unwrap();
    }
    encoded
}

async fn clickhouse_query(http: &Client, sql: &str) -> Result<String> {
    let response = http
        .post(CLICKHOUSE_URL)
        .basic_auth("default", Some(clickhouse_password()))
        .body(sql.to_owned())
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    anyhow::ensure!(status.is_success(), "query {sql:?} failed: {status} {body}");
    Ok(body)
}

async fn pending_entries(
    conn: &mut redis::aio::MultiplexedConnection,
    stream: &str,
    group: &str,
) -> Result<Vec<(String, String, u64, u64)>> {
    redis::cmd("XPENDING")
        .arg(stream)
        .arg(group)
        .arg("-")
        .arg("+")
        .arg(10)
        .query_async(conn)
        .await
        .context("query Redis pending entries")
}

#[tokio::test]
#[ignore]
async fn writer_drains_pending_and_flushes_before_shutdown_ack() -> Result<()> {
    let suffix = format!(
        "{}_{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
    );
    let stream = format!("test:polymarket:v3:writer:{suffix}");
    let group = "clickhouse";
    let consumer = "clickhouse-1";
    let table = format!("polymarket_orderbook_v3_writer_test_{suffix}");
    let http = Client::new();
    clickhouse_query(
        &http,
        &format!("DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{table}"),
    )
    .await?;

    let redis = redis::Client::open(REDIS_URL)?;
    let mut conn = redis.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream)
        .arg(group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await?;
    let _: String = redis::cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("timestamp_received")
        .arg(timestamp_received(100))
        .arg("sequence")
        .arg(100_u64)
        .arg("data")
        .arg(data())
        .query_async(&mut conn)
        .await?;
    let _: String = redis::cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("timestamp_received")
        .arg(timestamp_received(101))
        .arg("sequence")
        .arg(101_u64)
        .arg("data")
        .arg(data())
        .query_async(&mut conn)
        .await?;
    let _: redis::Value = redis::cmd("XREADGROUP")
        .arg("GROUP")
        .arg(group)
        .arg(consumer)
        .arg("COUNT")
        .arg(2)
        .arg("STREAMS")
        .arg(&stream)
        .arg(">")
        .query_async(&mut conn)
        .await?;
    assert_eq!(pending_entries(&mut conn, &stream, group).await?.len(), 2);

    let _: String = redis::cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("timestamp_received")
        .arg(timestamp_received(102))
        .arg("sequence")
        .arg(102_u64)
        .arg("data")
        .arg(data())
        .query_async(&mut conn)
        .await?;

    let sink = ClickHouseSink::connect(ClickHouseConfig {
        url: CLICKHOUSE_URL.into(),
        user: "default".into(),
        password: clickhouse_password(),
        database: CLICKHOUSE_DATABASE.into(),
        table: table.clone(),
    })
    .await?;
    let writer = Writer::new(
        REDIS_URL,
        WriterConfig {
            stream: stream.clone(),
            group: group.into(),
            consumer: consumer.into(),
            reconnect_delay: Duration::from_millis(10),
            batch_size: 10,
            flush_interval: Duration::from_secs(30),
        },
        sink,
    )?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let writer_handle = tokio::spawn(writer.run(shutdown_rx));

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if pending_entries(&mut conn, &stream, group).await?.len() == 3 {
                return Ok::<(), anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("writer did not read pending and new entries")??;

    shutdown_tx.send(true).context("signal writer shutdown")?;
    tokio::time::timeout(Duration::from_secs(5), writer_handle)
        .await
        .context("writer did not shut down")?
        .context("writer task failed")??;

    let length: i64 = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut conn)
        .await?;
    assert_eq!(length, 0);
    assert!(pending_entries(&mut conn, &stream, group).await?.is_empty());

    let counts = clickhouse_query(
        &http,
        &format!(
            "SELECT count(), uniqExact(sequence), min(sequence), max(sequence) \
             FROM {CLICKHOUSE_DATABASE}.{table} FORMAT TabSeparated"
        ),
    )
    .await?;
    assert_eq!(counts.trim(), "3\t3\t100\t102");

    let rows = clickhouse_query(
        &http,
        &format!(
            "SELECT sequence, toUnixTimestamp64Nano(timestamp_received), hex(data) \
             FROM {CLICKHOUSE_DATABASE}.{table} FINAL \
             ORDER BY sequence FORMAT TabSeparated"
        ),
    )
    .await?;
    let raw_data = data();
    let expected = format!(
        "100\t{}\t{}\n101\t{}\t{}\n102\t{}\t{}",
        timestamp_received(100),
        hex(&raw_data),
        timestamp_received(101),
        hex(&raw_data),
        timestamp_received(102),
        hex(&raw_data),
    );
    assert_eq!(rows.trim(), expected);

    clickhouse_query(&http, &format!("DROP TABLE {CLICKHOUSE_DATABASE}.{table}")).await?;
    let _: i64 = redis::cmd("DEL")
        .arg(&stream)
        .query_async(&mut conn)
        .await?;
    Ok(())
}
