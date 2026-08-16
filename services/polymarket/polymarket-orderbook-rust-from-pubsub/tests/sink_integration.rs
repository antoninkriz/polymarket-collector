//! End-to-end raw-row sink test against a disposable ClickHouse instance.
//!
//! Run with ClickHouse listening on `localhost:8124`:
//!
//! ```bash
//! cargo test --test sink_integration -- --ignored --nocapture
//! ```

use anyhow::Result;
use polymarket_orderbook_rust_from_pubsub::clickhouse::{ClickHouseConfig, ClickHouseSink, RawRow};
use reqwest::Client;

const CH_URL: &str = "http://localhost:8124";
const CH_USER: &str = "default";
const CH_DATABASE: &str = "default";
const CH_TABLE: &str = "polymarket_orderbook_v3_sink_test";

fn ch_password() -> String {
    std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default()
}

fn row(timestamp_received: i64, sequence: u64, data: &str) -> RawRow {
    RawRow {
        timestamp_received,
        sequence,
        data: data.into(),
    }
}

async fn query(http: &Client, sql: &str) -> Result<String> {
    let response = http
        .post(CH_URL)
        .basic_auth(CH_USER, Some(&ch_password()))
        .body(sql.to_owned())
        .send()
        .await?;
    let status = response.status();
    let text = response.text().await?;
    anyhow::ensure!(status.is_success(), "query {sql:?} failed: {status} {text}");
    Ok(text)
}

#[tokio::test]
#[ignore]
async fn sink_collapses_only_same_sequence_retry() -> Result<()> {
    let http = Client::new();
    query(
        &http,
        &format!("DROP TABLE IF EXISTS {CH_DATABASE}.{CH_TABLE}"),
    )
    .await?;

    let mut sink = ClickHouseSink::connect(ClickHouseConfig {
        url: CH_URL.into(),
        user: CH_USER.into(),
        password: ch_password(),
        database: CH_DATABASE.into(),
        table: CH_TABLE.into(),
    })
    .await?;
    let data = serde_json::json!({
        "event_type": "last_trade_price",
        "market": "0xmarket",
        "asset_id": "asset",
        "timestamp": "1757908892351",
        "price": "0.42",
        "size": "75",
        "side": "BUY",
        "fee_rate_bps": "10",
        "transaction_hash": "0xsame-transaction",
    })
    .to_string();
    let first_sequence = 7_u64 << 48;
    let batch = [
        row(1_757_908_892_351_123_456, first_sequence, &data),
        row(1_757_908_892_351_123_456, first_sequence, &data),
        row(1_757_908_892_351_123_457, first_sequence + 1, &data),
    ];
    sink.insert(batch.iter()).await?;

    let final_count = query(
        &http,
        &format!("SELECT count() FROM {CH_DATABASE}.{CH_TABLE} FINAL FORMAT TabSeparated"),
    )
    .await?;
    assert_eq!(final_count.trim(), "2");

    let rows = query(
        &http,
        &format!(
            "SELECT sequence, toUnixTimestamp64Nano(timestamp_received), data \
             FROM {CH_DATABASE}.{CH_TABLE} FINAL \
             ORDER BY sequence FORMAT TabSeparated"
        ),
    )
    .await?;
    let rows = rows.lines().collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].starts_with(&format!("{first_sequence}\t1757908892351123456\t")));
    assert!(rows[1].starts_with(&format!("{}\t1757908892351123457\t", first_sequence + 1,)));
    assert!(rows.iter().all(|row| row.contains("0xsame-transaction")));

    let ddl = query(
        &http,
        &format!(
            "SELECT create_table_query FROM system.tables \
             WHERE database = '{CH_DATABASE}' AND name = '{CH_TABLE}' \
             FORMAT TabSeparatedRaw"
        ),
    )
    .await?;
    assert!(ddl.contains("ReplacingMergeTree"), "DDL: {ddl}");
    assert!(ddl.contains("ORDER BY sequence"), "DDL: {ddl}");
    assert!(ddl.contains("DateTime64(9, 'UTC')"), "DDL: {ddl}");
    assert!(ddl.contains("ZSTD(1)"), "DDL: {ddl}");

    let columns = query(
        &http,
        &format!(
            "SELECT name FROM system.columns \
             WHERE database = '{CH_DATABASE}' AND table = '{CH_TABLE}' \
             ORDER BY position FORMAT TabSeparated"
        ),
    )
    .await?;
    assert_eq!(
        columns.lines().collect::<Vec<_>>(),
        ["timestamp_received", "sequence", "data"],
    );

    query(&http, &format!("DROP TABLE {CH_DATABASE}.{CH_TABLE}")).await?;
    Ok(())
}
