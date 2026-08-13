//! End-to-end test that the typed-schema sink writes rows to a real
//! ClickHouse instance with the expected column shapes.
//!
//! Hits the local CH at `http://localhost:8124` by default (matches the
//! docker-compose setup). Creates and drops a uniquely-named test table so
//! it can be re-run safely without colliding with production data.
//!
//! Skipped by default (`#[ignore]`); run with:
//!
//! ```bash
//! cargo test --release --test typed_sink_integration -- --ignored --nocapture
//! ```

use std::time::Duration;

use anyhow::Result;
use polymarket_orderbook_rust::events::{Event, Level};
use polymarket_orderbook_rust::record::CollectorContext;
use polymarket_orderbook_rust::sink::{Sink, SinkConfig, SinkItem, SinkSchema};
use reqwest::Client;
use rust_decimal::Decimal;
use tokio::sync::mpsc;

const CH_URL: &str = "http://localhost:8124";
const CH_USER: &str = "default";
fn ch_password() -> String {
    std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default()
}
const CH_DATABASE: &str = "default";
const CH_TABLE: &str = "polymarket_orderbook_rust_test_typed_cli";
const CH_V3_TABLE: &str = "polymarket_orderbook_rust_test_v3_cli";

async fn drop_v3_objects(http: &Client) -> Result<()> {
    query(
        http,
        &format!("DROP TABLE IF EXISTS {CH_DATABASE}.{CH_V3_TABLE}"),
    )
    .await?;
    Ok(())
}

fn dec(s: &str) -> Decimal {
    s.parse().unwrap()
}

async fn query(http: &Client, sql: &str) -> Result<String> {
    let resp = http
        .post(CH_URL)
        .basic_auth(CH_USER, Some(&ch_password()))
        .body(sql.to_string())
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await?;
    anyhow::ensure!(status.is_success(), "query {sql:?} failed: {status} {text}");
    Ok(text)
}

#[tokio::test]
#[ignore]
async fn typed_sink_writes_to_clickhouse_end_to_end() -> Result<()> {
    let http = Client::new();

    // Clean slate.
    query(
        &http,
        &format!("DROP TABLE IF EXISTS {CH_DATABASE}.{CH_TABLE}"),
    )
    .await?;

    // Build a sink in typed mode. drop_table_on_start is redundant with the
    // DROP above, but exercises the code path.
    let sink = Sink::connect(SinkConfig {
        url: CH_URL.into(),
        user: CH_USER.into(),
        password: ch_password(),
        database: CH_DATABASE.into(),
        table: CH_TABLE.into(),
        drop_table_on_start: true,
        exclude_hash: true,
        batch_size: 4,
        flush_interval: Duration::from_millis(200),
        ttl_minutes: 0,
        schema: SinkSchema::Typed,
    })
    .await?;

    let (tx, rx) = mpsc::channel::<SinkItem>(16);
    let handle = tokio::spawn(sink.run(rx, None));

    // One event of each variant — covers every column-population pattern
    // from docs/data-dump-optimizations.md.
    let events = vec![
        Event::Book {
            market: "0x0000000000000000000000000000000000000000000000000000000000000abc".into(),
            asset_id: "asset-book".into(),
            timestamp: "1757908892351".into(),
            bids: vec![Level {
                price: dec("0.41"),
                size: dec("100"),
            }],
            asks: vec![Level {
                price: dec("0.42"),
                size: dec("200.5"),
            }],
            hash: Some("h".into()),
        },
        Event::PriceChange {
            market: "0x0000000000000000000000000000000000000000000000000000000000000abc".into(),
            asset_id: "asset-pc".into(),
            timestamp: "1757908892360".into(),
            best_bid: Some(dec("0.40")),
            best_ask: Some(dec("0.42")),
            hash: Some("h".into()),
            price: dec("0.41"),
            size: dec("100"),
            side: "BUY".into(),
        },
        Event::LastTradePrice {
            market: "0x0000000000000000000000000000000000000000000000000000000000000abc".into(),
            asset_id: "asset-trade".into(),
            timestamp: "1757908892370".into(),
            price: dec("0.42"),
            size: dec("75.0625"),
            side: "BUY".into(),
            fee_rate_bps: "1000".into(),
            transaction_hash: "0xdeadbeef".into(),
        },
        Event::TickSizeChange {
            market: "0x0000000000000000000000000000000000000000000000000000000000000abc".into(),
            asset_id: "asset-tick".into(),
            timestamp: "1757908892380".into(),
            old_tick_size: dec("0.01"),
            new_tick_size: dec("0.001"),
        },
    ];
    for ev in events {
        tx.send(ev.into()).await?;
    }
    // Dropping the tx closes the channel — sink drains and exits.
    drop(tx);
    handle.await??;

    // -- Verify ----------------------------------------------------------

    let count: u64 = query(
        &http,
        &format!("SELECT count() FROM {CH_DATABASE}.{CH_TABLE} FORMAT TabSeparated"),
    )
    .await?
    .trim()
    .parse()?;
    assert_eq!(count, 4, "expected 4 rows, got {count}");

    // Pull each row back as JSON so we can assert against field shapes.
    let rows = query(
        &http,
        &format!(
            "SELECT event_type, asset_id, bids, asks, price, size, side, \
                    best_bid, best_ask, fee_rate_bps, transaction_hash, \
                    old_tick_size, new_tick_size \
             FROM {CH_DATABASE}.{CH_TABLE} \
             ORDER BY timestamp \
             FORMAT JSONEachRow"
        ),
    )
    .await?;
    let rows: Vec<serde_json::Value> = rows
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows.len(), 4);

    // Row 0: book — depth populated, other columns at default.
    let r = &rows[0];
    assert_eq!(r["event_type"], "book");
    assert_eq!(r["asset_id"], "asset-book");
    // bids/asks come back as CH's Array(Tuple) JSON form: [[price, size]].
    // Decimal32(4) → numeric JSON with up to 4-decimal precision.
    assert_eq!(r["bids"], serde_json::json!([[0.41, 100]]));
    assert_eq!(r["asks"], serde_json::json!([[0.42, 200.5]]));
    assert_eq!(r["price"], 0);
    assert_eq!(r["size"], 0);
    assert_eq!(r["side"], "");
    assert_eq!(r["fee_rate_bps"], 0);
    assert_eq!(r["transaction_hash"], "");

    // Row 1: price_change.
    let r = &rows[1];
    assert_eq!(r["event_type"], "price_change");
    assert_eq!(r["asset_id"], "asset-pc");
    assert_eq!(r["bids"], serde_json::json!([]));
    assert_eq!(r["asks"], serde_json::json!([]));
    assert_eq!(r["price"], 0.41);
    assert_eq!(r["size"], 100);
    assert_eq!(r["side"], "BUY");
    assert_eq!(r["best_bid"], 0.4);
    assert_eq!(r["best_ask"], 0.42);

    // Row 2: last_trade_price.
    let r = &rows[2];
    assert_eq!(r["event_type"], "last_trade_price");
    assert_eq!(r["asset_id"], "asset-trade");
    assert_eq!(r["price"], 0.42);
    assert_eq!(r["size"], 75.0625);
    assert_eq!(r["fee_rate_bps"], 1000);
    assert_eq!(r["transaction_hash"], "0xdeadbeef");

    // Row 3: tick_size_change.
    let r = &rows[3];
    assert_eq!(r["event_type"], "tick_size_change");
    assert_eq!(r["old_tick_size"], 0.01);
    assert_eq!(r["new_tick_size"], 0.001);

    // Confirm the DDL actually used the typed schema (LowCardinality +
    // Delta + parallel ORDER BY). `system.tables` is the source of truth.
    let ddl = query(
        &http,
        &format!(
            "SELECT create_table_query FROM system.tables \
             WHERE database = '{CH_DATABASE}' AND name = '{CH_TABLE}' \
             FORMAT TabSeparatedRaw"
        ),
    )
    .await?;
    // CH normalizes `Delta` → `Delta(8)` (inferred byte-width for DateTime64)
    // and `Decimal32(4)` → `Decimal(9, 4)` when reading back, so match the
    // normalized forms.
    assert!(
        ddl.contains("LowCardinality(FixedString(66))"),
        "DDL: {ddl}"
    );
    assert!(ddl.contains("CODEC(Delta(8), ZSTD(3))"), "DDL: {ddl}");
    assert!(
        ddl.contains("ORDER BY (market, asset_id, timestamp_received)"),
        "DDL: {ddl}"
    );

    // Clean up so the test is idempotent.
    query(&http, &format!("DROP TABLE {CH_DATABASE}.{CH_TABLE}")).await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn v3_sink_collapses_only_same_sequence_retry() -> Result<()> {
    let http = Client::new();
    drop_v3_objects(&http).await?;

    let sink = Sink::connect(SinkConfig {
        url: CH_URL.into(),
        user: CH_USER.into(),
        password: ch_password(),
        database: CH_DATABASE.into(),
        table: CH_V3_TABLE.into(),
        drop_table_on_start: true,
        exclude_hash: false,
        batch_size: 3,
        flush_interval: Duration::from_millis(200),
        ttl_minutes: 0,
        schema: SinkSchema::V3,
    })
    .await?;
    let trade = Event::LastTradePrice {
        market: "0xmarket".into(),
        asset_id: "asset".into(),
        timestamp: "1757908892351".into(),
        price: dec("0.42"),
        size: dec("75"),
        side: "BUY".into(),
        fee_rate_bps: "10".into(),
        transaction_hash: "0xsame-transaction".into(),
    };
    let collector = CollectorContext::with_publisher_generation(7);
    let first = collector.record(trade.clone(), 1_757_908_892_351_123_456);
    let second = collector.record(trade, 1_757_908_892_351_123_457);

    let (tx, rx) = mpsc::channel::<SinkItem>(8);
    let handle = tokio::spawn(sink.run(rx, None));
    tx.send(SinkItem {
        record: first.clone(),
        delivery_id: Some("1-0".into()),
    })
    .await?;
    tx.send(SinkItem {
        record: first,
        delivery_id: Some("1-1".into()),
    })
    .await?;
    tx.send(SinkItem {
        record: second,
        delivery_id: Some("1-2".into()),
    })
    .await?;
    drop(tx);
    handle.await??;

    let final_count = query(
        &http,
        &format!("SELECT count() FROM {CH_DATABASE}.{CH_V3_TABLE} FINAL FORMAT TabSeparated"),
    )
    .await?;
    assert_eq!(final_count.trim(), "2");

    let rows = query(
        &http,
        &format!(
            "SELECT sequence, toUnixTimestamp64Nano(timestamp_received), data \
             FROM {CH_DATABASE}.{CH_V3_TABLE} FINAL \
             ORDER BY sequence FORMAT TabSeparated"
        ),
    )
    .await?;
    let rows: Vec<&str> = rows.lines().collect();
    assert_eq!(rows.len(), 2);
    let first_sequence = 7_u64 << 48;
    assert!(rows[0].starts_with(&format!("{first_sequence}\t1757908892351123456\t")));
    assert!(rows[1].starts_with(&format!("{}\t1757908892351123457\t", first_sequence + 1,)));
    assert!(rows.iter().all(|row| row.contains("0xsame-transaction")));

    let ddl = query(
        &http,
        &format!(
            "SELECT create_table_query FROM system.tables \
             WHERE database = '{CH_DATABASE}' AND name = '{CH_V3_TABLE}' \
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
             WHERE database = '{CH_DATABASE}' AND table = '{CH_V3_TABLE}' \
             ORDER BY position FORMAT TabSeparated"
        ),
    )
    .await?;
    assert_eq!(
        columns.lines().collect::<Vec<_>>(),
        ["timestamp_received", "sequence", "data"],
    );

    drop_v3_objects(&http).await?;
    Ok(())
}
