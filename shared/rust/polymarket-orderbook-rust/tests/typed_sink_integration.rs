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
