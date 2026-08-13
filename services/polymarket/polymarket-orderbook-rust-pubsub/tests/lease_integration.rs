//! End-to-end Redis test for publisher exclusivity and fenced appends.
//!
//! Run with a disposable Redis on port 16380:
//! `cargo test --test lease_integration -- --ignored --nocapture`.

use std::time::Duration;

use anyhow::Result;
use polymarket_orderbook_rust::events::Event;
use polymarket_orderbook_rust::record::CollectorContext;
use polymarket_orderbook_rust_pubsub::lease::{PublisherLease, PublisherLeaseConfig};
use polymarket_orderbook_rust_pubsub::pubsub_sink::{PubSubSink, PubSubSinkConfig};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

const REDIS_URL: &str = "redis://localhost:16380";

fn lease_config(suffix: &str) -> PublisherLeaseConfig {
    PublisherLeaseConfig {
        redis_url: REDIS_URL.into(),
        lease_key: format!("test:polymarket:v3:lease:{suffix}"),
        generation_key: format!("test:polymarket:v3:generation:{suffix}"),
        ttl: Duration::from_millis(500),
        renew_interval: Duration::from_millis(100),
    }
}

fn record(fence: u64) -> polymarket_orderbook_rust::record::EventRecord {
    CollectorContext::with_publisher_fence(fence)
        .next_message(0, 1, 0, 0, 1, 1_757_908_892_351_123_456)
        .record(
            Event::LastTradePrice {
                market: "m".into(),
                asset_id: "a".into(),
                timestamp: "1757908892351".into(),
                price: "0.42".parse().unwrap(),
                size: "75".parse().unwrap(),
                side: "BUY".into(),
                fee_rate_bps: "10".into(),
                transaction_hash: "0xtx".into(),
            },
            0,
            1,
            "{\"event_type\":\"last_trade_price\"}".into(),
        )
}

#[tokio::test]
#[ignore]
async fn lease_is_exclusive_renewed_and_fences_stale_appends() -> Result<()> {
    let suffix = Uuid::new_v4().to_string();
    let cfg = lease_config(&suffix);
    let stream = format!("test:polymarket:v3:events:{suffix}");

    let mut lease = PublisherLease::acquire(cfg.clone()).await?;
    assert_eq!(lease.generation(), 1);
    assert!(PublisherLease::acquire(cfg.clone()).await.is_err());

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let renewal = tokio::spawn(lease.clone().renew_until_shutdown(shutdown_rx));
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(PublisherLease::acquire(cfg.clone()).await.is_err());

    let sink = PubSubSink::connect(PubSubSinkConfig {
        redis_url: REDIS_URL.into(),
        stream: stream.clone(),
        publisher_lease_key: lease.key().into(),
        publisher_lease_token: lease.token().into(),
        batch_max: 1,
        linger: Duration::from_millis(1),
    })
    .await?;
    let (tx, rx) = mpsc::channel(1);
    let sink_task = tokio::spawn(sink.run(rx));
    tx.send(record(lease.generation())).await?;
    drop(tx);
    sink_task.await??;

    let client = redis::Client::open(REDIS_URL)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let length: i64 = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut conn)
        .await?;
    assert_eq!(length, 1);

    let stale_key = lease.key().to_string();
    let stale_token = lease.token().to_string();
    let _ = shutdown_tx.send(true);
    renewal.await??;
    assert!(lease.release().await?);

    let mut successor = PublisherLease::acquire(cfg.clone()).await?;
    assert_eq!(successor.generation(), 2);
    let stale_sink = PubSubSink::connect(PubSubSinkConfig {
        redis_url: REDIS_URL.into(),
        stream: stream.clone(),
        publisher_lease_key: stale_key,
        publisher_lease_token: stale_token,
        batch_max: 1,
        linger: Duration::from_millis(1),
    })
    .await?;
    let (tx, rx) = mpsc::channel(1);
    tx.send(record(1)).await?;
    drop(tx);
    let error = stale_sink.run(rx).await.unwrap_err().to_string();
    assert!(
        error.contains("lease was lost"),
        "unexpected error: {error}"
    );
    let length: i64 = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut conn)
        .await?;
    assert_eq!(length, 1);

    assert!(successor.release().await?);
    let _: i64 = redis::cmd("DEL")
        .arg(&[stream, cfg.lease_key, cfg.generation_key])
        .query_async(&mut conn)
        .await?;
    Ok(())
}
