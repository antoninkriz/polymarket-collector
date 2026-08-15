//! End-to-end Redis test for publisher exclusivity and fenced appends.
//!
//! Run with a disposable AOF-enabled Redis on port 16380:
//! `redis-server --port 16380 --appendonly yes --dir /spare/local/akriz/tmp/redis-lease-test`
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
        minimum_generation: 0,
        persist_timeout: Duration::from_secs(2),
        ttl: Duration::from_secs(4),
        renew_interval: Duration::from_secs(1),
    }
}

fn records(generation: u64) -> [polymarket_orderbook_rust::record::EventRecord; 2] {
    let collector = CollectorContext::with_publisher_generation(generation);
    let event = Event::LastTradePrice {
        market: "m".into(),
        asset_id: "a".into(),
        timestamp: "1757908892351".into(),
        price: "0.42".parse().unwrap(),
        size: "75".parse().unwrap(),
        side: "BUY".into(),
        fee_rate_bps: "10".into(),
        transaction_hash: "0xtx".into(),
    };
    [
        collector.record(event.clone(), 1_757_908_892_351_123_456),
        collector.record(event, 1_757_908_892_351_123_457),
    ]
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
        batch_max: 2,
        linger: Duration::from_millis(1),
    })
    .await?;
    let expected = records(lease.generation());
    let (tx, rx) = mpsc::channel(2);
    for record in expected.iter().cloned() {
        tx.send(record).await?;
    }
    drop(tx);
    sink.run(rx).await?;

    let client = redis::Client::open(REDIS_URL)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let length: i64 = redis::cmd("XLEN")
        .arg(&stream)
        .query_async(&mut conn)
        .await?;
    assert_eq!(length, 2);
    let entries: Vec<(String, Vec<String>)> = redis::cmd("XRANGE")
        .arg(&stream)
        .arg("-")
        .arg("+")
        .query_async(&mut conn)
        .await?;
    assert_eq!(entries.len(), expected.len());
    for ((_, fields), record) in entries.iter().zip(&expected) {
        assert_eq!(
            fields,
            &[
                "timestamp_received".to_string(),
                record.timestamp_received_ns.to_string(),
                "sequence".to_string(),
                record.sequence.to_string(),
                "data".to_string(),
                serde_json::to_string(&record.event)?,
            ],
        );
    }

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
    let [stale_record, _] = records(1);
    tx.send(stale_record).await?;
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
    assert_eq!(length, 2);

    assert!(successor.release().await?);
    let mut recovery_cfg = cfg.clone();
    recovery_cfg.minimum_generation = 10;
    let mut recovered = PublisherLease::acquire(recovery_cfg).await?;
    assert_eq!(recovered.generation(), 11);
    assert!(recovered.release().await?);

    let _: i64 = redis::cmd("DEL")
        .arg(&[stream, cfg.lease_key, cfg.generation_key])
        .query_async(&mut conn)
        .await?;
    Ok(())
}
