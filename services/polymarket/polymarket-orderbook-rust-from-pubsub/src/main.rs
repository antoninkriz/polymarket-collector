//! Polymarket orderbook → ClickHouse writer fed from a durable Redis Stream.
//!
//! Consumes the stream written by `polymarket-orderbook-rust-pubsub`,
//! deserializes each v3 record, and acknowledges it only after the ClickHouse
//! sink commits. No WS pool or market lifecycle processing runs here.
//!
//! ```text
//!   Redis Stream ──► consumer group ──► mpsc<SinkItem> ──► Sink ──► ClickHouse
//! ```

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use polymarket_orderbook_rust::sink::{Sink, SinkConfig, SinkItem};

use polymarket_orderbook_rust_from_pubsub::config::Config;
use polymarket_orderbook_rust_from_pubsub::pubsub_subscriber::{
    self, PubSubSubscriberConfig, SubscriberStats,
};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    init_tracing();
    let cfg = Config::from_env().context("load config from env")?;

    info!(
        redis_url = %cfg.redis_url,
        event_stream = %cfg.redis_event_stream,
        clickhouse_url = %cfg.clickhouse_url,
        clickhouse_database = %cfg.clickhouse_database,
        clickhouse_table = %cfg.clickhouse_table,
        flush_batch_size = cfg.flush_batch_size,
        flush_interval_ms = cfg.flush_interval.as_millis() as u64,
        delete_acked_stream_entries = cfg.delete_acked_stream_entries,
        queue_size = cfg.queue_size,
        ack_queue_size = cfg.ack_queue_size,
        "starting polymarket-orderbook-rust-from-pubsub",
    );

    let (event_tx, event_rx) = mpsc::channel::<SinkItem>(cfg.queue_size);
    let (ack_tx, ack_rx) = mpsc::channel::<Vec<String>>(cfg.ack_queue_size);
    let ack_monitor_tx = ack_tx.clone();
    let sink = Sink::connect(SinkConfig {
        url: cfg.clickhouse_url.clone(),
        user: cfg.clickhouse_user.clone(),
        password: cfg.clickhouse_password.clone(),
        database: cfg.clickhouse_database.clone(),
        table: cfg.clickhouse_table.clone(),
        batch_size: cfg.flush_batch_size,
        flush_interval: cfg.flush_interval,
    })
    .await
    .context("connect Polymarket ClickHouse sink")?;
    let mut sink_handle: JoinHandle<Result<()>> = tokio::spawn(sink.run(event_rx, ack_tx));

    let stats = SubscriberStats::new();
    let subscriber_cfg = PubSubSubscriberConfig {
        redis_url: cfg.redis_url.clone(),
        stream: cfg.redis_event_stream.clone(),
        group: cfg.stream_consumer_group.clone(),
        consumer: cfg.stream_consumer_name.clone(),
        delete_acked_entries: cfg.delete_acked_stream_entries,
        reconnect_delay: cfg.pubsub_reconnect_delay,
    };
    let subscriber_handle = tokio::spawn(pubsub_subscriber::run(
        subscriber_cfg,
        event_tx.clone(),
        ack_rx,
        Arc::clone(&stats),
    ));

    let stats_handle = tokio::spawn(stats_loop(
        event_tx.clone(),
        ack_monitor_tx,
        Arc::clone(&stats),
    ));

    wait_for_shutdown().await;
    info!("shutdown signal received");

    subscriber_handle.abort();
    let _ = subscriber_handle.await;
    stats_handle.abort();
    let _ = stats_handle.await;
    drop(event_tx);
    match tokio::time::timeout(Duration::from_secs(10), &mut sink_handle).await {
        Ok(Ok(Ok(()))) => info!("polymarket ClickHouse sink shut down cleanly"),
        Ok(Ok(Err(err))) => warn!(error = %err, "polymarket ClickHouse sink exited with error"),
        Ok(Err(err)) => warn!(error = %err, "polymarket ClickHouse sink task failed"),
        Err(_) => {
            warn!("polymarket ClickHouse sink did not drain before shutdown timeout");
            sink_handle.abort();
            let _ = sink_handle.await;
        }
    }
    info!("shutdown complete");
    Ok(())
}

async fn stats_loop(
    event_tx: mpsc::Sender<SinkItem>,
    ack_tx: mpsc::Sender<Vec<String>>,
    stats: Arc<SubscriberStats>,
) {
    use std::sync::atomic::Ordering;
    const SAMPLES_PER_REPORT: u64 = 60;
    const PRESSURE_THRESHOLD_PCT: f64 = 75.0;

    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    let mut samples = 0_u64;
    let mut event_queue_high_water = 0_usize;
    let mut ack_queue_high_water = 0_usize;
    loop {
        tick.tick().await;
        let event_queue_max = event_tx.max_capacity();
        let event_queue_used = event_queue_max.saturating_sub(event_tx.capacity());
        let ack_queue_max = ack_tx.max_capacity();
        let ack_queue_used = ack_queue_max.saturating_sub(ack_tx.capacity());
        event_queue_high_water = event_queue_high_water.max(event_queue_used);
        ack_queue_high_water = ack_queue_high_water.max(ack_queue_used);
        samples += 1;
        if samples < SAMPLES_PER_REPORT {
            continue;
        }
        samples = 0;

        let event_queue_pct = if event_queue_max > 0 {
            (event_queue_used as f64 / event_queue_max as f64) * 100.0
        } else {
            0.0
        };
        let event_queue_high_water_pct = if event_queue_max > 0 {
            (event_queue_high_water as f64 / event_queue_max as f64) * 100.0
        } else {
            0.0
        };
        let ack_queue_pct = if ack_queue_max > 0 {
            (ack_queue_used as f64 / ack_queue_max as f64) * 100.0
        } else {
            0.0
        };
        let ack_queue_high_water_pct = if ack_queue_max > 0 {
            (ack_queue_high_water as f64 / ack_queue_max as f64) * 100.0
        } else {
            0.0
        };
        let events_forwarded = stats.events_forwarded.load(Ordering::Relaxed);
        let events_acked = stats.events_acked.load(Ordering::Relaxed);
        info!(
            event_queue_size = event_queue_used,
            event_queue_high_water,
            event_queue_max,
            event_queue_pct = format!("{event_queue_pct:.1}"),
            event_queue_high_water_pct = format!("{event_queue_high_water_pct:.1}"),
            ack_queue_batches = ack_queue_used,
            ack_queue_high_water_batches = ack_queue_high_water,
            ack_queue_max_batches = ack_queue_max,
            ack_queue_pct = format!("{ack_queue_pct:.1}"),
            ack_queue_high_water_pct = format!("{ack_queue_high_water_pct:.1}"),
            events_received = stats.events_received.load(Ordering::Relaxed),
            events_forwarded,
            events_acked,
            forwarded_minus_acked = events_forwarded.saturating_sub(events_acked),
            events_deleted = stats.events_deleted.load(Ordering::Relaxed),
            parse_failures = stats.parse_failures.load(Ordering::Relaxed),
            reconnects = stats.reconnects.load(Ordering::Relaxed),
            "[POLYMARKET-FROM-PUBSUB-STATS]",
        );
        if event_queue_high_water_pct >= PRESSURE_THRESHOLD_PCT {
            warn!(
                event_queue_size = event_queue_used,
                event_queue_high_water,
                event_queue_max,
                event_queue_pct = format!("{event_queue_pct:.1}"),
                event_queue_high_water_pct = format!("{event_queue_high_water_pct:.1}"),
                "polymarket from-pubsub event queue high-water at or above 75%",
            );
        }
        if ack_queue_high_water_pct >= PRESSURE_THRESHOLD_PCT {
            warn!(
                ack_queue_batches = ack_queue_used,
                ack_queue_high_water_batches = ack_queue_high_water,
                ack_queue_max_batches = ack_queue_max,
                ack_queue_pct = format!("{ack_queue_pct:.1}"),
                ack_queue_high_water_pct = format!("{ack_queue_high_water_pct:.1}"),
                "polymarket acknowledgement queue high-water at or above 75%",
            );
        }
        event_queue_high_water = 0;
        ack_queue_high_water = 0;
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .json()
        .init();
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => info!("received SIGINT"),
            _ = sigterm.recv() => info!("received SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        info!("received SIGINT");
    }
}
