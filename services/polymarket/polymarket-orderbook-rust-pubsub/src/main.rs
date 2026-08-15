//! Polymarket orderbook → durable Redis Stream publisher.
//!
//! Mirrors the streaming pipeline of the sibling `polymarket-orderbook-rust`
//! service, but the terminal sink appends v3 records to a Redis Stream
//! instead of inserting into ClickHouse.
//!
//! ```text
//!   Redis restart cache + Gamma reconciliation ──────┐
//!   WS lifecycle routes ──> deduplicated updates ────┤
//!                                                    v
//!   Polymarket WS pool ── (mpsc) ──> Redis XADD (durable event stream)
//! ```

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use polymarket_orderbook_rust::events::{Market, MarketLifecycleObservation};
use polymarket_orderbook_rust::markets;
use polymarket_orderbook_rust::markets::lifecycle::LifecycleRequest;
use polymarket_orderbook_rust::record::EventRecord;
use polymarket_orderbook_rust::ws::pool::Pool;

use polymarket_orderbook_rust_pubsub::config::Config;
use polymarket_orderbook_rust_pubsub::gamma_reconcile::{self, ReconciliationStats};
use polymarket_orderbook_rust_pubsub::lease::{PublisherLease, PublisherLeaseConfig};
use polymarket_orderbook_rust_pubsub::market_lifecycle::LifecycleCoordinator;
use polymarket_orderbook_rust_pubsub::pubsub_sink::{PubSubSink, PubSubSinkConfig};
use polymarket_orderbook_rust_pubsub::sequence_watermark::clickhouse_generation_floor;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    init_tracing();
    let cfg = Config::from_env().context("load config from env")?;
    let clickhouse_generation_floor = clickhouse_generation_floor(&cfg)
        .await
        .context("recover durable publisher generation")?;
    let minimum_generation = clickhouse_generation_floor.max(cfg.publisher_generation_floor);
    let mut publisher_lease = PublisherLease::acquire(PublisherLeaseConfig {
        redis_url: cfg.redis_url.clone(),
        lease_key: cfg.publisher_lease_key.clone(),
        generation_key: cfg.publisher_lease_generation_key.clone(),
        minimum_generation,
        persist_timeout: cfg.publisher_generation_persist_timeout,
        ttl: cfg.publisher_lease_ttl,
        renew_interval: cfg.publisher_lease_renew_interval,
    })
    .await
    .context("acquire authoritative publisher lease")?;
    let publisher_fence = publisher_lease.generation();
    info!(
        redis_url = %cfg.redis_url,
        event_stream = %cfg.redis_event_stream,
        max_assets_per_conn = cfg.max_assets_per_conn,
        queue_size = cfg.queue_size,
        lifecycle_queue_size = cfg.lifecycle_queue_size,
        publish_batch_max = cfg.publish_batch_max,
        publish_linger_ms = cfg.publish_linger.as_millis() as u64,
        publisher_fence,
        clickhouse_generation_floor,
        configured_generation_floor = cfg.publisher_generation_floor,
        "starting polymarket-orderbook-rust-pubsub",
    );

    let (lease_shutdown_tx, lease_shutdown_rx) = watch::channel(false);
    let (lease_failure_tx, mut lease_failure_rx) = mpsc::channel::<String>(1);
    let renewal_lease = publisher_lease.clone();
    let lease_handle = tokio::spawn(async move {
        let result = renewal_lease.renew_until_shutdown(lease_shutdown_rx).await;
        if let Err(error) = &result {
            let _ = lease_failure_tx.send(error.to_string()).await;
        }
        result
    });

    let sink = PubSubSink::connect(PubSubSinkConfig {
        redis_url: cfg.redis_url.clone(),
        stream: cfg.redis_event_stream.clone(),
        publisher_lease_key: publisher_lease.key().to_string(),
        publisher_lease_token: publisher_lease.token().to_string(),
        batch_max: cfg.publish_batch_max,
        linger: cfg.publish_linger,
    })
    .await
    .context("connect Redis stream sink")?;

    let (event_tx, event_rx) = mpsc::channel::<EventRecord>(cfg.queue_size);
    let mut sink_handle: JoinHandle<Result<()>> = tokio::spawn(sink.run(event_rx));

    let (websocket_lifecycle_tx, websocket_lifecycle_rx) = mpsc::channel(cfg.lifecycle_queue_size);
    let (reconciliation_tx, reconciliation_rx) = mpsc::channel(cfg.lifecycle_queue_size);
    let pool = Arc::new(Mutex::new(Pool::new_with_lifecycle(
        cfg.max_assets_per_conn,
        event_tx.clone(),
        publisher_fence,
        websocket_lifecycle_tx.clone(),
    )));
    pool.lock().await.start().await;

    let coordinator =
        LifecycleCoordinator::new(Arc::clone(&pool), websocket_lifecycle_rx, reconciliation_rx);
    let mut coordinator_handle = tokio::spawn(coordinator.run());

    let gamma_http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("build Gamma HTTP client")?;
    let gamma_client = markets::gamma::GammaClient::new(gamma_http);

    let (mut cached_markets, cache_fetched_at) = load_restart_cache(&cfg).await;
    let cold_start = cached_markets.is_empty();
    if !cached_markets.is_empty() {
        match gamma_reconcile::prioritize_restart_markets(
            &gamma_client,
            &mut cached_markets,
            Utc::now(),
        )
        .await
        {
            Ok(prioritized) => info!(prioritized, "prioritized recent restart-cache markets"),
            Err(error) => warn!(%error, "could not prioritize restart-cache markets"),
        }
        let market_count = cached_markets.len();
        apply_bootstrap(&reconciliation_tx, cached_markets).await?;
        if market_count > 0 {
            info!(
                markets = pool.lock().await.subscribed_market_count(),
                "pre-loaded markets",
            );
        }
    }

    let reconciliation_stats = Arc::new(ReconciliationStats::default());
    let (cache_trigger_tx, cache_trigger_rx) = mpsc::channel(1);
    let (gamma_shutdown_tx, gamma_shutdown_rx) = watch::channel(false);
    let mut full_scan_handle: JoinHandle<Result<()>> =
        tokio::spawn(gamma_reconcile::run_full_scans(
            gamma_client.clone(),
            reconciliation_tx.clone(),
            cold_start,
            cache_trigger_tx,
            Arc::clone(&reconciliation_stats),
            gamma_shutdown_rx,
        ));
    let startup_poll_time = Utc::now();
    let mut new_poll_handle = tokio::spawn(gamma_reconcile::run_new_market_polls(
        gamma_client.clone(),
        reconciliation_tx.clone(),
        startup_poll_time,
        Arc::clone(&reconciliation_stats),
    ));
    let mut closed_poll_handle = tokio::spawn(gamma_reconcile::run_closed_market_polls(
        gamma_client,
        reconciliation_tx.clone(),
        cache_fetched_at.unwrap_or(startup_poll_time),
        Arc::clone(&reconciliation_stats),
    ));
    let mut cache_saver_handle = tokio::spawn(gamma_reconcile::run_cache_saver(
        reconciliation_tx.clone(),
        cfg.redis_url.clone(),
        cfg.redis_key_active_markets.clone(),
        cache_trigger_rx,
        Arc::clone(&reconciliation_stats),
    ));

    let stats_pool = Arc::clone(&pool);
    let stats_event_tx = event_tx.clone();
    let stats_websocket_lifecycle_tx = websocket_lifecycle_tx.clone();
    let stats_reconciliation_tx = reconciliation_tx.clone();
    let stats_reconciliation = Arc::clone(&reconciliation_stats);
    let stats_handle = tokio::spawn(async move {
        stats_loop(
            stats_pool,
            stats_event_tx,
            stats_websocket_lifecycle_tx,
            stats_reconciliation_tx,
            stats_reconciliation,
        )
        .await;
    });

    let mut sink_outcome = None;
    let mut coordinator_stopped = false;
    let mut full_scan_stopped = false;
    let mut new_poll_stopped = false;
    let mut closed_poll_stopped = false;
    let mut cache_saver_stopped = false;
    tokio::select! {
        _ = wait_for_shutdown() => info!("shutdown signal received"),
        lease_error = lease_failure_rx.recv() => {
            warn!(error = lease_error.unwrap_or_else(|| "lease monitor stopped".into()), "publisher lease failed; stopping collector");
        }
        outcome = &mut sink_handle => {
            warn!(?outcome, "Redis event sink stopped; stopping collector");
            sink_outcome = Some(outcome);
        }
        outcome = &mut coordinator_handle => {
            warn!(?outcome, "market lifecycle coordinator stopped; stopping collector");
            coordinator_stopped = true;
        }
        outcome = &mut full_scan_handle => {
            warn!(?outcome, "Gamma full scan task stopped; stopping collector");
            full_scan_stopped = true;
        }
        outcome = &mut new_poll_handle => {
            warn!(?outcome, "Gamma new-market poll task stopped; stopping collector");
            new_poll_stopped = true;
        }
        outcome = &mut closed_poll_handle => {
            warn!(?outcome, "Gamma closed-market poll task stopped; stopping collector");
            closed_poll_stopped = true;
        }
        outcome = &mut cache_saver_handle => {
            warn!(?outcome, "market restart-cache task stopped; stopping collector");
            cache_saver_stopped = true;
        }
    }

    let _ = gamma_shutdown_tx.send(true);
    if !full_scan_stopped {
        let _ = full_scan_handle.await;
    }
    if !new_poll_stopped {
        new_poll_handle.abort();
        let _ = new_poll_handle.await;
    }
    if !closed_poll_stopped {
        closed_poll_handle.abort();
        let _ = closed_poll_handle.await;
    }
    if !cache_saver_stopped {
        cache_saver_handle.abort();
        let _ = cache_saver_handle.await;
    }

    if !coordinator_stopped {
        if let Err(error) = gamma_reconcile::persist_final_snapshot(
            &reconciliation_tx,
            &cfg.redis_url,
            &cfg.redis_key_active_markets,
        )
        .await
        {
            warn!(%error, "final market restart-cache save failed");
        }
    }

    if !coordinator_stopped {
        coordinator_handle.abort();
        let _ = coordinator_handle.await;
    }
    info!("market lifecycle coordinator stopped");

    stats_handle.abort();
    let _ = stats_handle.await;

    drop(event_tx);

    let pool = match Arc::try_unwrap(pool) {
        Ok(mutex) => mutex.into_inner(),
        Err(arc) => {
            warn!(
                strong_count = Arc::strong_count(&arc),
                "pool still has other strong refs at shutdown; leaking",
            );
            return Ok(());
        }
    };

    pool.shutdown().await.context("pool shutdown")?;
    info!("pool shut down");

    let sink_outcome = match sink_outcome {
        Some(outcome) => outcome,
        None => sink_handle.await,
    };
    match sink_outcome {
        Ok(Ok(())) => info!("sink shut down cleanly"),
        Ok(Err(e)) => warn!(error = %e, "sink ended with error"),
        Err(e) => warn!(error = %e, "sink task panicked"),
    }

    let _ = lease_shutdown_tx.send(true);
    match lease_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(%error, "publisher lease renewal stopped with error"),
        Err(error) => warn!(%error, "publisher lease renewal task failed"),
    }
    match publisher_lease.release().await {
        Ok(true) => info!(publisher_fence, "released authoritative publisher lease"),
        Ok(false) => warn!(
            publisher_fence,
            "publisher lease was no longer owned at shutdown"
        ),
        Err(error) => warn!(%error, publisher_fence, "failed to release publisher lease"),
    }

    info!("shutdown complete");
    Ok(())
}

async fn apply_bootstrap(
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
    markets: Vec<Market>,
) -> Result<()> {
    let (completion, completed) = oneshot::channel();
    lifecycle_tx
        .send(LifecycleRequest::Bootstrap {
            markets,
            completion,
        })
        .await
        .context("lifecycle coordinator stopped before bootstrap")?;
    completed
        .await
        .context("lifecycle coordinator dropped bootstrap completion")?
        .map_err(anyhow::Error::msg)
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

async fn load_restart_cache(cfg: &Config) -> (Vec<Market>, Option<DateTime<Utc>>) {
    if cfg.skip_active_markets_cache {
        info!("SKIP_ACTIVE_MARKETS_CACHE=true, starting cold Gamma reconciliation");
        return (Vec::new(), None);
    }
    match markets::redis_cache::load_document(&cfg.redis_url, &cfg.redis_key_active_markets).await {
        Ok(Some(document)) => {
            let fetched_at = document.fetched_at();
            (document.into_markets(), Some(fetched_at))
        }
        Ok(None) => {
            info!("market restart cache is absent; starting cold Gamma reconciliation");
            (Vec::new(), None)
        }
        Err(error) => {
            warn!(%error, "market restart cache is invalid; starting cold Gamma reconciliation");
            (Vec::new(), None)
        }
    }
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
    }
}

async fn stats_loop(
    pool: Arc<Mutex<Pool>>,
    event_tx: mpsc::Sender<EventRecord>,
    websocket_lifecycle_tx: mpsc::Sender<MarketLifecycleObservation>,
    reconciliation_tx: mpsc::Sender<LifecycleRequest>,
    reconciliation_stats: Arc<ReconciliationStats>,
) {
    const SAMPLES_PER_REPORT: u64 = 60;
    const PRESSURE_THRESHOLD_PCT: f64 = 50.0;

    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;

    let mut iteration = 0_u64;
    let mut samples = 0_u64;
    let mut queue_high_water = 0_usize;
    let mut websocket_lifecycle_high_water = 0_usize;
    let mut reconciliation_high_water = 0_usize;
    loop {
        tick.tick().await;

        let queue_max = event_tx.max_capacity();
        let queue_used = queue_max.saturating_sub(event_tx.capacity());
        let websocket_lifecycle_max = websocket_lifecycle_tx.max_capacity();
        let websocket_lifecycle_used =
            websocket_lifecycle_max.saturating_sub(websocket_lifecycle_tx.capacity());
        let reconciliation_max = reconciliation_tx.max_capacity();
        let reconciliation_used = reconciliation_max.saturating_sub(reconciliation_tx.capacity());
        queue_high_water = queue_high_water.max(queue_used);
        websocket_lifecycle_high_water =
            websocket_lifecycle_high_water.max(websocket_lifecycle_used);
        reconciliation_high_water = reconciliation_high_water.max(reconciliation_used);
        samples += 1;
        if samples < SAMPLES_PER_REPORT {
            continue;
        }
        samples = 0;
        iteration += 1;

        let stats = {
            let p = pool.lock().await;
            p.pool_stats()
        };
        let reconciliation_ages = reconciliation_stats.ages();

        let queue_pct = if queue_max > 0 {
            (queue_used as f64 / queue_max as f64) * 100.0
        } else {
            0.0
        };
        let queue_high_water_pct = if queue_max > 0 {
            (queue_high_water as f64 / queue_max as f64) * 100.0
        } else {
            0.0
        };
        let websocket_lifecycle_high_water_pct = if websocket_lifecycle_max > 0 {
            (websocket_lifecycle_high_water as f64 / websocket_lifecycle_max as f64) * 100.0
        } else {
            0.0
        };
        let reconciliation_high_water_pct = if reconciliation_max > 0 {
            (reconciliation_high_water as f64 / reconciliation_max as f64) * 100.0
        } else {
            0.0
        };

        info!(
            iter = iteration,
            queue_size = queue_used,
            queue_high_water,
            queue_max,
            queue_pct = format!("{:.1}", queue_pct),
            queue_high_water_pct = format!("{:.1}", queue_high_water_pct),
            websocket_lifecycle_queue_size = websocket_lifecycle_used,
            websocket_lifecycle_queue_high_water = websocket_lifecycle_high_water,
            websocket_lifecycle_queue_max = websocket_lifecycle_max,
            websocket_lifecycle_queue_high_water_pct =
                format!("{:.1}", websocket_lifecycle_high_water_pct),
            reconciliation_queue_size = reconciliation_used,
            reconciliation_queue_high_water = reconciliation_high_water,
            reconciliation_queue_max = reconciliation_max,
            reconciliation_queue_high_water_pct = format!("{:.1}", reconciliation_high_water_pct),
            subscribed_markets = stats.market_count,
            connections = stats.connection_count,
            lifecycle_listeners = stats.lifecycle_listener_count,
            gamma_full_scan_age_s = ?reconciliation_ages.full_scan_seconds,
            gamma_new_poll_age_s = ?reconciliation_ages.new_poll_seconds,
            gamma_closed_poll_age_s = ?reconciliation_ages.closed_poll_seconds,
            restart_cache_save_age_s = ?reconciliation_ages.cache_save_seconds,
            asset_down_events = stats.asset_down_events,
            conn_down_events = stats.conn_down_events,
            "[QUEUE-STATS]",
        );

        info!(
            grafana = true,
            event = "pool_stats",
            conns_down = stats.conns_down,
            assets_down = stats.assets_down,
            meta = %serde_json::json!({
                "iter": iteration,
                "subscribed_markets": stats.market_count,
                "connections": stats.connection_count,
                "lifecycle_listeners": stats.lifecycle_listener_count,
                "queue_size": queue_used,
                "queue_high_water": queue_high_water,
                "queue_max": queue_max,
                "queue_pct": format!("{:.1}", queue_pct),
                "queue_high_water_pct": format!("{:.1}", queue_high_water_pct),
                "websocket_lifecycle_queue_size": websocket_lifecycle_used,
                "websocket_lifecycle_queue_high_water": websocket_lifecycle_high_water,
                "websocket_lifecycle_queue_max": websocket_lifecycle_max,
                "websocket_lifecycle_queue_high_water_pct": format!("{:.1}", websocket_lifecycle_high_water_pct),
                "reconciliation_queue_size": reconciliation_used,
                "reconciliation_queue_high_water": reconciliation_high_water,
                "reconciliation_queue_max": reconciliation_max,
                "reconciliation_queue_high_water_pct": format!("{:.1}", reconciliation_high_water_pct),
                "asset_down_events_total": stats.asset_down_events,
                "conn_down_events_total": stats.conn_down_events,
                "gamma_full_scan_age_s": reconciliation_ages.full_scan_seconds,
                "gamma_new_poll_age_s": reconciliation_ages.new_poll_seconds,
                "gamma_closed_poll_age_s": reconciliation_ages.closed_poll_seconds,
                "restart_cache_save_age_s": reconciliation_ages.cache_save_seconds,
            }),
            "pool_stats",
        );

        if queue_high_water_pct >= PRESSURE_THRESHOLD_PCT
            || websocket_lifecycle_high_water_pct >= PRESSURE_THRESHOLD_PCT
            || reconciliation_high_water_pct >= PRESSURE_THRESHOLD_PCT
        {
            warn!(
                queue_size = queue_used,
                queue_high_water,
                queue_max,
                queue_pct = format!("{:.1}", queue_pct),
                queue_high_water_pct = format!("{:.1}", queue_high_water_pct),
                websocket_lifecycle_queue_high_water = websocket_lifecycle_high_water,
                websocket_lifecycle_queue_max = websocket_lifecycle_max,
                websocket_lifecycle_queue_high_water_pct =
                    format!("{:.1}", websocket_lifecycle_high_water_pct),
                reconciliation_queue_high_water = reconciliation_high_water,
                reconciliation_queue_max = reconciliation_max,
                reconciliation_queue_high_water_pct =
                    format!("{:.1}", reconciliation_high_water_pct),
                "[QUEUE-PRESSURE] pipeline queue high-water at or above 50%",
            );
            warn!(
                grafana = true,
                event = "queue_pressure",
                meta = %serde_json::json!({
                    "queue_size": queue_used,
                    "queue_high_water": queue_high_water,
                    "queue_max": queue_max,
                    "queue_pct": format!("{:.1}", queue_pct),
                    "queue_high_water_pct": format!("{:.1}", queue_high_water_pct),
                    "websocket_lifecycle_queue_high_water": websocket_lifecycle_high_water,
                    "websocket_lifecycle_queue_max": websocket_lifecycle_max,
                    "websocket_lifecycle_queue_high_water_pct": format!("{:.1}", websocket_lifecycle_high_water_pct),
                    "reconciliation_queue_high_water": reconciliation_high_water,
                    "reconciliation_queue_max": reconciliation_max,
                    "reconciliation_queue_high_water_pct": format!("{:.1}", reconciliation_high_water_pct),
                }),
                "queue_pressure",
            );
        }
        queue_high_water = 0;
        websocket_lifecycle_high_water = 0;
        reconciliation_high_water = 0;
    }
}
