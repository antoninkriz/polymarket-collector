//! Polymarket market-data collector with durable Redis Stream output.
//!
//! Collects the full market stream and appends v3 records to durable Redis.
//!
//! ```text
//!   Redis restart cache + Gamma reconciliation ──────┐
//!   WS lifecycle routes ──> deduplicated updates ────┤
//!                                                    v
//!   Polymarket WS pool ── (mpsc) ──> Redis XADD (durable event stream)
//! ```

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use polymarket_collector::config::Config;
use polymarket_collector::events::{Market, MarketLifecycleObservation};
use polymarket_collector::gamma_reconcile::{self, ReconciliationProgress};
use polymarket_collector::lease::{PublisherLease, PublisherLeaseConfig};
use polymarket_collector::market_lifecycle::LifecycleCoordinator;
use polymarket_collector::markets;
use polymarket_collector::markets::lifecycle::LifecycleRequest;
use polymarket_collector::record::EventRecord;
use polymarket_collector::redis_stream::{RedisStreamPublisher, RedisStreamPublisherConfig};
use polymarket_collector::sequence_watermark::clickhouse_generation_floor;
use polymarket_collector::ws::pool::{Pool, PoolStats};

const CACHE_BOOTSTRAP_BATCH_SIZE: usize = 100;
// A valid restart cache is much faster than a full Gamma scan, but opening the
// entire 1,000+ socket universe at once leaves some upstream sessions silent.
// Recent markets remain first while the long tail is admitted at a steady rate.
const CACHE_BOOTSTRAP_BATCH_INTERVAL: Duration = Duration::from_millis(100);

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<()> {
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
        "starting polymarket-collector",
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

    let sink = RedisStreamPublisher::connect(RedisStreamPublisherConfig {
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
    let (coordinator_shutdown_tx, coordinator_shutdown_rx) = oneshot::channel();
    let mut pool = Pool::new_with_lifecycle(
        cfg.max_assets_per_conn,
        event_tx.clone(),
        publisher_fence,
        websocket_lifecycle_tx.clone(),
    );
    pool.start().await;

    let coordinator = LifecycleCoordinator::new(
        pool,
        websocket_lifecycle_rx,
        reconciliation_rx,
        coordinator_shutdown_rx,
    );
    let mut coordinator_handle = tokio::spawn(coordinator.run());

    let gamma_http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("build Gamma HTTP client")?;
    let gamma_client = markets::gamma::GammaClient::new(gamma_http);

    let startup_poll_time = Utc::now();
    let (mut cached_markets, cache_fetched_at) = load_restart_cache(&cfg).await;
    let cold_start = cached_markets.is_empty();
    if !cached_markets.is_empty() {
        match gamma_reconcile::prepare_restart_markets(&gamma_client, &mut cached_markets).await {
            Ok(plan) => {
                let missing = plan.missing.len();
                gamma_reconcile::admit_restart_markets(&reconciliation_tx, plan.missing)
                    .await
                    .context("subscribe recent markets missing from restart cache")?;
                info!(
                    prioritized = plan.prioritized,
                    missing, "reconciled recent restart-cache markets"
                );
            }
            Err(error) => warn!(%error, "could not reconcile recent restart-cache markets"),
        }
        let market_count = cached_markets.len();
        apply_bootstrap_batched(&reconciliation_tx, cached_markets).await?;
        info!(markets = market_count, "pre-loaded markets");
    }

    // Preserve the existing cache and its honest fetched_at until this process
    // has completed active, new-market, and closed-market reconciliation.
    let (progress_tx, progress_rx) = watch::channel(ReconciliationProgress::default());
    let (gamma_shutdown_tx, gamma_shutdown_rx) = watch::channel(false);
    let mut full_scan_handle: JoinHandle<Result<()>> =
        tokio::spawn(gamma_reconcile::run_full_scans(
            gamma_client.clone(),
            reconciliation_tx.clone(),
            cold_start,
            progress_tx.clone(),
            gamma_shutdown_rx,
        ));
    let mut new_poll_handle = tokio::spawn(gamma_reconcile::run_new_market_polls(
        gamma_client.clone(),
        reconciliation_tx.clone(),
        cache_fetched_at.unwrap_or(startup_poll_time),
        progress_tx.clone(),
    ));
    let mut closed_poll_handle = tokio::spawn(gamma_reconcile::run_closed_market_polls(
        gamma_client,
        reconciliation_tx.clone(),
        cache_fetched_at.unwrap_or(startup_poll_time),
        progress_tx.clone(),
    ));
    let mut cache_saver_handle = tokio::spawn(gamma_reconcile::run_cache_saver(
        reconciliation_tx.clone(),
        cfg.redis_url.clone(),
        cfg.redis_key_active_markets.clone(),
        progress_tx,
        progress_rx.clone(),
    ));

    let stats_event_tx = event_tx.clone();
    let stats_websocket_lifecycle_tx = websocket_lifecycle_tx.clone();
    let stats_reconciliation_tx = reconciliation_tx.clone();
    let stats_progress = progress_rx.clone();
    let stats_handle = tokio::spawn(async move {
        stats_loop(
            stats_event_tx,
            stats_websocket_lifecycle_tx,
            stats_reconciliation_tx,
            stats_progress,
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

    let safe_fetched_at = progress_rx.borrow().safe_fetched_at();
    if !coordinator_stopped {
        if let Some(safe_fetched_at) = safe_fetched_at {
            if let Err(error) = gamma_reconcile::persist_final_snapshot(
                &reconciliation_tx,
                &cfg.redis_url,
                &cfg.redis_key_active_markets,
                safe_fetched_at,
            )
            .await
            {
                warn!(%error, "final market restart-cache save failed");
            }
        } else {
            info!("skipping incomplete market restart-cache save");
        }
    }

    // Stop telemetry requests before asking the coordinator to close its
    // request receivers. The coordinator keeps the WebSocket lifecycle
    // receiver alive until it has cancelled every connection producer.
    stats_handle.abort();
    let _ = stats_handle.await;
    drop(websocket_lifecycle_tx);

    if !coordinator_stopped {
        if let Err(error) = request_coordinator_shutdown(coordinator_shutdown_tx).await {
            warn!(%error, "market lifecycle coordinator shutdown request failed");
        }
        match coordinator_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(%error, "market lifecycle coordinator ended with error"),
            Err(error) => warn!(%error, "market lifecycle coordinator task failed"),
        }
    }
    info!("market lifecycle coordinator and pool stopped");

    drop(event_tx);

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

async fn request_pool_stats(lifecycle_tx: &mpsc::Sender<LifecycleRequest>) -> Result<PoolStats> {
    let (completion, completed) = oneshot::channel();
    lifecycle_tx
        .send(LifecycleRequest::PoolStats { completion })
        .await
        .context("lifecycle coordinator stopped before pool stats request")?;
    completed
        .await
        .context("lifecycle coordinator dropped pool stats response")
}

async fn request_coordinator_shutdown(
    shutdown_tx: oneshot::Sender<polymarket_collector::markets::lifecycle::LifecycleCompletion>,
) -> Result<()> {
    let (completion, completed) = oneshot::channel();
    shutdown_tx
        .send(completion)
        .map_err(|_| anyhow::anyhow!("lifecycle coordinator stopped before shutdown request"))?;
    completed
        .await
        .context("lifecycle coordinator dropped shutdown completion")?
        .map_err(anyhow::Error::msg)
}

async fn apply_bootstrap_batched(
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
    markets: Vec<Market>,
) -> Result<()> {
    let mut markets = markets.into_iter().peekable();
    while markets.peek().is_some() {
        let batch = markets.by_ref().take(CACHE_BOOTSTRAP_BATCH_SIZE).collect();
        apply_bootstrap(lifecycle_tx, batch).await?;
        if markets.peek().is_some() {
            tokio::time::sleep(CACHE_BOOTSTRAP_BATCH_INTERVAL).await;
        }
    }
    Ok(())
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
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received SIGINT"),
        _ = sigterm.recv() => info!("received SIGTERM"),
    }
}

async fn stats_loop(
    event_tx: mpsc::Sender<EventRecord>,
    websocket_lifecycle_tx: mpsc::Sender<MarketLifecycleObservation>,
    reconciliation_tx: mpsc::Sender<LifecycleRequest>,
    reconciliation_progress: watch::Receiver<ReconciliationProgress>,
) {
    const SAMPLES_PER_REPORT: u64 = 60;
    const PRESSURE_THRESHOLD_PCT: f64 = 50.0;

    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;

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
        let stats = match request_pool_stats(&reconciliation_tx).await {
            Ok(stats) => stats,
            Err(error) => {
                warn!(%error, "stopping stats loop after lifecycle coordinator stopped");
                return;
            }
        };
        let reconciliation_ages = reconciliation_progress.borrow().ages();
        let asset_recoveries = stats.asset_recoveries;
        let asset_recovery_latency_us = stats.asset_recovery_latency_us;
        let asset_recovery_avg_ms = if asset_recoveries > 0 {
            asset_recovery_latency_us as f64 / asset_recoveries as f64 / 1_000.0
        } else {
            0.0
        };
        let asset_recovery_max_ms = stats.asset_recovery_latency_us_max as f64 / 1_000.0;

        let queue_pct = (queue_used as f64 / queue_max as f64) * 100.0;
        let queue_high_water_pct = (queue_high_water as f64 / queue_max as f64) * 100.0;
        let websocket_lifecycle_high_water_pct =
            (websocket_lifecycle_high_water as f64 / websocket_lifecycle_max as f64) * 100.0;
        let reconciliation_high_water_pct =
            (reconciliation_high_water as f64 / reconciliation_max as f64) * 100.0;

        info!(
            queue_size = queue_used,
            queue_high_water,
            queue_max,
            queue_pct,
            queue_high_water_pct,
            websocket_lifecycle_queue_size = websocket_lifecycle_used,
            websocket_lifecycle_queue_high_water = websocket_lifecycle_high_water,
            websocket_lifecycle_queue_max = websocket_lifecycle_max,
            websocket_lifecycle_queue_high_water_pct = websocket_lifecycle_high_water_pct,
            reconciliation_queue_size = reconciliation_used,
            reconciliation_queue_high_water = reconciliation_high_water,
            reconciliation_queue_max = reconciliation_max,
            reconciliation_queue_high_water_pct = reconciliation_high_water_pct,
            subscribed_markets = stats.market_count,
            connections = stats.connection_count,
            lifecycle_listeners = stats.lifecycle_listener_count,
            conns_down = stats.conns_down,
            assets_down = stats.assets_down,
            gamma_full_scan_age_s = ?reconciliation_ages.full_scan_seconds,
            gamma_new_poll_age_s = ?reconciliation_ages.new_poll_seconds,
            gamma_closed_poll_age_s = ?reconciliation_ages.closed_poll_seconds,
            restart_cache_save_age_s = ?reconciliation_ages.cache_save_seconds,
            asset_down_events = stats.asset_down_events,
            asset_recovery_events = stats.asset_recovery_events,
            asset_recoveries,
            asset_recovery_avg_ms,
            asset_recovery_max_ms,
            conn_down_events = stats.conn_down_events,
            "[QUEUE-STATS]",
        );

        if queue_high_water_pct >= PRESSURE_THRESHOLD_PCT
            || websocket_lifecycle_high_water_pct >= PRESSURE_THRESHOLD_PCT
            || reconciliation_high_water_pct >= PRESSURE_THRESHOLD_PCT
        {
            warn!(
                queue_size = queue_used,
                queue_high_water,
                queue_max,
                queue_pct,
                queue_high_water_pct,
                websocket_lifecycle_queue_high_water = websocket_lifecycle_high_water,
                websocket_lifecycle_queue_max = websocket_lifecycle_max,
                websocket_lifecycle_queue_high_water_pct = websocket_lifecycle_high_water_pct,
                reconciliation_queue_high_water = reconciliation_high_water,
                reconciliation_queue_max = reconciliation_max,
                reconciliation_queue_high_water_pct = reconciliation_high_water_pct,
                "[QUEUE-PRESSURE] pipeline queue high-water at or above 50%",
            );
        }
        queue_high_water = 0;
        websocket_lifecycle_high_water = 0;
        reconciliation_high_water = 0;
    }
}
