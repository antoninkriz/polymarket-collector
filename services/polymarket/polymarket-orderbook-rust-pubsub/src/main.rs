//! Polymarket orderbook → durable Redis Stream publisher.
//!
//! Mirrors the streaming pipeline of the sibling `polymarket-orderbook-rust`
//! service, but the terminal sink appends v3 records to a Redis Stream
//! instead of inserting into ClickHouse.
//!
//! ```text
//!   Redis cache / Gamma API ──> initial market set ──┐
//!   Redis lifecycle stream ──> reconciliation ───────┤
//!   WS lifecycle routes ──> deduplicated updates ────┤
//!                                                   v
//!   Polymarket WS pool ── (mpsc) ──> Redis XADD (durable event stream)
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use polymarket_orderbook_rust::events::{Market, MarketLifecycle, MarketLifecycleObservation};
use polymarket_orderbook_rust::markets;
use polymarket_orderbook_rust::markets::stream::StreamConfig;
use polymarket_orderbook_rust::record::EventRecord;
use polymarket_orderbook_rust::ws::pool::Pool;

use polymarket_orderbook_rust_pubsub::config::Config;
use polymarket_orderbook_rust_pubsub::lease::{PublisherLease, PublisherLeaseConfig};
use polymarket_orderbook_rust_pubsub::pubsub_sink::{PubSubSink, PubSubSinkConfig};
use polymarket_orderbook_rust_pubsub::sequence_watermark::clickhouse_generation_floor;

#[derive(Debug, Parser)]
#[command(name = "polymarket-orderbook-rust-pubsub")]
#[command(about = "Polymarket orderbook → durable Redis Stream publisher")]
struct Cli {
    /// Only listen for new markets, skip pre-loading active markets.
    #[arg(long)]
    new_only: bool,

    /// Skip unprocessed Redis stream messages, only process new ones.
    #[arg(long)]
    skip_backlog: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    init_tracing();
    let cli = Cli::parse();
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
        new_only = cli.new_only,
        skip_backlog = cli.skip_backlog,
        redis_url = %cfg.redis_url,
        event_stream = %cfg.redis_event_stream,
        max_assets_per_conn = cfg.max_assets_per_conn,
        queue_size = cfg.queue_size,
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

    let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
    let pool = Arc::new(Mutex::new(Pool::new_with_lifecycle(
        cfg.max_assets_per_conn,
        event_tx.clone(),
        publisher_fence,
        lifecycle_tx,
    )));
    pool.lock().await.start().await;

    let lifecycle_pool = Arc::clone(&pool);
    let lifecycle_handle = tokio::spawn(async move {
        run_websocket_lifecycle(lifecycle_rx, lifecycle_pool).await;
    });

    if !cli.new_only {
        let markets = preload_markets(&cfg).await?;
        if !markets.is_empty() {
            pool.lock().await.subscribe_markets(markets).await?;
            info!(
                markets = pool.lock().await.subscribed_market_count(),
                "pre-loaded markets",
            );
        }
    } else {
        info!("--new-only set, skipping active market pre-load");
    }

    let stream_cfg = StreamConfig {
        redis_url: cfg.redis_url.clone(),
        stream_key: cfg.redis_stream_market_events.clone(),
        group: cfg.stream_consumer_group.clone(),
        consumer: cfg.stream_consumer_name.clone(),
        skip_backlog: cli.skip_backlog,
    };
    let stream_pool = Arc::clone(&pool);
    let stream_handle: JoinHandle<Result<()>> =
        tokio::spawn(async move { markets::stream::run(stream_cfg, stream_pool).await });

    let stats_pool = Arc::clone(&pool);
    let stats_event_tx = event_tx.clone();
    let stats_redis_url = cfg.redis_url.clone();
    let stats_cache_count_key = cfg.redis_key_active_markets_count.clone();
    let stats_handle = tokio::spawn(async move {
        stats_loop(
            stats_pool,
            stats_event_tx,
            stats_redis_url,
            stats_cache_count_key,
        )
        .await;
    });

    let mut sink_outcome = None;
    tokio::select! {
        _ = wait_for_shutdown() => info!("shutdown signal received"),
        lease_error = lease_failure_rx.recv() => {
            warn!(error = lease_error.unwrap_or_else(|| "lease monitor stopped".into()), "publisher lease failed; stopping collector");
        }
        outcome = &mut sink_handle => {
            warn!(?outcome, "Redis event sink stopped; stopping collector");
            sink_outcome = Some(outcome);
        }
    }

    stream_handle.abort();
    let _ = stream_handle.await;
    info!("stream listener stopped");

    lifecycle_handle.abort();
    let _ = lifecycle_handle.await;
    info!("WebSocket lifecycle listener stopped");

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

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .json()
        .init();
}

async fn preload_markets(cfg: &Config) -> Result<Vec<Market>> {
    if cfg.skip_active_markets_cache {
        info!("SKIP_ACTIVE_MARKETS_CACHE=true, fetching from Gamma API");
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("build gamma http client")?;
        let markets = markets::gamma::fetch_active_markets(&http).await?;
        info!(count = markets.len(), "fetched active markets from gamma");
        return Ok(markets);
    }

    let minimum_cache_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs()
        .saturating_sub(1);
    let updated_at_key = format!("{}:updated_at", cfg.redis_key_active_markets);
    let mut waiting_logged = false;
    loop {
        let updated_at = markets::redis_cache::last_updated(&cfg.redis_url, &updated_at_key)
            .await
            .context("load active markets cache timestamp")?;
        if updated_at.is_some_and(|timestamp| timestamp >= minimum_cache_timestamp) {
            if let Some(markets) =
                markets::redis_cache::load(&cfg.redis_url, &cfg.redis_key_active_markets)
                    .await
                    .context("load active markets from redis")?
            {
                return Ok(markets);
            }
        }

        if !waiting_logged {
            warn!(
                key = cfg.redis_key_active_markets,
                ?updated_at,
                minimum_cache_timestamp,
                "fresh active markets cache not ready; waiting"
            );
            waiting_logged = true;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
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

async fn run_websocket_lifecycle(
    mut lifecycle_rx: mpsc::UnboundedReceiver<MarketLifecycleObservation>,
    pool: Arc<Mutex<Pool>>,
) {
    let mut seen = HashSet::<(&'static str, String)>::new();
    while let Some(observation) = lifecycle_rx.recv().await {
        let Some(update) = observation.event.market_lifecycle() else {
            warn!("lifecycle controller received a token-scoped event");
            continue;
        };
        let key = match &update {
            MarketLifecycle::NewMarket { market, .. } => ("new_market", market.clone()),
            MarketLifecycle::MarketResolved { market } => ("market_resolved", market.clone()),
        };
        if !seen.insert(key) {
            continue;
        }

        let mut pool = pool.lock().await;
        pool.admit_lifecycle(observation.event, observation.timestamp_received_ns);
        let result = match update {
            MarketLifecycle::NewMarket {
                market,
                assets_ids,
                outcomes,
            } => {
                let Some(market) =
                    markets::binary_market_from_outcomes(market, &outcomes, &assets_ids)
                else {
                    warn!(
                        asset_count = assets_ids.len(),
                        outcome_count = outcomes.len(),
                        "ignoring non-binary WebSocket new_market"
                    );
                    continue;
                };
                pool.subscribe_markets(vec![market]).await
            }
            MarketLifecycle::MarketResolved { market } => {
                pool.unsubscribe_markets(vec![market]).await
            }
        };
        if let Err(error) = result {
            warn!(%error, "failed to apply WebSocket market lifecycle update");
        }
    }
}

async fn stats_loop(
    pool: Arc<Mutex<Pool>>,
    event_tx: mpsc::Sender<EventRecord>,
    redis_url: String,
    cache_count_key: String,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(60));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;

    let redis_client = redis::Client::open(redis_url.as_str()).ok();
    let mut redis_conn: Option<redis::aio::MultiplexedConnection> = None;

    let mut iteration = 0_u64;
    loop {
        tick.tick().await;
        iteration += 1;

        let stats = {
            let p = pool.lock().await;
            p.pool_stats()
        };

        let queue_max = event_tx.max_capacity();
        let queue_used = queue_max.saturating_sub(event_tx.capacity());
        let queue_pct = if queue_max > 0 {
            (queue_used as f64 / queue_max as f64) * 100.0
        } else {
            0.0
        };

        let cache_active_markets: u64 = 'redis: {
            if redis_conn.is_none() {
                if let Some(client) = &redis_client {
                    match client.get_multiplexed_async_connection().await {
                        Ok(conn) => redis_conn = Some(conn),
                        Err(e) => {
                            warn!(error = %e, "stats_loop: Redis connect failed");
                            break 'redis 0;
                        }
                    }
                } else {
                    break 'redis 0;
                }
            }
            let conn = redis_conn.as_mut().unwrap();
            let r: redis::RedisResult<Option<String>> = redis::cmd("GET")
                .arg(&cache_count_key)
                .query_async(conn)
                .await;
            match r {
                Ok(v) => v.and_then(|s| s.parse().ok()).unwrap_or(0),
                Err(e) => {
                    warn!(error = %e, "stats_loop: Redis GET failed, will reconnect");
                    redis_conn = None;
                    0
                }
            }
        };

        info!(
            iter = iteration,
            queue_size = queue_used,
            queue_max,
            queue_pct = format!("{:.1}", queue_pct),
            subscribed_markets = stats.market_count,
            cache_active_markets,
            connections = stats.connection_count,
            lifecycle_listeners = stats.lifecycle_listener_count,
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
                "cache_active_markets": cache_active_markets,
                "connections": stats.connection_count,
                "lifecycle_listeners": stats.lifecycle_listener_count,
                "queue_size": queue_used,
                "queue_max": queue_max,
                "queue_pct": format!("{:.1}", queue_pct),
                "asset_down_events_total": stats.asset_down_events,
                "conn_down_events_total": stats.conn_down_events,
            }),
            "pool_stats",
        );

        if queue_pct > 50.0 {
            warn!(
                queue_size = queue_used,
                queue_max,
                queue_pct = format!("{:.1}", queue_pct),
                "[QUEUE-PRESSURE] queue above 50%",
            );
            warn!(
                grafana = true,
                event = "queue_pressure",
                meta = %serde_json::json!({
                    "queue_size": queue_used,
                    "queue_max": queue_max,
                    "queue_pct": format!("{:.1}", queue_pct),
                }),
                "queue_pressure",
            );
        }
    }
}
