//! Polymarket orderbook → durable Redis Stream publisher.
//!
//! Mirrors the streaming pipeline of the sibling `polymarket-orderbook-rust`
//! service, but the terminal sink appends v3 records to a Redis Stream
//! instead of inserting into ClickHouse.
//!
//! ```text
//!   Redis cache / Gamma API ──┐
//!                             │
//!                             v
//!   WS pool ── (mpsc) ──> Redis XADD (durable stream)
//!     ^
//!     │
//!   Redis stream listener (market lifecycle)
//! ```

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use polymarket_orderbook_rust::events::Market;
use polymarket_orderbook_rust::markets;
use polymarket_orderbook_rust::markets::stream::StreamConfig;
use polymarket_orderbook_rust::record::EventRecord;
use polymarket_orderbook_rust::ws::pool::Pool;

use polymarket_orderbook_rust_pubsub::config::Config;
use polymarket_orderbook_rust_pubsub::pubsub_sink::{PubSubSink, PubSubSinkConfig};

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
    info!(
        new_only = cli.new_only,
        skip_backlog = cli.skip_backlog,
        redis_url = %cfg.redis_url,
        event_stream = %cfg.redis_event_stream,
        max_assets_per_conn = cfg.max_assets_per_conn,
        queue_size = cfg.queue_size,
        publish_batch_max = cfg.publish_batch_max,
        publish_linger_ms = cfg.publish_linger.as_millis() as u64,
        "starting polymarket-orderbook-rust-pubsub",
    );

    let sink = PubSubSink::connect(PubSubSinkConfig {
        redis_url: cfg.redis_url.clone(),
        stream: cfg.redis_event_stream.clone(),
        batch_max: cfg.publish_batch_max,
        linger: cfg.publish_linger,
    })
    .await
    .context("connect Redis stream sink")?;

    let (event_tx, event_rx) = mpsc::channel::<EventRecord>(cfg.queue_size);
    let sink_handle: JoinHandle<Result<()>> = tokio::spawn(sink.run(event_rx));

    let pool = Arc::new(Mutex::new(Pool::new(
        cfg.max_assets_per_conn,
        event_tx.clone(),
    )));

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
    let stream_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
        markets::stream::run(stream_cfg, stream_pool).await
    });

    let stats_pool = Arc::clone(&pool);
    let stats_event_tx = event_tx.clone();
    let stats_redis_url = cfg.redis_url.clone();
    let stats_cache_count_key = cfg.redis_key_active_markets_count.clone();
    let stats_handle = tokio::spawn(async move {
        stats_loop(stats_pool, stats_event_tx, stats_redis_url, stats_cache_count_key).await;
    });

    wait_for_shutdown().await;
    info!("shutdown signal received");

    stream_handle.abort();
    let _ = stream_handle.await;
    info!("stream listener stopped");

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

    match sink_handle.await {
        Ok(Ok(())) => info!("sink shut down cleanly"),
        Ok(Err(e)) => warn!(error = %e, "sink ended with error"),
        Err(e) => warn!(error = %e, "sink task panicked"),
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

    let markets = markets::redis_cache::load(&cfg.redis_url, &cfg.redis_key_active_markets)
        .await
        .context("load active markets from redis")?;
    Ok(markets)
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
            asset_down_events = stats.asset_down_events,
            asset_degraded_events = stats.asset_degraded_events,
            conn_down_events = stats.conn_down_events,
            "[QUEUE-STATS]",
        );

        info!(
            grafana = true,
            event = "pool_stats",
            conns_down = stats.conns_down,
            assets_down = stats.assets_down,
            assets_degraded = stats.assets_degraded,
            meta = %serde_json::json!({
                "iter": iteration,
                "subscribed_markets": stats.market_count,
                "cache_active_markets": cache_active_markets,
                "connections": stats.connection_count,
                "queue_size": queue_used,
                "queue_max": queue_max,
                "queue_pct": format!("{:.1}", queue_pct),
                "asset_down_events_total": stats.asset_down_events,
                "asset_degraded_events_total": stats.asset_degraded_events,
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
