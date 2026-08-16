//! Polymarket orderbook → ClickHouse writer fed from a durable Redis Stream.
//!
//! Consumes the stream written by `polymarket-collector`,
//! validates each raw v3 record, preserves its JSON text, and acknowledges it
//! only after ClickHouse commits. No market-data decoding or WebSocket work
//! runs here.
//!
//! ```text
//!   Redis Stream ──► writer actor ──► ClickHouse commit ──► Redis acknowledgement
//! ```

use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::watch;
use tracing::{info, warn};

use polymarket_clickhouse_writer::clickhouse::{ClickHouseConfig, ClickHouseSink};
use polymarket_clickhouse_writer::config::Config;
use polymarket_clickhouse_writer::writer::{Writer, WriterConfig};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
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
        "starting polymarket-clickhouse-writer",
    );

    let sink = ClickHouseSink::connect(ClickHouseConfig {
        url: cfg.clickhouse_url.clone(),
        user: cfg.clickhouse_user.clone(),
        password: cfg.clickhouse_password.clone(),
        database: cfg.clickhouse_database.clone(),
        table: cfg.clickhouse_table.clone(),
    })
    .await
    .context("connect Polymarket ClickHouse sink")?;
    let writer_cfg = WriterConfig {
        stream: cfg.redis_event_stream.clone(),
        group: cfg.stream_consumer_group.clone(),
        consumer: cfg.stream_consumer_name.clone(),
        reconnect_delay: cfg.stream_reconnect_delay,
        batch_size: cfg.flush_batch_size,
        flush_interval: cfg.flush_interval,
    };
    let writer = Writer::new(&cfg.redis_url, writer_cfg, sink).context("build stream writer")?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let writer_future = writer.run(shutdown_rx);
    tokio::pin!(writer_future);

    tokio::select! {
        result = &mut writer_future => {
            match result {
                Ok(()) => anyhow::bail!("Redis to ClickHouse writer exited unexpectedly"),
                Err(error) => return Err(error).context("Redis to ClickHouse writer failed"),
            }
        }
        _ = wait_for_shutdown() => {}
    }
    info!("shutdown signal received");

    let _ = shutdown_tx.send(true);
    match tokio::time::timeout(Duration::from_secs(10), &mut writer_future).await {
        Ok(Ok(())) => info!("Redis to ClickHouse writer shut down cleanly"),
        Ok(Err(error)) => {
            return Err(error).context("Redis to ClickHouse writer shutdown failed");
        }
        Err(_) => {
            warn!("Redis to ClickHouse writer did not drain before shutdown timeout");
        }
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

async fn wait_for_shutdown() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("received SIGINT"),
        _ = sigterm.recv() => info!("received SIGTERM"),
    }
}
