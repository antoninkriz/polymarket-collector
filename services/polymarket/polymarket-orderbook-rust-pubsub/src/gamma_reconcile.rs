//! Background Gamma reconciliation and Rust-owned restart-cache persistence.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, TimeDelta, Utc};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{info, warn};

use polymarket_orderbook_rust::events::{Event, MarketLifecycleObservation};
use polymarket_orderbook_rust::markets::gamma::{GammaClient, GammaMarket};
use polymarket_orderbook_rust::markets::lifecycle::{
    ActiveMarketSnapshot, LifecycleRequest, LifecycleSource, ScannedActiveMarket,
};
use polymarket_orderbook_rust::markets::redis_cache::{self, CacheDocument};

const FULL_SCAN_INTERVAL: Duration = Duration::from_secs(30 * 60);
const FULL_SCAN_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const NEW_MARKET_INTERVAL: Duration = Duration::from_secs(10);
const CLOSED_MARKET_INTERVAL: Duration = Duration::from_secs(30);
const CACHE_INTERVAL: Duration = Duration::from_secs(60);
const POLL_OVERLAP: TimeDelta = TimeDelta::minutes(2);

#[derive(Debug, Clone, Copy)]
pub enum CacheSaveTrigger {
    Force,
}

#[derive(Default)]
pub struct ReconciliationStats {
    full_scan_success_ms: AtomicI64,
    new_poll_success_ms: AtomicI64,
    closed_poll_success_ms: AtomicI64,
    cache_save_success_ms: AtomicI64,
}

#[derive(Debug, Clone, Copy)]
pub struct ReconciliationAges {
    pub full_scan_seconds: Option<u64>,
    pub new_poll_seconds: Option<u64>,
    pub closed_poll_seconds: Option<u64>,
    pub cache_save_seconds: Option<u64>,
}

impl ReconciliationStats {
    pub fn ages(&self) -> ReconciliationAges {
        let now = Utc::now().timestamp_millis();
        ReconciliationAges {
            full_scan_seconds: age_seconds(now, self.full_scan_success_ms.load(Ordering::Relaxed)),
            new_poll_seconds: age_seconds(now, self.new_poll_success_ms.load(Ordering::Relaxed)),
            closed_poll_seconds: age_seconds(
                now,
                self.closed_poll_success_ms.load(Ordering::Relaxed),
            ),
            cache_save_seconds: age_seconds(
                now,
                self.cache_save_success_ms.load(Ordering::Relaxed),
            ),
        }
    }
}

pub async fn run_full_scans(
    client: GammaClient,
    lifecycle_tx: mpsc::Sender<LifecycleRequest>,
    cold_start: bool,
    cache_trigger: mpsc::Sender<CacheSaveTrigger>,
    stats: Arc<ReconciliationStats>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut scan_id = 0_u64;
    let mut first = true;
    loop {
        scan_id = scan_id.saturating_add(1);
        let cold = first && cold_start;
        let succeeded = tokio::select! {
            result = run_full_scan(&client, &lifecycle_tx, scan_id, cold) => {
                if let Err(error) = result {
                    warn!(scan_id, %error, "full Gamma reconciliation failed");
                    let _ = abort_scan(&lifecycle_tx, scan_id).await;
                    false
                } else {
                    stats
                        .full_scan_success_ms
                        .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
                    let _ = cache_trigger.try_send(CacheSaveTrigger::Force);
                    true
                }
            }
            changed = shutdown.changed() => {
                let _ = abort_scan(&lifecycle_tx, scan_id).await;
                changed.context("Gamma shutdown sender dropped")?;
                return Ok(());
            }
        };
        if succeeded {
            first = false;
        }
        let delay = if succeeded {
            FULL_SCAN_INTERVAL
        } else {
            FULL_SCAN_RETRY_INTERVAL
        };
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            changed = shutdown.changed() => {
                changed.context("Gamma shutdown sender dropped")?;
                return Ok(());
            }
        }
    }
}

async fn run_full_scan(
    client: &GammaClient,
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
    scan_id: u64,
    cold_start: bool,
) -> Result<()> {
    send_confirmed(lifecycle_tx, |completion| LifecycleRequest::ScanStart {
        scan_id,
        cold_start,
        completion,
    })
    .await?;
    let mut scan = client.full_active_scan();
    let mut pages = 0_usize;
    while let Some(markets) = client.next_keyset_page(&mut scan).await? {
        pages += 1;
        let markets = markets.into_iter().filter_map(scanned_active).collect();
        send_confirmed(lifecycle_tx, |completion| LifecycleRequest::ScanPage {
            scan_id,
            markets,
            completion,
        })
        .await?;
    }
    send_confirmed(lifecycle_tx, |completion| LifecycleRequest::ScanFinish {
        scan_id,
        completion,
    })
    .await?;
    info!(scan_id, pages, cold_start, "full Gamma scan completed");
    Ok(())
}

async fn abort_scan(lifecycle_tx: &mpsc::Sender<LifecycleRequest>, scan_id: u64) -> Result<()> {
    send_confirmed(lifecycle_tx, |completion| LifecycleRequest::ScanAbort {
        scan_id,
        completion,
    })
    .await
}

pub async fn run_new_market_polls(
    client: GammaClient,
    lifecycle_tx: mpsc::Sender<LifecycleRequest>,
    initial_since: DateTime<Utc>,
    stats: Arc<ReconciliationStats>,
) -> Result<()> {
    let mut watermark = initial_since.min(Utc::now());
    let mut first_poll = true;
    loop {
        let cutoff = new_poll_cutoff(watermark, first_poll);
        match stream_scan_observations(
            &client,
            client.active_since_scan(cutoff),
            &lifecycle_tx,
            new_market_observation,
        )
        .await
        {
            Ok(()) => {
                watermark = Utc::now();
                first_poll = false;
                stats
                    .new_poll_success_ms
                    .store(watermark.timestamp_millis(), Ordering::Relaxed);
            }
            Err(error) => warn!(%error, "incremental Gamma new-market poll failed"),
        }
        tokio::time::sleep(NEW_MARKET_INTERVAL).await;
    }
}

pub async fn run_closed_market_polls(
    client: GammaClient,
    lifecycle_tx: mpsc::Sender<LifecycleRequest>,
    initial_since: DateTime<Utc>,
    stats: Arc<ReconciliationStats>,
) -> Result<()> {
    let mut watermark = initial_since.min(Utc::now());
    loop {
        let cutoff = (watermark - POLL_OVERLAP).timestamp_millis();
        match stream_scan_observations(
            &client,
            client.closed_since_scan(cutoff),
            &lifecycle_tx,
            resolved_observation,
        )
        .await
        {
            Ok(()) => {
                watermark = Utc::now();
                stats
                    .closed_poll_success_ms
                    .store(watermark.timestamp_millis(), Ordering::Relaxed);
            }
            Err(error) => warn!(%error, "Gamma closed-market poll failed"),
        }
        tokio::time::sleep(CLOSED_MARKET_INTERVAL).await;
    }
}

async fn stream_scan_observations(
    client: &GammaClient,
    mut scan: polymarket_orderbook_rust::markets::gamma::KeysetScan,
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
    to_observation: fn(&GammaMarket) -> Option<MarketLifecycleObservation>,
) -> Result<()> {
    while let Some(page) = client.next_keyset_page(&mut scan).await? {
        for market in page {
            if let Some(observation) = to_observation(&market) {
                send_observation(lifecycle_tx, observation).await?;
            }
        }
    }
    Ok(())
}

pub async fn run_cache_saver(
    lifecycle_tx: mpsc::Sender<LifecycleRequest>,
    redis_url: String,
    key: String,
    mut trigger_rx: mpsc::Receiver<CacheSaveTrigger>,
    stats: Arc<ReconciliationStats>,
) -> Result<()> {
    let mut interval = tokio::time::interval(CACHE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut saved_revision = None;
    loop {
        let mut force = false;
        tokio::select! {
            _ = interval.tick() => {}
            trigger = trigger_rx.recv() => {
                match trigger {
                    Some(CacheSaveTrigger::Force) => force = true,
                    None => {
                        // Periodic persistence remains useful when the optional
                        // full-scan producer is disabled by --new-only.
                        interval.tick().await;
                    }
                }
            }
        }
        match persist_snapshot(&lifecycle_tx, &redis_url, &key, &mut saved_revision, force).await {
            Ok(true) => {
                stats
                    .cache_save_success_ms
                    .store(Utc::now().timestamp_millis(), Ordering::Relaxed);
            }
            Ok(false) => {}
            Err(error) => warn!(%error, "save Rust market restart cache failed"),
        }
    }
}

pub async fn persist_final_snapshot(
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
    redis_url: &str,
    key: &str,
) -> Result<()> {
    persist_snapshot(lifecycle_tx, redis_url, key, &mut None, true)
        .await
        .map(|_| ())
}

async fn persist_snapshot(
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
    redis_url: &str,
    key: &str,
    saved_revision: &mut Option<u64>,
    force: bool,
) -> Result<bool> {
    let snapshot = request_snapshot(lifecycle_tx).await?;
    if !force && *saved_revision == Some(snapshot.revision) {
        return Ok(false);
    }
    let revision = snapshot.revision;
    let raw = tokio::task::spawn_blocking(move || {
        CacheDocument::new(Utc::now(), snapshot.markets)?.to_json()
    })
    .await
    .context("cache serialization task failed")??;
    redis_cache::save_json(redis_url, key, raw).await?;
    *saved_revision = Some(revision);
    Ok(true)
}

async fn request_snapshot(
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
) -> Result<ActiveMarketSnapshot> {
    let (completion, completed) = oneshot::channel();
    lifecycle_tx
        .send(LifecycleRequest::Snapshot { completion })
        .await
        .context("lifecycle coordinator stopped before snapshot")?;
    completed
        .await
        .context("lifecycle coordinator dropped snapshot")
}

async fn send_observation(
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
    observation: MarketLifecycleObservation,
) -> Result<()> {
    send_confirmed(lifecycle_tx, |completion| LifecycleRequest::Observation {
        source: LifecycleSource::Gamma,
        observation,
        completion,
    })
    .await
}

async fn send_confirmed<F>(lifecycle_tx: &mpsc::Sender<LifecycleRequest>, request: F) -> Result<()>
where
    F: FnOnce(oneshot::Sender<Result<(), String>>) -> LifecycleRequest,
{
    let (completion, completed) = oneshot::channel();
    lifecycle_tx
        .send(request(completion))
        .await
        .context("lifecycle coordinator channel closed")?;
    completed
        .await
        .context("lifecycle coordinator dropped completion")?
        .map_err(anyhow::Error::msg)
}

fn scanned_active(market: GammaMarket) -> Option<ScannedActiveMarket> {
    let subscription = market.active_subscription()?;
    Some(ScannedActiveMarket {
        market: subscription,
        observation: new_market_observation(&market),
    })
}

fn new_market_observation(market: &GammaMarket) -> Option<MarketLifecycleObservation> {
    let timestamp = market.new_market_timestamp_ms()?;
    Some(MarketLifecycleObservation {
        event: Event::NewMarket {
            id: market.id.clone(),
            market: market.condition_id.clone(),
            timestamp: timestamp.to_string(),
            assets_ids: market.assets_ids.clone(),
            outcomes: market.outcomes.clone(),
            question: (!market.question.is_empty()).then(|| market.question.clone()),
            slug: (!market.slug.is_empty()).then(|| market.slug.clone()),
        },
        timestamp_received_ns: market.received_at_ns,
    })
}

fn resolved_observation(market: &GammaMarket) -> Option<MarketLifecycleObservation> {
    if !market.is_resolved() {
        return None;
    }
    let timestamp = market.closed_time_ms?;
    let winner = market.winner();
    Some(MarketLifecycleObservation {
        event: Event::MarketResolved {
            id: market.id.clone(),
            market: market.condition_id.clone(),
            timestamp: timestamp.to_string(),
            assets_ids: market.assets_ids.clone(),
            winning_asset_id: winner.map(|winner| winner.asset_id.to_string()),
            winning_outcome: winner.map(|winner| winner.outcome.to_string()),
        },
        timestamp_received_ns: market.received_at_ns,
    })
}

fn age_seconds(now_ms: i64, then_ms: i64) -> Option<u64> {
    (then_ms > 0).then(|| now_ms.saturating_sub(then_ms).max(0) as u64 / 1_000)
}

fn new_poll_cutoff(watermark: DateTime<Utc>, first_poll: bool) -> i64 {
    if first_poll {
        watermark.timestamp_millis()
    } else {
        (watermark - POLL_OVERLAP).timestamp_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gamma_market() -> GammaMarket {
        GammaMarket {
            id: "7".into(),
            condition_id: "m".into(),
            question: String::new(),
            slug: String::new(),
            active: true,
            closed: false,
            uma_resolution_status: String::new(),
            assets_ids: vec!["a".into(), "b".into()],
            outcomes: vec!["Yes".into(), "No".into()],
            outcome_prices: Vec::new(),
            created_at_ms: Some(100),
            start_date_ms: Some(200),
            closed_time_ms: None,
            received_at_ns: 300,
        }
    }

    #[test]
    fn first_new_poll_has_no_backward_overlap() {
        let watermark: DateTime<Utc> = "2026-08-14T12:00:00Z".parse().unwrap();
        assert_eq!(
            new_poll_cutoff(watermark, true),
            watermark.timestamp_millis()
        );
        assert_eq!(
            new_poll_cutoff(watermark, false),
            (watermark - POLL_OVERLAP).timestamp_millis()
        );
    }

    #[test]
    fn missing_source_timestamp_suppresses_synthetic_new_market() {
        let mut market = gamma_market();
        market.created_at_ms = None;
        market.start_date_ms = None;
        assert!(new_market_observation(&market).is_none());
        assert!(scanned_active(market).unwrap().observation.is_none());
    }

    #[test]
    fn empty_optional_text_is_null_and_receive_time_is_preserved() {
        let observation = new_market_observation(&gamma_market()).unwrap();
        let Event::NewMarket { question, slug, .. } = observation.event else {
            panic!("expected new_market")
        };
        assert_eq!(question, None);
        assert_eq!(slug, None);
        assert_eq!(observation.timestamp_received_ns, 300);
    }

    #[test]
    fn pending_or_timestamp_less_close_is_not_synthetic_resolution() {
        let mut market = gamma_market();
        market.closed = true;
        market.closed_time_ms = Some(400);
        market.uma_resolution_status = "pending".into();
        assert!(resolved_observation(&market).is_none());
        market.uma_resolution_status = "RESOLVED".into();
        market.closed_time_ms = None;
        assert!(resolved_observation(&market).is_none());
    }
}
