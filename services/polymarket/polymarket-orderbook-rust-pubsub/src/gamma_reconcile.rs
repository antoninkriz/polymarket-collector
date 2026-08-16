//! Background Gamma reconciliation and Rust-owned restart-cache persistence.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use chrono::{DateTime, TimeDelta, Utc};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{info, warn};

use polymarket_orderbook_rust::events::{Event, Market, MarketLifecycleObservation};
use polymarket_orderbook_rust::markets::gamma::{GammaClient, GammaMarket};
use polymarket_orderbook_rust::markets::lifecycle::{
    ActiveMarketSnapshot, LifecycleRequest, LifecycleSource,
};
use polymarket_orderbook_rust::markets::redis_cache::{self, CacheDocument};

const FULL_SCAN_INTERVAL: Duration = Duration::from_secs(30 * 60);
const FULL_SCAN_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const NEW_MARKET_INTERVAL: Duration = Duration::from_secs(10);
const CLOSED_MARKET_INTERVAL: Duration = Duration::from_secs(30);
const CACHE_INTERVAL: Duration = Duration::from_secs(60);
const POLL_OVERLAP: TimeDelta = TimeDelta::minutes(2);
const RESTART_PRIORITY_PAGES: usize = 20;
const GAMMA_PAGE_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconciliationProgress {
    full_scan_completed_at: Option<DateTime<Utc>>,
    new_markets_through: Option<DateTime<Utc>>,
    resolutions_through: Option<DateTime<Utc>>,
    cache_saved_at: Option<DateTime<Utc>>,
}

impl ReconciliationProgress {
    pub fn safe_fetched_at(self) -> Option<DateTime<Utc>> {
        self.full_scan_completed_at?;
        Some(self.new_markets_through?.min(self.resolutions_through?) - POLL_OVERLAP)
    }

    pub fn ages(&self) -> ReconciliationAges {
        self.ages_at(Utc::now())
    }

    fn ages_at(self, now: DateTime<Utc>) -> ReconciliationAges {
        ReconciliationAges {
            full_scan_seconds: age_seconds(now, self.full_scan_completed_at),
            new_poll_seconds: age_seconds(now, self.new_markets_through),
            closed_poll_seconds: age_seconds(now, self.resolutions_through),
            cache_save_seconds: age_seconds(now, self.cache_saved_at),
        }
    }

    fn record_full_scan(&mut self, completed_at: DateTime<Utc>) {
        advance(&mut self.full_scan_completed_at, completed_at);
    }

    fn record_new_markets(&mut self, through: DateTime<Utc>) {
        advance(&mut self.new_markets_through, through);
    }

    fn record_resolutions(&mut self, through: DateTime<Utc>) {
        advance(&mut self.resolutions_through, through);
    }

    fn record_cache_save(&mut self, saved_at: DateTime<Utc>) {
        advance(&mut self.cache_saved_at, saved_at);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ReconciliationAges {
    pub full_scan_seconds: Option<u64>,
    pub new_poll_seconds: Option<u64>,
    pub closed_poll_seconds: Option<u64>,
    pub cache_save_seconds: Option<u64>,
}

pub struct RestartMarketPlan {
    pub prioritized: usize,
    pub missing: Vec<GammaMarket>,
}

/// Prioritize cached markets and recover recent active markets missing from it.
///
/// The scan is deliberately bounded so a valid cache begins subscribing within
/// a few seconds. A later full scan remains authoritative for the complete
/// active universe.
pub async fn prepare_restart_markets(
    client: &GammaClient,
    markets: &mut [Market],
) -> Result<RestartMarketPlan> {
    let mut scan = client.full_active_scan();
    let mut recent = Vec::new();
    for _ in 0..RESTART_PRIORITY_PAGES {
        let Some(page) = client.next_keyset_page(&mut scan).await? else {
            break;
        };
        recent.extend(page);
    }
    build_restart_market_plan(markets, recent)
}

fn build_restart_market_plan(
    markets: &mut [Market],
    recent: Vec<GammaMarket>,
) -> Result<RestartMarketPlan> {
    let mut cached_conditions = HashMap::with_capacity(markets.len());
    let mut cached_asset_owners = HashMap::with_capacity(markets.len().saturating_mul(2));
    for market in markets.iter() {
        cached_conditions.insert(market.hash.as_str(), canonical_asset_refs(&market.assets));
        for asset in &market.assets {
            cached_asset_owners.insert(asset.as_str(), market.hash.as_str());
        }
    }
    let mut missing_conditions = HashMap::<String, [String; 2]>::new();
    let mut missing_asset_owners = HashMap::<String, String>::new();
    let mut priorities = Vec::with_capacity(recent.len());
    let mut missing = Vec::new();
    for market in recent {
        ensure!(
            market.is_active_binary(),
            "recent Gamma scan returned an invalid active binary market"
        );
        priorities.push(market.condition_id.clone());
        let condition = market.condition_id.as_str();
        let assets = canonical_asset_refs(&market.assets_ids);
        if let Some(cached_assets) = cached_conditions.get(condition) {
            ensure!(
                cached_assets == &assets,
                "recent Gamma market {condition} conflicts with cached asset pair"
            );
            continue;
        }
        if let Some(existing) = missing_conditions.get(condition) {
            ensure!(
                canonical_asset_refs(existing) == assets,
                "recent Gamma market {condition} has conflicting asset pairs"
            );
            continue;
        }
        for asset in assets {
            if let Some(owner) = cached_asset_owners.get(asset) {
                anyhow::bail!(
                    "recent Gamma market {condition} reuses cached asset {asset} owned by {owner}"
                );
            }
            if let Some(owner) = missing_asset_owners.get(asset) {
                anyhow::bail!(
                    "recent Gamma market {condition} reuses asset {asset} owned by {owner}"
                );
            }
            missing_asset_owners.insert(asset.to_string(), condition.to_string());
        }
        missing_conditions.insert(
            condition.to_string(),
            [market.assets_ids[0].clone(), market.assets_ids[1].clone()],
        );
        missing.push(market);
    }
    drop(cached_conditions);
    drop(cached_asset_owners);
    Ok(RestartMarketPlan {
        prioritized: reorder_cached_markets(markets, &priorities),
        missing,
    })
}

fn canonical_asset_refs(assets: &[String]) -> [&str; 2] {
    if assets[0] <= assets[1] {
        [&assets[0], &assets[1]]
    } else {
        [&assets[1], &assets[0]]
    }
}

pub async fn admit_restart_markets(
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
    markets: Vec<GammaMarket>,
) -> Result<()> {
    let mut markets = markets.into_iter().peekable();
    while markets.peek().is_some() {
        let page = markets.by_ref().take(GAMMA_PAGE_SIZE).collect();
        send_confirmed(lifecycle_tx, |completion| LifecycleRequest::GammaPage {
            cold_start: false,
            markets: page,
            completion,
        })
        .await?;
    }
    Ok(())
}

fn reorder_cached_markets(markets: &mut [Market], priorities: &[String]) -> usize {
    let mut ranks = HashMap::with_capacity(priorities.len());
    for (rank, condition) in priorities.iter().enumerate() {
        ranks.entry(condition.as_str()).or_insert(rank);
    }
    let prioritized = markets
        .iter()
        .filter(|market| ranks.contains_key(market.hash.as_str()))
        .count();
    markets.sort_unstable_by(|left, right| {
        let left_rank = ranks.get(left.hash.as_str()).copied();
        let right_rank = ranks.get(right.hash.as_str()).copied();
        left_rank
            .is_none()
            .cmp(&right_rank.is_none())
            .then_with(|| left_rank.cmp(&right_rank))
            .then_with(|| left.hash.cmp(&right.hash))
    });
    prioritized
}

pub async fn run_full_scans(
    client: GammaClient,
    lifecycle_tx: mpsc::Sender<LifecycleRequest>,
    cold_start: bool,
    progress: watch::Sender<ReconciliationProgress>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut first = true;
    loop {
        let cold = first && cold_start;
        let succeeded = tokio::select! {
            result = run_full_scan(&client, &lifecycle_tx, cold) => {
                if let Err(error) = result {
                    warn!(%error, "full Gamma reconciliation failed");
                    false
                } else {
                    progress.send_modify(|progress| progress.record_full_scan(Utc::now()));
                    true
                }
            }
            changed = shutdown.changed() => {
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
    cold_start: bool,
) -> Result<()> {
    let mut scan = client.full_active_scan();
    let mut pages = 0_usize;
    let mut total_markets = 0_usize;
    while let Some(markets) = client.next_keyset_page(&mut scan).await? {
        pages += 1;
        total_markets += markets.len();
        send_confirmed(lifecycle_tx, |completion| LifecycleRequest::GammaPage {
            cold_start,
            markets,
            completion,
        })
        .await?;
        if pages.is_multiple_of(100) {
            info!(pages, total_markets, "full Gamma scan progress");
        }
    }
    ensure!(
        total_markets > 0,
        "full Gamma scan returned no active binary markets"
    );
    info!(
        pages,
        total_markets, cold_start, "full Gamma scan completed"
    );
    Ok(())
}

pub async fn run_new_market_polls(
    client: GammaClient,
    lifecycle_tx: mpsc::Sender<LifecycleRequest>,
    initial_since: DateTime<Utc>,
    progress: watch::Sender<ReconciliationProgress>,
) -> Result<()> {
    let mut watermark = initial_since.min(Utc::now());
    let mut first_poll = true;
    loop {
        let poll_started_at = Utc::now();
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
                watermark = watermark.max(poll_started_at);
                first_poll = false;
                progress.send_modify(|progress| progress.record_new_markets(watermark));
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
    progress: watch::Sender<ReconciliationProgress>,
) -> Result<()> {
    let mut watermark = initial_since.min(Utc::now());
    loop {
        let poll_started_at = Utc::now();
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
                watermark = watermark.max(poll_started_at);
                progress.send_modify(|progress| progress.record_resolutions(watermark));
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
    progress_tx: watch::Sender<ReconciliationProgress>,
    mut progress_rx: watch::Receiver<ReconciliationProgress>,
) -> Result<()> {
    wait_for_cache_readiness(&mut progress_rx).await?;
    let mut saved_snapshot = None;
    loop {
        let safe_fetched_at = progress_rx
            .borrow()
            .safe_fetched_at()
            .context("cache readiness regressed after a complete baseline")?;
        match persist_snapshot(
            &lifecycle_tx,
            &redis_url,
            &key,
            safe_fetched_at,
            &mut saved_snapshot,
        )
        .await
        {
            Ok(true) => {
                progress_tx.send_modify(|progress| progress.record_cache_save(Utc::now()));
            }
            Ok(false) => {}
            Err(error) => warn!(%error, "save Rust market restart cache failed"),
        }
        tokio::time::sleep(CACHE_INTERVAL).await;
    }
}

async fn wait_for_cache_readiness(
    progress: &mut watch::Receiver<ReconciliationProgress>,
) -> Result<DateTime<Utc>> {
    loop {
        if let Some(fetched_at) = progress.borrow_and_update().safe_fetched_at() {
            return Ok(fetched_at);
        }
        progress
            .changed()
            .await
            .context("cache readiness sender dropped before a complete baseline")?;
    }
}

pub async fn persist_final_snapshot(
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
    redis_url: &str,
    key: &str,
    safe_fetched_at: DateTime<Utc>,
) -> Result<()> {
    persist_snapshot(lifecycle_tx, redis_url, key, safe_fetched_at, &mut None)
        .await
        .map(|_| ())
}

async fn persist_snapshot(
    lifecycle_tx: &mpsc::Sender<LifecycleRequest>,
    redis_url: &str,
    key: &str,
    safe_fetched_at: DateTime<Utc>,
    saved_snapshot: &mut Option<(u64, DateTime<Utc>)>,
) -> Result<bool> {
    let snapshot = request_snapshot(lifecycle_tx).await?;
    let identity = (snapshot.revision, safe_fetched_at);
    if !snapshot_changed(*saved_snapshot, identity) {
        return Ok(false);
    }
    let raw = tokio::task::spawn_blocking(move || serialize_snapshot(snapshot, safe_fetched_at))
        .await
        .context("cache serialization task failed")??;
    redis_cache::save_json(redis_url, key, raw).await?;
    *saved_snapshot = Some(identity);
    Ok(true)
}

fn snapshot_changed(saved: Option<(u64, DateTime<Utc>)>, candidate: (u64, DateTime<Utc>)) -> bool {
    saved != Some(candidate)
}

fn serialize_snapshot(
    snapshot: ActiveMarketSnapshot,
    safe_fetched_at: DateTime<Utc>,
) -> Result<String> {
    CacheDocument::new(safe_fetched_at, snapshot.markets)?.to_json()
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

fn new_market_observation(market: &GammaMarket) -> Option<MarketLifecycleObservation> {
    market.new_market_observation()
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

fn advance(slot: &mut Option<DateTime<Utc>>, candidate: DateTime<Utc>) {
    if slot.is_none_or(|current| candidate > current) {
        *slot = Some(candidate);
    }
}

fn age_seconds(now: DateTime<Utc>, then: Option<DateTime<Utc>>) -> Option<u64> {
    then.map(|then| now.signed_duration_since(then).num_seconds().max(0) as u64)
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

    fn market(hash: &str) -> Market {
        Market::new(hash.into(), format!("{hash}-a"), format!("{hash}-b"))
    }

    fn gamma_market_for(hash: &str) -> GammaMarket {
        let mut market = gamma_market();
        market.condition_id = hash.into();
        market.assets_ids = vec![format!("{hash}-a"), format!("{hash}-b")];
        market
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).unwrap()
    }

    #[test]
    fn restart_cache_places_recent_conditions_first() {
        let mut markets = vec![
            market("old-z"),
            market("recent-b"),
            market("old-a"),
            market("recent-a"),
        ];
        let prioritized = reorder_cached_markets(
            &mut markets,
            &["recent-a".into(), "missing".into(), "recent-b".into()],
        );

        assert_eq!(prioritized, 2);
        assert_eq!(
            markets
                .iter()
                .map(|market| market.hash.as_str())
                .collect::<Vec<_>>(),
            ["recent-a", "recent-b", "old-a", "old-z"]
        );
    }

    #[test]
    fn restart_plan_recovers_recent_markets_missing_from_cache() {
        let mut cached = vec![market("old"), market("cached-recent")];
        let plan = build_restart_market_plan(
            &mut cached,
            vec![
                gamma_market_for("missing"),
                gamma_market_for("cached-recent"),
            ],
        )
        .unwrap();

        assert_eq!(plan.prioritized, 1);
        assert_eq!(cached[0].hash, "cached-recent");
        assert_eq!(plan.missing.len(), 1);
        assert_eq!(plan.missing[0].condition_id, "missing");
        assert!(plan.missing[0].new_market_observation().is_some());
    }

    #[test]
    fn restart_plan_rejects_asset_conflicts_before_admission() {
        let mut cached = vec![market("cached")];
        let mut conflicting = gamma_market_for("missing");
        conflicting.assets_ids[0] = "cached-a".into();

        let error = build_restart_market_plan(&mut cached, vec![conflicting])
            .err()
            .unwrap();
        assert!(error.to_string().contains("reuses cached asset"));
    }

    #[test]
    fn restart_plan_deduplicates_repeated_gamma_conditions() {
        let mut cached = vec![market("cached")];
        let plan = build_restart_market_plan(
            &mut cached,
            vec![gamma_market_for("missing"), gamma_market_for("missing")],
        )
        .unwrap();
        assert_eq!(plan.missing.len(), 1);
    }

    #[tokio::test]
    async fn restart_admission_sends_warm_gamma_pages() {
        let timestamped = gamma_market_for("timestamped");
        let mut without_timestamp = gamma_market_for("silent");
        without_timestamp.created_at_ms = None;
        without_timestamp.start_date_ms = None;
        let silent = without_timestamp;
        let (lifecycle_tx, mut lifecycle_rx) = mpsc::channel(2);

        let admission = tokio::spawn(async move {
            admit_restart_markets(&lifecycle_tx, vec![timestamped, silent]).await
        });

        let LifecycleRequest::GammaPage {
            cold_start,
            markets,
            completion,
        } = lifecycle_rx.recv().await.unwrap()
        else {
            panic!("expected Gamma page")
        };
        assert!(!cold_start);
        assert_eq!(markets.len(), 2);
        assert_eq!(markets[0].condition_id, "timestamped");
        assert_eq!(markets[0].received_at_ns, 300);
        assert!(markets[0].new_market_observation().is_some());
        assert_eq!(markets[1].condition_id, "silent");
        assert!(markets[1].new_market_observation().is_none());
        completion.send(Ok(())).unwrap();

        admission.await.unwrap().unwrap();
        assert!(lifecycle_rx.try_recv().is_err());
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
        assert!(market.new_market_observation().is_none());
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

    #[test]
    fn safe_watermark_requires_every_reconciliation_in_any_order() {
        #[derive(Clone, Copy)]
        enum Step {
            Full,
            New,
            Resolved,
        }

        let permutations = [
            [Step::Full, Step::New, Step::Resolved],
            [Step::Full, Step::Resolved, Step::New],
            [Step::New, Step::Full, Step::Resolved],
            [Step::New, Step::Resolved, Step::Full],
            [Step::Resolved, Step::Full, Step::New],
            [Step::Resolved, Step::New, Step::Full],
        ];
        for permutation in permutations {
            let mut progress = ReconciliationProgress::default();
            for (index, step) in permutation.into_iter().enumerate() {
                match step {
                    Step::Full => progress.record_full_scan(at(30)),
                    Step::New => progress.record_new_markets(at(20)),
                    Step::Resolved => progress.record_resolutions(at(10)),
                }
                if index < 2 {
                    assert_eq!(progress.safe_fetched_at(), None);
                }
            }
            assert_eq!(progress.safe_fetched_at(), Some(at(10) - POLL_OVERLAP));
        }
    }

    #[test]
    fn reconciliation_progress_never_regresses() {
        let mut progress = ReconciliationProgress::default();
        progress.record_full_scan(at(30));
        progress.record_new_markets(at(20));
        progress.record_resolutions(at(15));
        progress.record_cache_save(at(40));

        progress.record_full_scan(at(29));
        progress.record_new_markets(at(19));
        progress.record_resolutions(at(14));
        progress.record_cache_save(at(39));

        assert_eq!(progress.full_scan_completed_at, Some(at(30)));
        assert_eq!(progress.new_markets_through, Some(at(20)));
        assert_eq!(progress.resolutions_through, Some(at(15)));
        assert_eq!(progress.cache_saved_at, Some(at(40)));
        assert_eq!(progress.safe_fetched_at(), Some(at(15) - POLL_OVERLAP));
        assert_eq!(progress.ages_at(at(50)).full_scan_seconds, Some(20));
        assert_eq!(progress.ages_at(at(50)).new_poll_seconds, Some(30));
        assert_eq!(progress.ages_at(at(50)).closed_poll_seconds, Some(35));
        assert_eq!(progress.ages_at(at(50)).cache_save_seconds, Some(10));
    }

    #[tokio::test]
    async fn cache_save_waits_for_full_new_and_resolved_reconciliation() {
        let (progress_tx, mut progress_rx) = watch::channel(ReconciliationProgress::default());
        let waiter = tokio::spawn(async move { wait_for_cache_readiness(&mut progress_rx).await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        progress_tx.send_modify(|progress| progress.record_new_markets(at(20)));
        progress_tx.send_modify(|progress| progress.record_full_scan(at(30)));
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        progress_tx.send_modify(|progress| progress.record_resolutions(at(10)));
        assert_eq!(waiter.await.unwrap().unwrap(), at(10) - POLL_OVERLAP);
    }

    #[tokio::test]
    async fn cache_save_fails_closed_without_a_complete_baseline() {
        let (progress_tx, mut progress_rx) = watch::channel(ReconciliationProgress::default());
        drop(progress_tx);
        let error = wait_for_cache_readiness(&mut progress_rx)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("complete baseline"));
    }

    #[test]
    fn cache_identity_includes_revision_and_safe_watermark() {
        let saved = Some((7, at(10)));
        assert!(!snapshot_changed(saved, (7, at(10))));
        assert!(snapshot_changed(saved, (8, at(10))));
        assert!(snapshot_changed(saved, (7, at(11))));
    }

    #[test]
    fn restart_watermark_preserves_delayed_visibility_overlap() {
        let direct_watermark = at(600);
        let delayed_event = direct_watermark - TimeDelta::minutes(1);
        let mut progress = ReconciliationProgress::default();
        progress.record_full_scan(at(700));
        progress.record_new_markets(direct_watermark);
        progress.record_resolutions(at(650));

        let restart_watermark = progress.safe_fetched_at().unwrap();
        assert_eq!(restart_watermark, direct_watermark - POLL_OVERLAP);
        assert!(restart_watermark <= delayed_event);
        assert_eq!(
            new_poll_cutoff(restart_watermark, true),
            restart_watermark.timestamp_millis()
        );
    }

    #[test]
    fn cache_serialization_uses_the_safe_watermark_exactly() {
        let safe_fetched_at = at(10) - POLL_OVERLAP;
        let raw = serialize_snapshot(
            ActiveMarketSnapshot {
                revision: 7,
                markets: vec![market("m")],
            },
            safe_fetched_at,
        )
        .unwrap();
        let document = CacheDocument::from_json(&raw).unwrap();
        assert_eq!(document.fetched_at(), safe_fetched_at);
        assert_eq!(document.markets().len(), 1);
        assert_eq!(document.markets()[0].hash, "m");
        assert_eq!(document.markets()[0].assets, ["m-a", "m-b"]);
    }
}
