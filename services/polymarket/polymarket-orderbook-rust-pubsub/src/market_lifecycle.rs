//! Authoritative lifecycle state and pool mutation coordinator.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};

use polymarket_orderbook_rust::events::{Market, MarketLifecycle, MarketLifecycleObservation};
use polymarket_orderbook_rust::markets;
use polymarket_orderbook_rust::markets::gamma::GammaMarket;
use polymarket_orderbook_rust::markets::lifecycle::{
    ActiveMarketSnapshot, LifecycleRequest, LifecycleSource,
};
use polymarket_orderbook_rust::ws::pool::Pool;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LifecycleKey {
    NewMarket(String),
    MarketResolved(String),
}

#[derive(Debug, Clone)]
enum PlannedObservation {
    Drop,
    Duplicate,
    SuppressTerminal { key: LifecycleKey },
    AdmitExisting { key: LifecycleKey },
    Subscribe { key: LifecycleKey, market: Market },
    Resolve { key: LifecycleKey, market: String },
}

struct PlannedGammaPage {
    observations: Vec<MarketLifecycleObservation>,
    bootstrap: Vec<Market>,
}

#[derive(Default)]
struct LifecycleState {
    active: HashMap<String, Market>,
    asset_owner: HashMap<String, String>,
    first_source: HashMap<LifecycleKey, LifecycleSource>,
    revision: u64,
}

impl LifecycleState {
    fn plan_bootstrap(&self, markets: Vec<Market>) -> Result<Vec<Market>> {
        let mut batch = HashMap::<String, [String; 2]>::new();
        let mut batch_asset_owner = HashMap::<String, String>::new();
        let mut planned = Vec::new();
        for market in markets {
            if self
                .first_source
                .contains_key(&LifecycleKey::MarketResolved(market.hash.clone()))
            {
                continue;
            }
            validate_market(&market)?;
            let assets = canonical_assets(&market.assets);
            if let Some(existing) = self.active.get(&market.hash) {
                let existing_assets = canonical_assets(&existing.assets);
                ensure!(
                    existing_assets == assets,
                    "market {} changed assets from {:?} to {:?}",
                    market.hash,
                    existing_assets,
                    assets
                );
                continue;
            }
            if let Some(existing) = batch.get(&market.hash) {
                ensure!(
                    existing == &assets,
                    "market {} has conflicting bootstrap assets {:?} and {:?}",
                    market.hash,
                    existing,
                    assets
                );
                continue;
            }
            for asset in &assets {
                if let Some(owner) = self.asset_owner.get(asset) {
                    ensure!(
                        owner == &market.hash,
                        "asset {asset} is already owned by market {owner}, cannot assign it to {}",
                        market.hash
                    );
                }
                if let Some(owner) = batch_asset_owner.get(asset) {
                    ensure!(
                        owner == &market.hash,
                        "asset {asset} is already owned by market {owner}, cannot assign it to {}",
                        market.hash
                    );
                }
                batch_asset_owner.insert(asset.clone(), market.hash.clone());
            }
            batch.insert(market.hash.clone(), assets);
            planned.push(market);
        }
        Ok(planned)
    }

    fn commit_bootstrap(&mut self, markets: &[Market]) {
        for market in markets {
            let assets = canonical_assets(&market.assets);
            for asset in &assets {
                self.asset_owner.insert(asset.clone(), market.hash.clone());
            }
            self.active.insert(market.hash.clone(), market.clone());
        }
        if !markets.is_empty() {
            self.bump_revision();
        }
    }

    fn plan_observation(
        &self,
        observation: &MarketLifecycleObservation,
    ) -> Result<PlannedObservation> {
        let update = observation
            .event
            .market_lifecycle()
            .context("token-scoped event sent to lifecycle coordinator")?;
        match update {
            MarketLifecycle::NewMarket {
                market,
                assets_ids,
                outcomes,
            } => {
                if market.is_empty() {
                    return Ok(PlannedObservation::Drop);
                }
                let key = LifecycleKey::NewMarket(market.clone());
                if self.first_source.contains_key(&key) {
                    return Ok(PlannedObservation::Duplicate);
                }
                if self
                    .first_source
                    .contains_key(&LifecycleKey::MarketResolved(market.clone()))
                {
                    return Ok(PlannedObservation::SuppressTerminal { key });
                }
                let Some(subscription) =
                    markets::binary_market_from_outcomes(market.clone(), &outcomes, &assets_ids)
                else {
                    return Ok(PlannedObservation::Drop);
                };
                validate_market(&subscription)?;
                let assets = canonical_assets(&subscription.assets);
                if let Some(existing) = self.active.get(&market) {
                    let existing_assets = canonical_assets(&existing.assets);
                    ensure!(
                        existing_assets == assets,
                        "market {market} changed assets from {:?} to {:?}",
                        existing_assets,
                        assets
                    );
                    return Ok(PlannedObservation::AdmitExisting { key });
                }
                for asset in &assets {
                    if let Some(owner) = self.asset_owner.get(asset) {
                        ensure!(
                            owner == &market,
                            "asset {asset} is already owned by market {owner}, cannot assign it to {market}"
                        );
                    }
                }
                Ok(PlannedObservation::Subscribe {
                    key,
                    market: subscription,
                })
            }
            MarketLifecycle::MarketResolved { market } => {
                ensure!(
                    !market.is_empty(),
                    "market_resolved has an empty condition ID"
                );
                let key = LifecycleKey::MarketResolved(market.clone());
                if self.first_source.contains_key(&key) {
                    return Ok(PlannedObservation::Duplicate);
                }
                Ok(PlannedObservation::Resolve { key, market })
            }
        }
    }

    fn plan_gamma_page(
        &self,
        markets: Vec<GammaMarket>,
        cold_start: bool,
    ) -> Result<PlannedGammaPage> {
        let mut batch = HashMap::<String, [String; 2]>::new();
        let mut batch_asset_owner = HashMap::<String, String>::new();
        let mut observations = Vec::new();
        let mut bootstrap = Vec::new();
        for market in markets {
            ensure!(
                market.is_active_binary(),
                "Gamma page contains an invalid active binary market"
            );
            if let Some(existing) = self.active.get(&market.condition_id) {
                ensure!(
                    canonical_asset_refs(&existing.assets)
                        == canonical_asset_refs(&market.assets_ids),
                    "Gamma changed assets for {}",
                    market.condition_id
                );
                continue;
            }
            if self
                .first_source
                .contains_key(&LifecycleKey::MarketResolved(market.condition_id.clone()))
            {
                continue;
            }
            let subscription = market
                .active_subscription()
                .context("Gamma page contains an invalid active binary market")?;
            let assets = canonical_assets(&subscription.assets);
            if let Some(existing) = batch.get(&subscription.hash) {
                ensure!(
                    existing == &assets,
                    "Gamma page contains conflicting assets for {}",
                    subscription.hash
                );
                continue;
            }
            for asset in &assets {
                if let Some(owner) = self.asset_owner.get(asset) {
                    ensure!(
                        owner == &subscription.hash,
                        "asset {asset} is already owned by market {owner}, cannot assign it to {}",
                        subscription.hash
                    );
                }
                if let Some(owner) = batch_asset_owner.get(asset) {
                    ensure!(
                        owner == &subscription.hash,
                        "asset {asset} is already owned by market {owner}, cannot assign it to {}",
                        subscription.hash
                    );
                }
                batch_asset_owner.insert(asset.clone(), subscription.hash.clone());
            }
            batch.insert(subscription.hash.clone(), assets);
            if !cold_start {
                if let Some(observation) = market.new_market_observation() {
                    observations.push(observation);
                    continue;
                }
            }
            bootstrap.push(subscription);
        }
        Ok(PlannedGammaPage {
            observations,
            bootstrap,
        })
    }

    fn commit_observation(&mut self, plan: &PlannedObservation, source: LifecycleSource) {
        match plan {
            PlannedObservation::Drop | PlannedObservation::Duplicate => {}
            PlannedObservation::SuppressTerminal { key } => {
                self.first_source.insert(key.clone(), source);
            }
            PlannedObservation::AdmitExisting { key } => {
                self.first_source.insert(key.clone(), source);
            }
            PlannedObservation::Subscribe { key, market } => {
                let assets = canonical_assets(&market.assets);
                for asset in &assets {
                    self.asset_owner.insert(asset.clone(), market.hash.clone());
                }
                self.active.insert(market.hash.clone(), market.clone());
                self.bump_revision();
                self.first_source.insert(key.clone(), source);
            }
            PlannedObservation::Resolve { key, market } => {
                if let Some(active_market) = self.active.remove(market) {
                    for asset in active_market.assets {
                        self.asset_owner.remove(&asset);
                    }
                    self.bump_revision();
                }
                self.first_source.insert(key.clone(), source);
            }
        }
    }

    fn snapshot(&self) -> ActiveMarketSnapshot {
        let mut markets: Vec<_> = self.active.values().cloned().collect();
        markets.sort_unstable_by(|left, right| left.hash.cmp(&right.hash));
        ActiveMarketSnapshot {
            revision: self.revision,
            markets,
        }
    }

    fn bump_revision(&mut self) {
        self.revision = self.revision.saturating_add(1);
    }
}

pub struct LifecycleCoordinator {
    pool: Arc<Mutex<Pool>>,
    state: LifecycleState,
    websocket_rx: mpsc::Receiver<MarketLifecycleObservation>,
    reconciliation_rx: mpsc::Receiver<LifecycleRequest>,
}

impl LifecycleCoordinator {
    pub fn new(
        pool: Arc<Mutex<Pool>>,
        websocket_rx: mpsc::Receiver<MarketLifecycleObservation>,
        reconciliation_rx: mpsc::Receiver<LifecycleRequest>,
    ) -> Self {
        Self {
            pool,
            state: LifecycleState::default(),
            websocket_rx,
            reconciliation_rx,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let mut websocket_open = true;
        let mut reconciliation_open = true;
        while websocket_open || reconciliation_open {
            tokio::select! {
                biased;
                observation = self.websocket_rx.recv(), if websocket_open => {
                    match observation {
                        Some(observation) => {
                            self.apply_observation(LifecycleSource::WebSocket, observation).await?;
                        }
                        None => websocket_open = false,
                    }
                }
                request = self.reconciliation_rx.recv(), if reconciliation_open => {
                    match request {
                        Some(request) => self.apply_request(request).await?,
                        None => reconciliation_open = false,
                    }
                }
            }
        }
        Ok(())
    }

    async fn apply_request(&mut self, request: LifecycleRequest) -> Result<()> {
        match request {
            LifecycleRequest::Bootstrap {
                markets,
                completion,
            } => {
                let result = self.apply_bootstrap(markets).await;
                complete_or_fail(completion, result)
            }
            LifecycleRequest::Observation {
                source,
                observation,
                completion,
            } => {
                let result = self.apply_observation(source, observation).await;
                complete_or_fail(completion, result)
            }
            LifecycleRequest::Snapshot { completion } => {
                if completion.send(self.state.snapshot()).is_err() {
                    warn!("lifecycle snapshot requester stopped before receiving snapshot");
                }
                Ok(())
            }
            LifecycleRequest::GammaPage {
                cold_start,
                markets,
                completion,
            } => {
                let result = self.apply_gamma_page(cold_start, markets).await;
                complete_or_fail(completion, result)
            }
        }
    }

    async fn apply_gamma_page(
        &mut self,
        cold_start: bool,
        markets: Vec<GammaMarket>,
    ) -> Result<()> {
        let plan = self.state.plan_gamma_page(markets, cold_start)?;
        for observation in plan.observations {
            self.apply_observation(LifecycleSource::Gamma, observation)
                .await?;
        }
        if !plan.bootstrap.is_empty() {
            let planned = self.state.plan_bootstrap(plan.bootstrap)?;
            self.pool
                .lock()
                .await
                .subscribe_markets(planned.clone())
                .await
                .context("subscribe Gamma page")?;
            self.state.commit_bootstrap(&planned);
        }
        Ok(())
    }

    async fn apply_bootstrap(&mut self, markets: Vec<Market>) -> Result<()> {
        let planned = self.state.plan_bootstrap(markets)?;
        if planned.is_empty() {
            return Ok(());
        }
        self.pool
            .lock()
            .await
            .subscribe_markets(planned.clone())
            .await
            .context("subscribe bootstrap markets")?;
        self.state.commit_bootstrap(&planned);
        info!(
            added = planned.len(),
            active_markets = self.state.active.len(),
            "lifecycle bootstrap applied"
        );
        Ok(())
    }

    async fn apply_observation(
        &mut self,
        source: LifecycleSource,
        observation: MarketLifecycleObservation,
    ) -> Result<()> {
        if source == LifecycleSource::Gamma {
            match observation.event.market_lifecycle() {
                Some(MarketLifecycle::NewMarket { market, .. })
                    if self.state.active.contains_key(&market) =>
                {
                    return Ok(());
                }
                Some(MarketLifecycle::MarketResolved { market })
                    if !self.state.active.contains_key(&market) =>
                {
                    return Ok(());
                }
                _ => {}
            }
        }
        let plan = self.state.plan_observation(&observation)?;
        match &plan {
            PlannedObservation::Drop => {
                warn!(
                    source = ?source,
                    "dropping incomplete or non-binary new_market observation"
                );
            }
            PlannedObservation::Duplicate => {}
            PlannedObservation::SuppressTerminal { .. } => {
                warn!(
                    source = ?source,
                    "suppressing stale new_market observation after market_resolved"
                );
            }
            PlannedObservation::AdmitExisting { .. } => {
                self.pool
                    .lock()
                    .await
                    .admit_lifecycle(observation.event, observation.timestamp_received_ns);
            }
            PlannedObservation::Subscribe { market, .. } => {
                let mut pool = self.pool.lock().await;
                pool.admit_lifecycle(observation.event, observation.timestamp_received_ns);
                pool.subscribe_markets(vec![market.clone()])
                    .await
                    .context("subscribe lifecycle market")?;
                info!(
                    source = ?source,
                    market = market.hash,
                    "[MARKET_EVENT] new_market"
                );
            }
            PlannedObservation::Resolve { market, .. } => {
                let mut pool = self.pool.lock().await;
                pool.admit_lifecycle(observation.event, observation.timestamp_received_ns);
                pool.unsubscribe_markets(vec![market.clone()])
                    .await
                    .context("unsubscribe resolved market")?;
                info!(source = ?source, market, "[MARKET_EVENT] market_resolved");
            }
        }
        self.state.commit_observation(&plan, source);
        Ok(())
    }
}

fn complete_or_fail(
    completion: polymarket_orderbook_rust::markets::lifecycle::LifecycleCompletion,
    result: Result<()>,
) -> Result<()> {
    match result {
        Ok(()) => {
            if completion.send(Ok(())).is_err() {
                warn!("lifecycle requester stopped before receiving completion");
            }
            Ok(())
        }
        Err(error) => {
            let message = error.to_string();
            let _ = completion.send(Err(message));
            Err(error)
        }
    }
}

fn validate_market(market: &Market) -> Result<()> {
    ensure!(!market.hash.is_empty(), "market hash must not be empty");
    ensure!(
        market.assets.iter().all(|asset| !asset.is_empty()),
        "market {} has an empty asset ID",
        market.hash
    );
    ensure!(
        market.assets[0] != market.assets[1],
        "market {} assigns both outcomes to asset {}",
        market.hash,
        market.assets[0]
    );
    Ok(())
}

fn canonical_assets(assets: &[String; 2]) -> [String; 2] {
    if assets[0] <= assets[1] {
        assets.clone()
    } else {
        [assets[1].clone(), assets[0].clone()]
    }
}

fn canonical_asset_refs(assets: &[String]) -> [&str; 2] {
    if assets[0] <= assets[1] {
        [&assets[0], &assets[1]]
    } else {
        [&assets[1], &assets[0]]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_orderbook_rust::events::Event;

    fn market(hash: &str, first: &str, second: &str) -> Market {
        Market::new(hash.into(), first.into(), second.into())
    }

    fn new_market(hash: &str, assets: [&str; 2]) -> MarketLifecycleObservation {
        MarketLifecycleObservation {
            event: Event::NewMarket {
                id: "1".into(),
                market: hash.into(),
                timestamp: "1".into(),
                assets_ids: assets.into_iter().map(str::to_string).collect(),
                outcomes: vec!["Yes".into(), "No".into()],
                question: None,
                slug: None,
            },
            timestamp_received_ns: 1,
        }
    }

    fn resolved(hash: &str) -> MarketLifecycleObservation {
        MarketLifecycleObservation {
            event: Event::MarketResolved {
                id: "1".into(),
                market: hash.into(),
                timestamp: "2".into(),
                assets_ids: vec!["a".into(), "b".into()],
                winning_asset_id: Some("a".into()),
                winning_outcome: Some("Yes".into()),
            },
            timestamp_received_ns: 2,
        }
    }

    fn gamma_market(hash: &str) -> GammaMarket {
        GammaMarket {
            id: "1".into(),
            condition_id: hash.into(),
            question: String::new(),
            slug: String::new(),
            active: true,
            closed: false,
            uma_resolution_status: String::new(),
            assets_ids: vec![format!("{hash}-a"), format!("{hash}-b")],
            outcomes: vec!["Yes".into(), "No".into()],
            outcome_prices: Vec::new(),
            created_at_ms: Some(10),
            start_date_ms: Some(20),
            closed_time_ms: None,
            received_at_ns: 30,
        }
    }

    #[test]
    fn bootstrap_seeds_state_without_an_export_action() {
        let mut state = LifecycleState::default();
        let planned = state.plan_bootstrap(vec![market("m", "a", "b")]).unwrap();
        assert_eq!(planned.len(), 1);
        state.commit_bootstrap(&planned);
        assert_eq!(state.active.len(), 1);
        assert_eq!(state.asset_owner.len(), 2);
        assert!(state.first_source.is_empty());

        let observation = new_market("m", ["a", "b"]);
        let first_observation = state.plan_observation(&observation).unwrap();
        assert!(matches!(
            first_observation,
            PlannedObservation::AdmitExisting { .. }
        ));
        state.commit_observation(&first_observation, LifecycleSource::WebSocket);
        assert_eq!(
            state.first_source.get(&LifecycleKey::NewMarket("m".into())),
            Some(&LifecycleSource::WebSocket)
        );
    }

    #[test]
    fn duplicate_ws_and_gamma_inputs_keep_first_source() {
        let mut state = LifecycleState::default();
        let observation = new_market("m", ["a", "b"]);
        let first = state.plan_observation(&observation).unwrap();
        assert!(matches!(first, PlannedObservation::Subscribe { .. }));
        state.commit_observation(&first, LifecycleSource::WebSocket);

        let duplicate = state.plan_observation(&observation).unwrap();
        assert!(matches!(duplicate, PlannedObservation::Duplicate));
        state.commit_observation(&duplicate, LifecycleSource::Gamma);
        assert_eq!(
            state.first_source.get(&LifecycleKey::NewMarket("m".into())),
            Some(&LifecycleSource::WebSocket)
        );
    }

    #[test]
    fn gamma_can_win_source_precedence() {
        let mut state = LifecycleState::default();
        let observation = new_market("m", ["a", "b"]);
        let first = state.plan_observation(&observation).unwrap();
        state.commit_observation(&first, LifecycleSource::Gamma);
        let duplicate = state.plan_observation(&observation).unwrap();
        state.commit_observation(&duplicate, LifecycleSource::WebSocket);
        assert_eq!(
            state.first_source.get(&LifecycleKey::NewMarket("m".into())),
            Some(&LifecycleSource::Gamma)
        );
    }

    #[test]
    fn conflicting_active_asset_pair_is_rejected() {
        let mut state = LifecycleState::default();
        let planned = state.plan_bootstrap(vec![market("m", "a", "b")]).unwrap();
        state.commit_bootstrap(&planned);
        let error = state
            .plan_observation(&new_market("m", ["a", "c"]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("changed assets"), "{error}");
    }

    #[test]
    fn asset_cannot_be_owned_by_two_active_markets() {
        let mut state = LifecycleState::default();
        let planned = state
            .plan_bootstrap(vec![market("first", "a", "b")])
            .unwrap();
        state.commit_bootstrap(&planned);

        let error = state
            .plan_observation(&new_market("second", ["a", "c"]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("already owned by market first"), "{error}");
    }

    #[test]
    fn bootstrap_rejects_an_asset_shared_within_one_batch() {
        let state = LifecycleState::default();
        let error = state
            .plan_bootstrap(vec![market("first", "a", "b"), market("second", "a", "c")])
            .unwrap_err()
            .to_string();

        assert!(error.contains("already owned by market first"), "{error}");
    }

    #[test]
    fn incomplete_new_market_does_not_consume_dedup_state() {
        let mut state = LifecycleState::default();
        let mut incomplete = new_market("m", ["a", "b"]);
        if let Event::NewMarket { assets_ids, .. } = &mut incomplete.event {
            assets_ids.pop();
        }

        let dropped = state.plan_observation(&incomplete).unwrap();
        assert!(matches!(dropped, PlannedObservation::Drop));
        state.commit_observation(&dropped, LifecycleSource::Gamma);
        assert!(state.first_source.is_empty());

        assert!(matches!(
            state
                .plan_observation(&new_market("m", ["a", "b"]))
                .unwrap(),
            PlannedObservation::Subscribe { .. }
        ));
    }

    #[test]
    fn resolution_removes_active_state_and_deduplicates() {
        let mut state = LifecycleState::default();
        let planned = state.plan_bootstrap(vec![market("m", "a", "b")]).unwrap();
        state.commit_bootstrap(&planned);

        let observation = resolved("m");
        let resolution = state.plan_observation(&observation).unwrap();
        assert!(matches!(resolution, PlannedObservation::Resolve { .. }));
        state.commit_observation(&resolution, LifecycleSource::WebSocket);
        assert!(state.active.is_empty());
        assert!(state.asset_owner.is_empty());

        let duplicate = state.plan_observation(&observation).unwrap();
        assert!(matches!(duplicate, PlannedObservation::Duplicate));
        state.commit_observation(&duplicate, LifecycleSource::Gamma);
        assert_eq!(
            state
                .first_source
                .get(&LifecycleKey::MarketResolved("m".into())),
            Some(&LifecycleSource::WebSocket)
        );
    }

    #[test]
    fn resolution_before_new_market_keeps_condition_terminal() {
        let mut state = LifecycleState::default();
        let resolution = resolved("m");
        let planned_resolution = state.plan_observation(&resolution).unwrap();
        state.commit_observation(&planned_resolution, LifecycleSource::WebSocket);

        let late_new_market = new_market("m", ["a", "b"]);
        let suppressed = state.plan_observation(&late_new_market).unwrap();
        assert!(matches!(
            suppressed,
            PlannedObservation::SuppressTerminal { .. }
        ));
        state.commit_observation(&suppressed, LifecycleSource::Gamma);

        assert!(state.active.is_empty());
        assert!(state.asset_owner.is_empty());
        assert_eq!(
            state.first_source.get(&LifecycleKey::NewMarket("m".into())),
            Some(&LifecycleSource::Gamma)
        );

        assert!(state
            .plan_bootstrap(vec![market("m", "a", "b")])
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn coordinator_snapshot_request_is_sorted_and_cloned() {
        let (event_tx, _event_rx) = mpsc::channel(8);
        let pool = Arc::new(Mutex::new(Pool::new(2, event_tx)));
        let (_websocket_tx, websocket_rx) = mpsc::channel(8);
        let (_reconciliation_tx, reconciliation_rx) = mpsc::channel(8);
        let mut coordinator = LifecycleCoordinator::new(pool, websocket_rx, reconciliation_rx);
        let planned = coordinator
            .state
            .plan_bootstrap(vec![
                market("z", "z-yes", "z-no"),
                market("a", "a-yes", "a-no"),
            ])
            .unwrap();
        coordinator.state.commit_bootstrap(&planned);

        let (completion, completed) = tokio::sync::oneshot::channel();
        coordinator
            .apply_request(LifecycleRequest::Snapshot { completion })
            .await
            .unwrap();
        let snapshot = completed.await.unwrap();

        assert_eq!(
            snapshot
                .markets
                .iter()
                .map(|market| market.hash.as_str())
                .collect::<Vec<_>>(),
            ["a", "z"]
        );
        assert_eq!(snapshot.markets[1].assets, ["z-yes", "z-no"]);

        let resolution = resolved("a");
        let plan = coordinator.state.plan_observation(&resolution).unwrap();
        coordinator
            .state
            .commit_observation(&plan, LifecycleSource::WebSocket);
        assert_eq!(coordinator.state.active.len(), 1);
        assert_eq!(snapshot.markets.len(), 2);
    }

    #[test]
    fn admitting_lifecycle_for_existing_market_does_not_dirty_snapshot() {
        let mut state = LifecycleState::default();
        let planned = state.plan_bootstrap(vec![market("m", "a", "b")]).unwrap();
        state.commit_bootstrap(&planned);
        let revision = state.revision;

        let observation = new_market("m", ["a", "b"]);
        let admitted = state.plan_observation(&observation).unwrap();
        state.commit_observation(&admitted, LifecycleSource::WebSocket);

        assert_eq!(state.revision, revision);
        assert!(state.active.contains_key("m"));
    }

    #[test]
    fn gamma_planner_preserves_cold_and_warm_semantics() {
        let state = LifecycleState::default();
        let cold = state
            .plan_gamma_page(vec![gamma_market("cold")], true)
            .unwrap();
        assert_eq!(cold.bootstrap.len(), 1);
        assert!(cold.observations.is_empty());
        let warm = state
            .plan_gamma_page(vec![gamma_market("warm")], false)
            .unwrap();
        assert!(warm.bootstrap.is_empty());
        assert_eq!(warm.observations.len(), 1);
        assert_eq!(warm.observations[0].timestamp_received_ns, 30);

        let mut timestamp_less = gamma_market("silent");
        timestamp_less.created_at_ms = None;
        timestamp_less.start_date_ms = None;
        let silent = state.plan_gamma_page(vec![timestamp_less], false).unwrap();
        assert_eq!(silent.bootstrap.len(), 1);
        assert!(silent.observations.is_empty());
    }

    #[test]
    fn gamma_page_rejects_conflicts_before_observation() {
        let state = LifecycleState::default();
        let first = gamma_market("same");
        let mut conflicting = first.clone();
        conflicting.assets_ids[1] = "different".into();

        let error = state
            .plan_gamma_page(vec![first, conflicting], false)
            .err()
            .unwrap();
        assert!(error.to_string().contains("conflicting assets"));
    }

    #[test]
    fn gamma_page_validates_existing_assets_without_replaying_lifecycle() {
        let mut state = LifecycleState::default();
        let planned = state
            .plan_bootstrap(vec![market("existing", "existing-a", "existing-b")])
            .unwrap();
        state.commit_bootstrap(&planned);

        let unchanged = state
            .plan_gamma_page(vec![gamma_market("existing")], false)
            .unwrap();
        assert!(unchanged.observations.is_empty());
        assert!(unchanged.bootstrap.is_empty());

        let mut conflicting = gamma_market("existing");
        conflicting.assets_ids[1] = "changed".into();
        let error = state
            .plan_gamma_page(vec![conflicting], false)
            .err()
            .unwrap();
        assert!(error.to_string().contains("Gamma changed assets"));
    }

    #[test]
    fn gamma_page_does_not_reactivate_resolved_market() {
        let mut state = LifecycleState::default();
        state.first_source.insert(
            LifecycleKey::MarketResolved("terminal".into()),
            LifecycleSource::WebSocket,
        );

        let plan = state
            .plan_gamma_page(vec![gamma_market("terminal")], false)
            .unwrap();
        assert!(plan.observations.is_empty());
        assert!(plan.bootstrap.is_empty());
    }

    #[tokio::test]
    async fn unknown_gamma_resolution_is_dropped_without_dedup_state() {
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let pool = Arc::new(Mutex::new(Pool::new(2, event_tx)));
        let (_websocket_tx, websocket_rx) = mpsc::channel(8);
        let (_reconciliation_tx, reconciliation_rx) = mpsc::channel(8);
        let mut coordinator = LifecycleCoordinator::new(pool, websocket_rx, reconciliation_rx);

        coordinator
            .apply_observation(LifecycleSource::Gamma, resolved("unknown"))
            .await
            .unwrap();

        assert!(coordinator.state.first_source.is_empty());
        assert!(event_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn gamma_new_market_does_not_replay_bootstrapped_condition() {
        let (event_tx, mut event_rx) = mpsc::channel(8);
        let pool = Arc::new(Mutex::new(Pool::new(2, event_tx)));
        let (_websocket_tx, websocket_rx) = mpsc::channel(8);
        let (_reconciliation_tx, reconciliation_rx) = mpsc::channel(8);
        let mut coordinator = LifecycleCoordinator::new(pool, websocket_rx, reconciliation_rx);
        let planned = coordinator
            .state
            .plan_bootstrap(vec![market("m", "a", "b")])
            .unwrap();
        coordinator.state.commit_bootstrap(&planned);

        coordinator
            .apply_observation(LifecycleSource::Gamma, new_market("m", ["a", "b"]))
            .await
            .unwrap();

        assert!(coordinator.state.first_source.is_empty());
        assert!(event_rx.try_recv().is_err());
    }
}
