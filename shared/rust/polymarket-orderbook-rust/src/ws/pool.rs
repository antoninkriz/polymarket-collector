//! WebSocket connection pool with one authoritative route per market.
//!
//! Both outcome assets of a binary market are always subscribed on the same
//! connection.  This matters because one `price_change` parent can contain
//! updates for both assets: splitting those assets across redundant sockets
//! creates two independently delivered copies whose relative order cannot be
//! merged correctly.  V3 chooses correctness over seamless failover.  A
//! after a reconnect, fresh `book` snapshots replace the full local state.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{ensure, Result};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::events::{Market, MarketLifecycle};
use crate::record::{CollectorContext, EventRecord};
use crate::ws::connection::{Command, ConnStatus, Connection};

const COMMAND_CHANNEL_SIZE: usize = 64;

struct ConnHandle {
    conn_id: usize,
    assets: HashSet<String>,
    cmd_tx: mpsc::Sender<Command>,
    join: JoinHandle<Result<()>>,
}

pub struct PoolStats {
    pub market_count: usize,
    pub connection_count: usize,
    pub asset_down_events: u64,
    pub conn_down_events: u64,
    pub conns_down: usize,
    pub assets_down: usize,
}

#[derive(Default)]
pub struct HealthCounters {
    pub asset_down_events: AtomicU64,
    pub conn_down_events: AtomicU64,
    pub conns_down: AtomicU64,
    pub assets_down: AtomicU64,
}

pub struct Pool {
    max_assets_per_conn: usize,
    event_tx: mpsc::Sender<EventRecord>,
    collector: Arc<CollectorContext>,
    connections: Vec<ConnHandle>,
    markets: HashMap<String, Market>,
    /// Stable connection IDs, not vector positions.
    market_to_conn: HashMap<String, usize>,
    asset_to_conn: HashMap<String, usize>,
    next_conn_id: usize,
    /// Stable ID of the one socket allowed to emit connection-wide market
    /// lifecycle events. The leader remains alive even with no data assets.
    lifecycle_conn_id: Option<usize>,
    lifecycle_tx: Option<mpsc::UnboundedSender<MarketLifecycle>>,
    health_counters: Arc<HealthCounters>,
    monitor_join: Option<JoinHandle<()>>,
    status_event_tx: mpsc::UnboundedSender<(usize, ConnStatus)>,
    asset_conns_tx: watch::Sender<HashMap<String, usize>>,
}

impl Pool {
    pub fn new(max_assets_per_conn: usize, event_tx: mpsc::Sender<EventRecord>) -> Self {
        Self::new_with_publisher_generation(max_assets_per_conn, event_tx, 0)
    }

    pub fn new_with_publisher_generation(
        max_assets_per_conn: usize,
        event_tx: mpsc::Sender<EventRecord>,
        publisher_generation: u64,
    ) -> Self {
        Self::build(max_assets_per_conn, event_tx, publisher_generation, None)
    }

    pub fn new_with_lifecycle(
        max_assets_per_conn: usize,
        event_tx: mpsc::Sender<EventRecord>,
        publisher_generation: u64,
        lifecycle_tx: mpsc::UnboundedSender<MarketLifecycle>,
    ) -> Self {
        Self::build(
            max_assets_per_conn,
            event_tx,
            publisher_generation,
            Some(lifecycle_tx),
        )
    }

    fn build(
        max_assets_per_conn: usize,
        event_tx: mpsc::Sender<EventRecord>,
        publisher_generation: u64,
        lifecycle_tx: Option<mpsc::UnboundedSender<MarketLifecycle>>,
    ) -> Self {
        let collector = Arc::new(CollectorContext::with_publisher_generation(
            publisher_generation,
        ));
        let health_counters = Arc::new(HealthCounters::default());
        let (status_event_tx, status_event_rx) = mpsc::unbounded_channel();
        let (asset_conns_tx, asset_conns_rx) = watch::channel(HashMap::new());

        let monitor_join = Some(tokio::spawn(run_health_monitor(
            status_event_rx,
            asset_conns_rx,
            Arc::clone(&health_counters),
        )));

        Self {
            max_assets_per_conn,
            event_tx,
            collector,
            connections: Vec::new(),
            markets: HashMap::new(),
            market_to_conn: HashMap::new(),
            asset_to_conn: HashMap::new(),
            next_conn_id: 0,
            lifecycle_conn_id: None,
            lifecycle_tx,
            health_counters,
            monitor_join,
            status_event_tx,
            asset_conns_tx,
        }
    }

    pub fn subscribed_market_count(&self) -> usize {
        self.markets.len()
    }

    /// Ensure the lifecycle leader exists. This is required even with no
    /// preloaded assets so `--new-only` can receive `new_market` events.
    pub async fn start(&mut self) {
        if self.lifecycle_conn_id.is_none() {
            self.spawn_connection().await;
        }
    }

    #[cfg(test)]
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    pub fn pool_stats(&self) -> PoolStats {
        PoolStats {
            market_count: self.markets.len(),
            connection_count: self.connections.len(),
            asset_down_events: self
                .health_counters
                .asset_down_events
                .load(Ordering::Relaxed),
            conn_down_events: self
                .health_counters
                .conn_down_events
                .load(Ordering::Relaxed),
            conns_down: self.health_counters.conns_down.load(Ordering::Relaxed) as usize,
            assets_down: self.health_counters.assets_down.load(Ordering::Relaxed) as usize,
        }
    }

    /// Subscribe new markets without splitting a market across connections.
    pub async fn subscribe_markets(&mut self, markets: Vec<Market>) -> Result<()> {
        ensure!(
            self.max_assets_per_conn >= 2,
            "MAX_ASSETS_PER_CONN must be at least 2 so a market stays atomic"
        );

        // Validate and deduplicate the whole input before mutating pool state.
        // Checking only `self.markets` is insufficient: the same hash can
        // occur twice in one preload batch and, at a capacity boundary, end
        // up subscribed on two different sockets.
        let mut new_markets = Vec::new();
        let mut batch_markets: HashMap<String, [String; 2]> = HashMap::new();
        let mut batch_assets: HashMap<String, String> = HashMap::new();
        for market in markets {
            ensure!(!market.hash.is_empty(), "market hash must not be empty");
            ensure!(
                market.assets.iter().all(|asset| !asset.is_empty()),
                "market {} has an empty asset id",
                market.hash,
            );
            ensure!(
                market.assets[0] != market.assets[1],
                "market {} assigns both outcomes to asset {}",
                market.hash,
                market.assets[0],
            );

            if let Some(existing) = self.markets.get(&market.hash) {
                ensure!(
                    existing.assets == market.assets,
                    "market {} changed assets from {:?} to {:?}",
                    market.hash,
                    existing.assets,
                    market.assets,
                );
                continue;
            }
            if let Some(existing_assets) = batch_markets.get(&market.hash) {
                ensure!(
                    existing_assets == &market.assets,
                    "market {} has conflicting assets {:?} and {:?} in one batch",
                    market.hash,
                    existing_assets,
                    market.assets,
                );
                continue;
            }

            for asset in &market.assets {
                ensure!(
                    !self.asset_to_conn.contains_key(asset),
                    "asset {} is already assigned to another subscribed market",
                    asset,
                );
                if let Some(owner) = batch_assets.get(asset) {
                    ensure!(
                        owner == &market.hash,
                        "asset {} is shared by markets {} and {} in one batch",
                        asset,
                        owner,
                        market.hash,
                    );
                } else {
                    batch_assets.insert(asset.clone(), market.hash.clone());
                }
            }
            batch_markets.insert(market.hash.clone(), market.assets.clone());
            new_markets.push(market);
        }
        if new_markets.is_empty() {
            return Ok(());
        }

        info!(
            new_markets = new_markets.len(),
            new_assets = new_markets.len() * 2,
            "subscribing markets on authoritative connections",
        );

        let mut pending: HashMap<usize, Vec<String>> = HashMap::new();
        for market in new_markets {
            let conn_index = match self.find_conn_with_capacity(2, &pending) {
                Some(index) => index,
                None => {
                    self.spawn_connection().await;
                    self.connections.len() - 1
                }
            };
            let conn_id = self.connections[conn_index].conn_id;
            let assets = market.assets.clone();

            pending
                .entry(conn_id)
                .or_default()
                .extend(assets.iter().cloned());
            self.market_to_conn.insert(market.hash.clone(), conn_id);
            for asset in &assets {
                self.asset_to_conn.insert(asset.clone(), conn_id);
            }
            self.markets.insert(market.hash.clone(), market);
        }

        for (conn_id, assets) in pending {
            let handle = self
                .connections
                .iter_mut()
                .find(|handle| handle.conn_id == conn_id)
                .expect("newly assigned connection must exist");
            handle.assets.extend(assets.iter().cloned());
            if let Err(error) = handle.cmd_tx.send(Command::Subscribe(assets)).await {
                warn!(conn = conn_id, %error, "subscribe send failed");
            }
        }

        self.update_monitor_mappings();
        info!(
            total_markets = self.markets.len(),
            total_connections = self.connections.len(),
            total_assets_tracked = self.asset_to_conn.len(),
            "subscribe_markets complete",
        );
        Ok(())
    }

    pub async fn unsubscribe_markets(&mut self, market_hashes: Vec<String>) -> Result<()> {
        let mut conn_unsubs: HashMap<usize, Vec<String>> = HashMap::new();
        let mut removed_count = 0_usize;

        for hash in market_hashes {
            let Some(market) = self.markets.remove(&hash) else {
                continue;
            };
            removed_count += 1;
            if let Some(conn_id) = self.market_to_conn.remove(&hash) {
                for asset in market.assets {
                    self.asset_to_conn.remove(&asset);
                    conn_unsubs.entry(conn_id).or_default().push(asset);
                }
            }
        }

        for (conn_id, assets) in conn_unsubs {
            if let Some(handle) = self.connections.iter_mut().find(|h| h.conn_id == conn_id) {
                for asset in &assets {
                    handle.assets.remove(asset);
                }
                if let Err(error) = handle.cmd_tx.send(Command::Unsubscribe(assets)).await {
                    warn!(conn = conn_id, %error, "unsubscribe send failed");
                }
            }
        }

        let mut index = 0;
        while index < self.connections.len() {
            if self.connections[index].assets.is_empty()
                && Some(self.connections[index].conn_id) != self.lifecycle_conn_id
            {
                let handle = self.connections.swap_remove(index);
                let conn_id = handle.conn_id;
                drop(handle.cmd_tx);
                info!(
                    conn = conn_id,
                    remaining = self.connections.len(),
                    "removed empty connection"
                );
                tokio::spawn(async move {
                    let _ = handle.join.await;
                });
            } else {
                index += 1;
            }
        }

        if removed_count > 0 {
            self.update_monitor_mappings();
        }
        info!(
            unsubscribed = removed_count,
            remaining_markets = self.markets.len(),
            "unsubscribe_markets complete",
        );
        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<()> {
        info!(connections = self.connections.len(), "shutting down pool");
        if let Some(join) = self.monitor_join.take() {
            join.abort();
            let _ = join.await;
        }

        for handle in self.connections.drain(..) {
            let conn_id = handle.conn_id;
            drop(handle.cmd_tx);
            match handle.join.await {
                Ok(Ok(())) => debug!(conn = conn_id, "connection joined"),
                Ok(Err(error)) => warn!(conn = conn_id, %error, "connection ended with error"),
                Err(error) => warn!(conn = conn_id, %error, "connection panicked"),
            }
        }
        Ok(())
    }

    fn find_conn_with_capacity(
        &self,
        required: usize,
        pending: &HashMap<usize, Vec<String>>,
    ) -> Option<usize> {
        self.connections
            .iter()
            .enumerate()
            .find_map(|(index, handle)| {
                let pending_count = pending.get(&handle.conn_id).map(Vec::len).unwrap_or(0);
                (handle.assets.len() + pending_count + required <= self.max_assets_per_conn)
                    .then_some(index)
            })
    }

    async fn spawn_connection(&mut self) {
        let conn_id = self.next_conn_id;
        self.next_conn_id += 1;
        let lifecycle_leader = self.lifecycle_conn_id.is_none();
        if lifecycle_leader {
            self.lifecycle_conn_id = Some(conn_id);
        }
        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_SIZE);
        let connection = Connection::new(
            conn_id,
            self.event_tx.clone(),
            Arc::clone(&self.collector),
            self.status_event_tx.clone(),
            lifecycle_leader,
            self.lifecycle_tx.clone(),
        );
        let join = tokio::spawn(connection.run(cmd_rx));
        self.connections.push(ConnHandle {
            conn_id,
            assets: HashSet::new(),
            cmd_tx,
            join,
        });
        info!(
            conn = conn_id,
            lifecycle_leader,
            total = self.connections.len(),
            "spawned connection"
        );
    }

    fn update_monitor_mappings(&self) {
        let _ = self.asset_conns_tx.send(self.asset_to_conn.clone());
    }
}

async fn run_health_monitor(
    mut status_rx: mpsc::UnboundedReceiver<(usize, ConnStatus)>,
    mut asset_conns_rx: watch::Receiver<HashMap<String, usize>>,
    counters: Arc<HealthCounters>,
) {
    let mut asset_conns: HashMap<String, usize> = HashMap::new();
    let mut conn_status: HashMap<usize, ConnStatus> = HashMap::new();
    let mut down_assets: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            event = status_rx.recv() => {
                let Some((conn_id, new_status)) = event else { break };
                let old_status = conn_status
                    .insert(conn_id, new_status)
                    .unwrap_or(ConnStatus::Disconnected);
                if old_status == new_status {
                    continue;
                }
                match new_status {
                    ConnStatus::Disconnected => {
                        counters.conn_down_events.fetch_add(1, Ordering::Relaxed);
                    }
                    ConnStatus::Connected => {}
                }

                for (asset, assigned_conn) in &asset_conns {
                    if *assigned_conn != conn_id {
                        continue;
                    }
                    match new_status {
                        ConnStatus::Disconnected => {
                            if down_assets.insert(asset.clone()) {
                                counters.asset_down_events.fetch_add(1, Ordering::Relaxed);
                                warn!(asset, conn = conn_id, "[ASSET-DATA-GAP] authoritative connection down");
                            }
                        }
                        ConnStatus::Connected => {
                            if down_assets.remove(asset) {
                                info!(asset, conn = conn_id, "asset connection restored; waiting for book snapshot");
                            }
                        }
                    }
                }
                update_gauges(&counters, &asset_conns, &conn_status);
            }
            changed = asset_conns_rx.changed() => {
                if changed.is_err() {
                    break;
                }
                asset_conns = asset_conns_rx.borrow_and_update().clone();
                down_assets.retain(|asset| asset_conns.contains_key(asset));
                update_gauges(&counters, &asset_conns, &conn_status);
            }
        }
    }
}

fn update_gauges(
    counters: &HealthCounters,
    asset_conns: &HashMap<String, usize>,
    conn_status: &HashMap<usize, ConnStatus>,
) {
    let active_connections: HashSet<usize> = asset_conns.values().copied().collect();
    let conns_down = active_connections
        .iter()
        .filter(|id| !matches!(conn_status.get(id), Some(ConnStatus::Connected)))
        .count();
    let assets_down = asset_conns
        .values()
        .filter(|id| !matches!(conn_status.get(id), Some(ConnStatus::Connected)))
        .count();
    counters
        .conns_down
        .store(conns_down as u64, Ordering::Relaxed);
    counters
        .assets_down
        .store(assets_down as u64, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market(hash: &str, yes: &str, no: &str) -> Market {
        Market::new(hash.into(), yes.into(), no.into())
    }

    fn pool(max_assets: usize) -> Pool {
        let (tx, _rx) = mpsc::channel::<EventRecord>(1024);
        Pool::new(max_assets, tx)
    }

    #[tokio::test]
    async fn both_assets_of_market_share_one_connection() {
        let mut pool = pool(200);
        pool.subscribe_markets(vec![market("m1", "yes", "no")])
            .await
            .unwrap();
        assert_eq!(pool.asset_to_conn["yes"], pool.asset_to_conn["no"]);
    }

    #[tokio::test]
    async fn connections_do_not_exceed_capacity_or_split_markets() {
        let mut pool = pool(4);
        pool.subscribe_markets(vec![
            market("m1", "a1y", "a1n"),
            market("m2", "a2y", "a2n"),
            market("m3", "a3y", "a3n"),
        ])
        .await
        .unwrap();

        assert_eq!(pool.connection_count(), 2);
        for handle in &pool.connections {
            assert!(handle.assets.len() <= 4);
        }
        for market in pool.markets.values() {
            assert_eq!(
                pool.asset_to_conn[&market.assets[0]],
                pool.asset_to_conn[&market.assets[1]],
            );
        }
    }

    #[tokio::test]
    async fn rejects_capacity_that_cannot_hold_a_whole_market() {
        let mut pool = pool(1);
        assert!(pool
            .subscribe_markets(vec![market("m1", "yes", "no")])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn duplicate_market_is_skipped() {
        let mut pool = pool(200);
        pool.subscribe_markets(vec![market("m1", "a1y", "a1n")])
            .await
            .unwrap();
        let before = pool.connection_count();
        pool.subscribe_markets(vec![market("m1", "a1y", "a1n")])
            .await
            .unwrap();
        assert_eq!(pool.connection_count(), before);
        assert_eq!(pool.subscribed_market_count(), 1);
    }

    #[tokio::test]
    async fn duplicate_market_in_one_capacity_sized_batch_has_one_route() {
        let mut pool = pool(2);
        let duplicate = market("m1", "a1y", "a1n");
        pool.subscribe_markets(vec![duplicate.clone(), duplicate])
            .await
            .unwrap();

        assert_eq!(pool.connection_count(), 1);
        assert_eq!(pool.subscribed_market_count(), 1);
        assert_eq!(pool.connections[0].assets.len(), 2);
    }

    #[tokio::test]
    async fn conflicting_market_or_asset_identity_is_rejected_before_mutation() {
        let mut pool = pool(4);
        let result = pool
            .subscribe_markets(vec![
                market("m1", "shared", "a1n"),
                market("m2", "shared", "a2n"),
            ])
            .await;

        assert!(result.is_err());
        assert_eq!(pool.connection_count(), 0);
        assert_eq!(pool.subscribed_market_count(), 0);

        pool.subscribe_markets(vec![market("m1", "a1y", "a1n")])
            .await
            .unwrap();
        let result = pool
            .subscribe_markets(vec![market("m1", "different-y", "different-n")])
            .await;
        assert!(result.is_err());
        assert_eq!(pool.connection_count(), 1);
        assert_eq!(pool.subscribed_market_count(), 1);
    }

    #[tokio::test]
    async fn unsubscribe_keeps_empty_lifecycle_leader() {
        let mut pool = pool(200);
        pool.subscribe_markets(vec![market("m1", "a1y", "a1n")])
            .await
            .unwrap();
        pool.unsubscribe_markets(vec!["m1".into()]).await.unwrap();
        assert_eq!(pool.subscribed_market_count(), 0);
        assert!(pool.asset_to_conn.is_empty());
        assert_eq!(pool.connection_count(), 1);
        assert!(pool.connections[0].assets.is_empty());
        assert_eq!(pool.lifecycle_conn_id, Some(pool.connections[0].conn_id));
    }

    #[tokio::test]
    async fn start_creates_one_empty_lifecycle_leader() {
        let mut pool = pool(200);
        pool.start().await;
        pool.start().await;

        assert_eq!(pool.connection_count(), 1);
        assert!(pool.connections[0].assets.is_empty());
        assert_eq!(pool.lifecycle_conn_id, Some(pool.connections[0].conn_id));
    }
}
