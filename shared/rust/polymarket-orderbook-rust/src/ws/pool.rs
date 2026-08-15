//! WebSocket connection pool with one authoritative route per market.
//!
//! Both outcome assets of a binary market are always subscribed on the same
//! connection.  This matters because one `price_change` parent can contain
//! updates for both assets: splitting those assets across redundant sockets
//! creates two independently delivered copies whose relative order cannot be
//! merged correctly. V3 chooses correctness over seamless failover. After a
//! reconnect, fresh `book` snapshots replace the full local state.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, ensure, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::events::{Event, Market, MarketLifecycleObservation};
use crate::record::{CollectorContext, EventRecord};
use crate::ws::connection::{Command, ConnStatus, Connection, HealthEvent};

const COMMAND_CHANNEL_SIZE: usize = 64;
const ROUTE_UPDATE_CHANNEL_SIZE: usize = 8_192;
const LIFECYCLE_LISTENER_CONNECTIONS: usize = 3;

enum RouteUpdate {
    Assigned(Vec<(String, usize)>),
    Removed { conn_id: usize, assets: Vec<String> },
}

struct ConnHandle {
    conn_id: usize,
    assets: HashSet<String>,
    lifecycle_listener: bool,
    cmd_tx: mpsc::Sender<Command>,
    join: JoinHandle<Result<()>>,
}

pub struct PoolStats {
    pub market_count: usize,
    pub connection_count: usize,
    pub lifecycle_listener_count: usize,
    pub asset_down_events: u64,
    pub asset_recovery_events: u64,
    pub asset_recoveries: u64,
    pub asset_recovery_latency_us: u64,
    pub asset_recovery_latency_us_max: u64,
    pub conn_down_events: u64,
    pub conns_down: usize,
    pub assets_down: usize,
}

#[derive(Default)]
pub struct HealthCounters {
    pub asset_down_events: AtomicU64,
    recovery: Mutex<RecoveryCounters>,
    pub conn_down_events: AtomicU64,
    pub conns_down: AtomicU64,
    pub assets_down: AtomicU64,
}

#[derive(Default)]
struct RecoveryCounters {
    total: u64,
    window: RecoveryWindow,
}

#[derive(Default)]
struct RecoveryWindow {
    count: u64,
    latency_us: u64,
    latency_us_max: u64,
}

impl HealthCounters {
    fn record_recovery(&self, latency_us: u64) {
        let mut recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        recovery.total = recovery.total.saturating_add(1);
        recovery.window.count = recovery.window.count.saturating_add(1);
        recovery.window.latency_us = recovery.window.latency_us.saturating_add(latency_us);
        recovery.window.latency_us_max = recovery.window.latency_us_max.max(latency_us);
    }

    fn take_recovery_window(&self) -> (u64, RecoveryWindow) {
        let mut recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let total = recovery.total;
        (total, std::mem::take(&mut recovery.window))
    }
}

pub struct Pool {
    max_assets_per_conn: usize,
    event_tx: mpsc::Sender<EventRecord>,
    collector: Arc<CollectorContext>,
    connections: Vec<ConnHandle>,
    /// Stable connection IDs, not vector positions.
    market_to_conn: HashMap<String, usize>,
    asset_to_conn: HashMap<String, usize>,
    next_conn_id: usize,
    /// Stable ID of the lifecycle anchor. It remains alive with no data
    /// assets so lifecycle discovery remains available during cold start.
    lifecycle_conn_id: Option<usize>,
    lifecycle_tx: Option<mpsc::Sender<MarketLifecycleObservation>>,
    health_counters: Arc<HealthCounters>,
    monitor_join: Option<JoinHandle<()>>,
    status_event_tx: mpsc::UnboundedSender<HealthEvent>,
    route_update_tx: mpsc::Sender<RouteUpdate>,
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
        lifecycle_tx: mpsc::Sender<MarketLifecycleObservation>,
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
        lifecycle_tx: Option<mpsc::Sender<MarketLifecycleObservation>>,
    ) -> Self {
        let collector = Arc::new(CollectorContext::with_publisher_generation(
            publisher_generation,
        ));
        let health_counters = Arc::new(HealthCounters::default());
        let (status_event_tx, status_event_rx) = mpsc::unbounded_channel();
        let (route_update_tx, route_update_rx) = mpsc::channel(ROUTE_UPDATE_CHANNEL_SIZE);

        let monitor_join = Some(tokio::spawn(run_health_monitor(
            status_event_rx,
            route_update_rx,
            Arc::clone(&health_counters),
        )));

        Self {
            max_assets_per_conn,
            event_tx,
            collector,
            connections: Vec::new(),
            market_to_conn: HashMap::new(),
            asset_to_conn: HashMap::new(),
            next_conn_id: 0,
            lifecycle_conn_id: None,
            lifecycle_tx,
            health_counters,
            monitor_join,
            status_event_tx,
            route_update_tx,
        }
    }

    /// Ensure the lifecycle anchor exists. This is required even with no
    /// preloaded assets so cold start can receive `new_market` events.
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
        let (asset_recovery_events, recovery_window) = self.health_counters.take_recovery_window();
        PoolStats {
            market_count: self.market_to_conn.len(),
            connection_count: self.connections.len(),
            lifecycle_listener_count: self
                .connections
                .iter()
                .filter(|connection| connection.lifecycle_listener)
                .count(),
            asset_down_events: self
                .health_counters
                .asset_down_events
                .load(Ordering::Relaxed),
            asset_recovery_events,
            asset_recoveries: recovery_window.count,
            asset_recovery_latency_us: recovery_window.latency_us,
            asset_recovery_latency_us_max: recovery_window.latency_us_max,
            conn_down_events: self
                .health_counters
                .conn_down_events
                .load(Ordering::Relaxed),
            conns_down: self.health_counters.conns_down.load(Ordering::Relaxed) as usize,
            assets_down: self.health_counters.assets_down.load(Ordering::Relaxed) as usize,
        }
    }

    /// Assign a sequence and enqueue one centrally accepted lifecycle event.
    pub fn admit_lifecycle(&self, event: Event, timestamp_received_ns: i64) {
        let record = self.collector.record(event, timestamp_received_ns);
        match self.event_tx.try_send(record) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::error!(
                    capacity = self.event_tx.capacity(),
                    max_capacity = self.event_tx.max_capacity(),
                    "[QUEUE-OVERFLOW] lifecycle event channel full, exiting"
                );
                std::process::exit(1);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::error!("lifecycle event sink closed; exiting before losing data");
                std::process::exit(1);
            }
        }
    }

    /// Subscribe validated markets without splitting a market across connections.
    ///
    /// The caller owns condition identity and asset-ownership validation. This
    /// method validates only the routing invariants represented by the pool.
    pub async fn subscribe_markets(&mut self, markets: &[Market]) -> Result<()> {
        ensure!(
            self.max_assets_per_conn >= 2,
            "MAX_ASSETS_PER_CONN must be at least 2 so a market stays atomic"
        );

        // LifecycleState has already validated market identity. The pool only
        // defends its routing invariants before mutating any route.
        let mut new_markets = Vec::new();
        let mut batch_markets = HashSet::new();
        let mut batch_assets = HashSet::new();
        for market in markets {
            if self.market_to_conn.contains_key(&market.hash) {
                continue;
            }
            if !batch_markets.insert(market.hash.as_str()) {
                continue;
            }

            for asset in &market.assets {
                ensure!(
                    !self.asset_to_conn.contains_key(asset),
                    "asset {} is already assigned to another subscribed market",
                    asset,
                );
                ensure!(
                    batch_assets.insert(asset.as_str()),
                    "asset {} is assigned more than once in one routing batch",
                    asset,
                );
            }
            new_markets.push(market);
        }
        if new_markets.is_empty() {
            return Ok(());
        }

        // Preserve caller order. Startup reconciliation deliberately sends
        // recent markets first so their lower-numbered connections establish
        // subscriptions before the long tail of the active universe.

        let mut pending_assets = vec![Vec::new(); self.connections.len()];
        let mut conn_index = 0;
        let mut assigned_routes = Vec::with_capacity(new_markets.len() * 2);
        for market in new_markets {
            // Every market consumes two slots. Once a connection has fewer
            // than two free slots, no later market in this batch can use it.
            while conn_index < self.connections.len()
                && self.connections[conn_index].assets.len() + pending_assets[conn_index].len() + 2
                    > self.max_assets_per_conn
            {
                conn_index += 1;
            }
            if conn_index == self.connections.len() {
                self.spawn_connection().await;
                pending_assets.push(Vec::new());
            }

            let conn_id = self.connections[conn_index].conn_id;
            let assets = &market.assets;

            pending_assets[conn_index].extend(assets.iter().cloned());
            self.market_to_conn.insert(market.hash.clone(), conn_id);
            for asset in assets {
                self.asset_to_conn.insert(asset.clone(), conn_id);
                assigned_routes.push((asset.clone(), conn_id));
            }
        }

        self.send_route_update(RouteUpdate::Assigned(assigned_routes))
            .await;

        let mut command_error = None;
        for (handle, assets) in self.connections.iter_mut().zip(pending_assets) {
            if assets.is_empty() {
                continue;
            }
            let conn_id = handle.conn_id;
            handle.assets.extend(assets.iter().cloned());
            if let Err(error) = handle.cmd_tx.send(Command::Subscribe(assets)).await {
                command_error.get_or_insert_with(|| {
                    anyhow!("send subscribe command to connection {conn_id}: {error}")
                });
            }
        }

        if let Some(error) = command_error {
            return Err(error);
        }

        Ok(())
    }

    pub async fn unsubscribe_market(
        &mut self,
        market_hash: &str,
        assets: &[String; 2],
    ) -> Result<()> {
        let Some(&conn_id) = self.market_to_conn.get(market_hash) else {
            return Ok(());
        };
        for asset in assets {
            ensure!(
                self.asset_to_conn.get(asset) == Some(&conn_id),
                "market {market_hash} asset {asset} is not routed to connection {conn_id}",
            );
        }

        self.market_to_conn.remove(market_hash);
        for asset in assets {
            self.asset_to_conn.remove(asset);
        }
        self.send_route_update(RouteUpdate::Removed {
            conn_id,
            assets: assets.to_vec(),
        })
        .await;

        let mut command_error = None;
        if let Some(handle) = self.connections.iter_mut().find(|h| h.conn_id == conn_id) {
            for asset in assets {
                handle.assets.remove(asset);
            }
            if let Err(error) = handle
                .cmd_tx
                .send(Command::Unsubscribe(assets.to_vec()))
                .await
            {
                command_error = Some(anyhow!(
                    "send unsubscribe command to connection {conn_id}: {error}"
                ));
            }
        }

        let mut index = 0;
        while index < self.connections.len() {
            if self.connections[index].assets.is_empty()
                && !self.connections[index].lifecycle_listener
            {
                let handle = self.connections.swap_remove(index);
                drop(handle.cmd_tx);
                handle.join.abort();
            } else {
                index += 1;
            }
        }

        if let Some(error) = command_error {
            return Err(error);
        }

        Ok(())
    }

    pub async fn shutdown(mut self) -> Result<()> {
        info!(connections = self.connections.len(), "shutting down pool");
        // Connections may be sleeping in a reconnect backoff or waiting for a
        // bounded handshake. They own no buffered records, so cancel every
        // producer before awaiting any of them.
        self.abort_tasks();

        if let Some(join) = self.monitor_join.take() {
            let _ = join.await;
        }

        for handle in self.connections.drain(..) {
            let conn_id = handle.conn_id;
            drop(handle.cmd_tx);
            match handle.join.await {
                Ok(Ok(())) => debug!(conn = conn_id, "connection joined"),
                Ok(Err(error)) => warn!(conn = conn_id, %error, "connection ended with error"),
                Err(error) if error.is_cancelled() => {
                    debug!(conn = conn_id, "connection cancelled")
                }
                Err(error) => warn!(conn = conn_id, %error, "connection panicked"),
            }
        }
        Ok(())
    }

    fn abort_tasks(&mut self) {
        if let Some(join) = &self.monitor_join {
            join.abort();
        }
        for handle in &self.connections {
            handle.join.abort();
        }
    }

    async fn spawn_connection(&mut self) {
        let conn_id = self.next_conn_id;
        self.next_conn_id += 1;
        let lifecycle_anchor = self.lifecycle_conn_id.is_none();
        if lifecycle_anchor {
            self.lifecycle_conn_id = Some(conn_id);
        }
        let lifecycle_listener = lifecycle_anchor
            || (self.lifecycle_tx.is_some()
                && self.connections.len() < LIFECYCLE_LISTENER_CONNECTIONS);
        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_SIZE);
        let connection = Connection::new(
            conn_id,
            self.event_tx.clone(),
            Arc::clone(&self.collector),
            self.status_event_tx.clone(),
            lifecycle_listener,
            self.lifecycle_tx.clone(),
        );
        let join = tokio::spawn(connection.run(cmd_rx));
        self.connections.push(ConnHandle {
            conn_id,
            assets: HashSet::new(),
            lifecycle_listener,
            cmd_tx,
            join,
        });
    }

    async fn send_route_update(&self, update: RouteUpdate) {
        if self.route_update_tx.send(update).await.is_err() {
            warn!("pool health monitor route channel closed");
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}

async fn run_health_monitor(
    mut status_rx: mpsc::UnboundedReceiver<HealthEvent>,
    mut route_update_rx: mpsc::Receiver<RouteUpdate>,
    counters: Arc<HealthCounters>,
) {
    let mut state = HealthState::default();

    loop {
        tokio::select! {
            biased;
            update = route_update_rx.recv() => {
                let Some(update) = update else { break };
                state.apply_route_update(update, &counters);
            }
            event = status_rx.recv() => {
                let Some(event) = event else { break };
                state.apply_event(event, &counters);
            }
        }
    }
}

#[derive(Default)]
struct HealthState {
    connections: HashMap<usize, ConnHealth>,
    assets_down_count: usize,
    conns_down_count: usize,
}

struct ConnHealth {
    status: ConnStatus,
    generation: u64,
    assets: HashMap<String, AssetHealth>,
}

impl Default for ConnHealth {
    fn default() -> Self {
        Self {
            status: ConnStatus::Disconnected,
            generation: 0,
            assets: HashMap::new(),
        }
    }
}

#[derive(Default)]
struct AssetHealth {
    ready_generation: Option<u64>,
    down_since: Option<Instant>,
}

impl ConnHealth {
    fn asset_is_ready(&self, asset: &AssetHealth) -> bool {
        self.status == ConnStatus::Connected && asset.ready_generation == Some(self.generation)
    }

    fn ready_asset_count(&self) -> usize {
        self.assets
            .values()
            .filter(|asset| self.asset_is_ready(asset))
            .count()
    }
}

impl HealthState {
    fn apply_event(&mut self, event: HealthEvent, counters: &HealthCounters) {
        match event {
            HealthEvent::Connection { conn_id, status } => {
                let connection = self.connections.entry(conn_id).or_default();
                let old_status = connection.status;
                if old_status == status {
                    return;
                }

                let assigned_assets = connection.assets.len();
                let was_down = assigned_assets != 0 && old_status == ConnStatus::Disconnected;
                let ready_before = connection.ready_asset_count();

                if status == ConnStatus::Disconnected {
                    counters.conn_down_events.fetch_add(1, Ordering::Relaxed);
                    let now = Instant::now();
                    let generation = connection.generation;
                    for asset in connection
                        .assets
                        .values_mut()
                        .filter(|asset| asset.ready_generation == Some(generation))
                    {
                        counters.asset_down_events.fetch_add(1, Ordering::Relaxed);
                        asset.down_since = Some(now);
                    }
                    if ready_before != 0 {
                        warn!(
                            conn = conn_id,
                            invalidated_assets = ready_before,
                            assigned_assets,
                            "[CONNECTION-DATA-GAP] authoritative connection down"
                        );
                    }
                } else {
                    connection.generation = connection.generation.wrapping_add(1);
                }
                connection.status = status;

                let ready_after = connection.ready_asset_count();
                self.assets_down_count =
                    adjust_down_count(self.assets_down_count, ready_before, ready_after);
                let is_down = assigned_assets != 0 && status == ConnStatus::Disconnected;
                self.conns_down_count =
                    adjust_boolean_count(self.conns_down_count, was_down, is_down);
                self.publish_gauges(counters);
            }
            HealthEvent::BookSnapshot { conn_id, asset_id } => {
                let authoritative = self
                    .connections
                    .get(&conn_id)
                    .is_some_and(|connection| connection.assets.contains_key(&asset_id));
                if !authoritative {
                    if let Some(expected_conn) = self.connection_for_asset(&asset_id) {
                        warn!(
                            asset = asset_id,
                            conn = conn_id,
                            expected_conn,
                            "ignoring book readiness from non-authoritative connection"
                        );
                    }
                    return;
                }
                let (became_ready, recovery_us) = {
                    let connection = self
                        .connections
                        .get_mut(&conn_id)
                        .expect("authoritative connection must exist");
                    let status = connection.status;
                    let generation = connection.generation;
                    let asset = connection
                        .assets
                        .get_mut(&asset_id)
                        .expect("authoritative asset must exist");
                    let was_ready = status == ConnStatus::Connected
                        && asset.ready_generation == Some(generation);
                    asset.ready_generation = Some(generation);
                    let became_ready = !was_ready && status == ConnStatus::Connected;
                    let recovery_us = became_ready
                        .then(|| asset.down_since.take())
                        .flatten()
                        .map(|started| started.elapsed().as_micros().min(u64::MAX as u128) as u64);
                    (became_ready, recovery_us)
                };
                // Snapshots are the hot path at startup and after a batch
                // reconnect. Updating this one asset must remain O(1), not
                // scan the complete subscription universe.
                if became_ready {
                    self.assets_down_count = self.assets_down_count.saturating_sub(1);
                    self.publish_gauges(counters);
                }
                if let Some(recovery_us) = recovery_us {
                    counters.record_recovery(recovery_us);
                }
            }
        }
    }

    fn apply_route_update(&mut self, update: RouteUpdate, counters: &HealthCounters) {
        match update {
            RouteUpdate::Assigned(routes) => {
                for (asset, conn_id) in routes {
                    // Pool routing validates ownership before emitting this
                    // update; the monitor only projects the accepted route.
                    let connection = self.connections.entry(conn_id).or_default();
                    if connection.assets.contains_key(&asset) {
                        continue;
                    }
                    let conn_was_empty = connection.assets.is_empty();
                    connection.assets.insert(asset, AssetHealth::default());
                    self.assets_down_count = self.assets_down_count.saturating_add(1);
                    if conn_was_empty && connection.status != ConnStatus::Connected {
                        self.conns_down_count = self.conns_down_count.saturating_add(1);
                    }
                }
            }
            RouteUpdate::Removed { conn_id, assets } => {
                let Some(connection) = self.connections.get_mut(&conn_id) else {
                    self.publish_gauges(counters);
                    return;
                };
                let conn_was_empty = connection.assets.is_empty();
                for asset in assets {
                    let Some(asset_health) = connection.assets.remove(&asset) else {
                        continue;
                    };
                    if !connection.asset_is_ready(&asset_health) {
                        self.assets_down_count = self.assets_down_count.saturating_sub(1);
                    }
                }
                if !conn_was_empty
                    && connection.assets.is_empty()
                    && connection.status == ConnStatus::Disconnected
                {
                    self.conns_down_count = self.conns_down_count.saturating_sub(1);
                }
            }
        }
        self.publish_gauges(counters);
    }

    fn connection_for_asset(&self, asset: &str) -> Option<usize> {
        self.connections.iter().find_map(|(conn_id, connection)| {
            connection.assets.contains_key(asset).then_some(*conn_id)
        })
    }

    fn publish_gauges(&self, counters: &HealthCounters) {
        counters
            .conns_down
            .store(self.conns_down_count as u64, Ordering::Relaxed);
        counters
            .assets_down
            .store(self.assets_down_count as u64, Ordering::Relaxed);
    }
}

fn adjust_down_count(current: usize, ready_before: usize, ready_after: usize) -> usize {
    if ready_before >= ready_after {
        current.saturating_add(ready_before - ready_after)
    } else {
        current.saturating_sub(ready_after - ready_before)
    }
}

fn adjust_boolean_count(current: usize, before: bool, after: bool) -> usize {
    match (before, after) {
        (false, true) => current.saturating_add(1),
        (true, false) => current.saturating_sub(1),
        _ => current,
    }
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

    fn add_test_connection(pool: &mut Pool, conn_id: usize) -> mpsc::Receiver<Command> {
        let (cmd_tx, cmd_rx) = mpsc::channel(COMMAND_CHANNEL_SIZE);
        let join = tokio::spawn(std::future::pending::<Result<()>>());
        pool.connections.push(ConnHandle {
            conn_id,
            assets: HashSet::new(),
            lifecycle_listener: true,
            cmd_tx,
            join,
        });
        pool.next_conn_id = conn_id.saturating_add(1);
        pool.lifecycle_conn_id = Some(conn_id);
        cmd_rx
    }

    #[test]
    fn asset_recovers_only_after_a_fresh_book_snapshot() {
        let counters = HealthCounters::default();
        let mut state = HealthState::default();
        state.apply_route_update(RouteUpdate::Assigned(vec![("asset".into(), 7)]), &counters);

        state.apply_event(
            HealthEvent::Connection {
                conn_id: 7,
                status: ConnStatus::Connected,
            },
            &counters,
        );
        assert_eq!(counters.conns_down.load(Ordering::Relaxed), 0);
        assert_eq!(counters.assets_down.load(Ordering::Relaxed), 1);

        state.apply_event(
            HealthEvent::BookSnapshot {
                conn_id: 7,
                asset_id: "asset".into(),
            },
            &counters,
        );
        assert_eq!(counters.assets_down.load(Ordering::Relaxed), 0);

        state.apply_event(
            HealthEvent::Connection {
                conn_id: 7,
                status: ConnStatus::Disconnected,
            },
            &counters,
        );
        assert_eq!(counters.asset_down_events.load(Ordering::Relaxed), 1);
        assert_eq!(counters.assets_down.load(Ordering::Relaxed), 1);

        state.apply_event(
            HealthEvent::Connection {
                conn_id: 7,
                status: ConnStatus::Connected,
            },
            &counters,
        );
        assert_eq!(
            counters.assets_down.load(Ordering::Relaxed),
            1,
            "a new TCP session is not data recovery"
        );

        state.apply_event(
            HealthEvent::BookSnapshot {
                conn_id: 7,
                asset_id: "asset".into(),
            },
            &counters,
        );
        assert_eq!(counters.assets_down.load(Ordering::Relaxed), 0);
        let (recovery_events, recovery_window) = counters.take_recovery_window();
        assert_eq!(recovery_events, 1);
        assert_eq!(recovery_window.count, 1);
        assert!(state.connections[&7].assets["asset"].down_since.is_none());
    }

    #[test]
    fn snapshot_while_disconnected_does_not_end_recovery() {
        let counters = HealthCounters::default();
        let mut state = HealthState::default();
        state.apply_route_update(RouteUpdate::Assigned(vec![("asset".into(), 7)]), &counters);
        state.apply_event(
            HealthEvent::Connection {
                conn_id: 7,
                status: ConnStatus::Connected,
            },
            &counters,
        );
        state.apply_event(
            HealthEvent::BookSnapshot {
                conn_id: 7,
                asset_id: "asset".into(),
            },
            &counters,
        );
        state.apply_event(
            HealthEvent::Connection {
                conn_id: 7,
                status: ConnStatus::Disconnected,
            },
            &counters,
        );

        state.apply_event(
            HealthEvent::BookSnapshot {
                conn_id: 7,
                asset_id: "asset".into(),
            },
            &counters,
        );
        assert!(state.connections[&7].assets["asset"].down_since.is_some());
        let (recovery_events, recovery_window) = counters.take_recovery_window();
        assert_eq!(recovery_events, 0);
        assert_eq!(recovery_window.count, 0);

        state.apply_event(
            HealthEvent::Connection {
                conn_id: 7,
                status: ConnStatus::Connected,
            },
            &counters,
        );
        state.apply_event(
            HealthEvent::BookSnapshot {
                conn_id: 7,
                asset_id: "asset".into(),
            },
            &counters,
        );
        assert!(state.connections[&7].assets["asset"].down_since.is_none());
        let (recovery_events, recovery_window) = counters.take_recovery_window();
        assert_eq!(recovery_events, 1);
        assert_eq!(recovery_window.count, 1);
    }

    #[test]
    fn removing_an_unready_route_updates_health_gauges() {
        let counters = HealthCounters::default();
        let mut state = HealthState::default();
        state.apply_route_update(RouteUpdate::Assigned(vec![("asset".into(), 7)]), &counters);

        assert_eq!(counters.conns_down.load(Ordering::Relaxed), 1);
        assert_eq!(counters.assets_down.load(Ordering::Relaxed), 1);

        state.apply_route_update(
            RouteUpdate::Removed {
                conn_id: 7,
                assets: vec!["asset".into()],
            },
            &counters,
        );

        assert_eq!(counters.conns_down.load(Ordering::Relaxed), 0);
        assert_eq!(counters.assets_down.load(Ordering::Relaxed), 0);
        assert!(state.connections[&7].assets.is_empty());
    }

    #[test]
    fn removing_a_route_uses_its_stable_connection_id() {
        let counters = HealthCounters::default();
        let mut state = HealthState::default();
        state.apply_route_update(RouteUpdate::Assigned(vec![("asset".into(), 7)]), &counters);

        state.apply_route_update(
            RouteUpdate::Removed {
                conn_id: 8,
                assets: vec!["asset".into()],
            },
            &counters,
        );

        assert!(state.connections[&7].assets.contains_key("asset"));
        assert_eq!(counters.conns_down.load(Ordering::Relaxed), 1);
        assert_eq!(counters.assets_down.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn removed_asset_can_be_reassigned_without_stale_readiness() {
        let counters = HealthCounters::default();
        let mut state = HealthState::default();
        state.apply_route_update(RouteUpdate::Assigned(vec![("asset".into(), 7)]), &counters);
        state.apply_event(
            HealthEvent::Connection {
                conn_id: 7,
                status: ConnStatus::Connected,
            },
            &counters,
        );
        state.apply_event(
            HealthEvent::BookSnapshot {
                conn_id: 7,
                asset_id: "asset".into(),
            },
            &counters,
        );
        state.apply_route_update(
            RouteUpdate::Removed {
                conn_id: 7,
                assets: vec!["asset".into()],
            },
            &counters,
        );
        state.apply_route_update(RouteUpdate::Assigned(vec![("asset".into(), 8)]), &counters);

        state.apply_event(
            HealthEvent::BookSnapshot {
                conn_id: 7,
                asset_id: "asset".into(),
            },
            &counters,
        );
        assert_eq!(counters.conns_down.load(Ordering::Relaxed), 1);
        assert_eq!(counters.assets_down.load(Ordering::Relaxed), 1);

        state.apply_event(
            HealthEvent::Connection {
                conn_id: 8,
                status: ConnStatus::Connected,
            },
            &counters,
        );
        state.apply_event(
            HealthEvent::BookSnapshot {
                conn_id: 8,
                asset_id: "asset".into(),
            },
            &counters,
        );
        assert_eq!(counters.conns_down.load(Ordering::Relaxed), 0);
        assert_eq!(counters.assets_down.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn snapshot_from_non_authoritative_connection_does_not_recover_asset() {
        let counters = HealthCounters::default();
        let mut state = HealthState::default();
        state.apply_route_update(RouteUpdate::Assigned(vec![("asset".into(), 7)]), &counters);
        state.apply_event(
            HealthEvent::Connection {
                conn_id: 7,
                status: ConnStatus::Connected,
            },
            &counters,
        );

        state.apply_event(
            HealthEvent::BookSnapshot {
                conn_id: 8,
                asset_id: "asset".into(),
            },
            &counters,
        );

        assert_eq!(counters.assets_down.load(Ordering::Relaxed), 1);
        assert_eq!(state.connections[&7].assets["asset"].ready_generation, None);
    }

    #[test]
    fn recovery_window_is_taken_as_one_consistent_interval() {
        let counters = HealthCounters::default();
        counters.record_recovery(725);
        counters.record_recovery(125);

        let (total, window) = counters.take_recovery_window();
        assert_eq!(total, 2);
        assert_eq!(window.count, 2);
        assert_eq!(window.latency_us, 850);
        assert_eq!(window.latency_us_max, 725);
        assert!(window.latency_us <= window.latency_us_max * window.count);

        let (total, next_window) = counters.take_recovery_window();
        assert_eq!(total, 2);
        assert_eq!(next_window.count, 0);
        assert_eq!(next_window.latency_us, 0);
        assert_eq!(next_window.latency_us_max, 0);
    }

    #[tokio::test]
    async fn both_assets_of_market_share_one_connection() {
        let mut pool = pool(200);
        pool.subscribe_markets(&[market("m1", "yes", "no")])
            .await
            .unwrap();
        assert_eq!(pool.asset_to_conn["yes"], pool.asset_to_conn["no"]);
    }

    #[tokio::test]
    async fn subscribe_fails_when_connection_command_channel_is_closed() {
        let mut pool = pool(2);
        let commands = add_test_connection(&mut pool, 41);
        drop(commands);
        let subscribed = market("m1", "yes", "no");

        let error = pool
            .subscribe_markets(std::slice::from_ref(&subscribed))
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("send subscribe command to connection 41"));
        assert_eq!(pool.market_to_conn[&subscribed.hash], 41);
        assert_eq!(pool.asset_to_conn[&subscribed.assets[0]], 41);
        assert_eq!(pool.asset_to_conn[&subscribed.assets[1]], 41);
        assert_eq!(pool.connections[0].assets.len(), 2);
    }

    #[tokio::test]
    async fn connections_do_not_exceed_capacity_or_split_markets() {
        let mut pool = pool(4);
        let markets = [
            market("m1", "a1y", "a1n"),
            market("m2", "a2y", "a2n"),
            market("m3", "a3y", "a3n"),
        ];
        pool.subscribe_markets(&markets).await.unwrap();

        assert_eq!(pool.connection_count(), 2);
        for handle in &pool.connections {
            assert!(handle.assets.len() <= 4);
        }
        for market in &markets {
            assert_eq!(
                pool.asset_to_conn[&market.assets[0]],
                pool.asset_to_conn[&market.assets[1]],
            );
        }
    }

    #[tokio::test]
    async fn preload_partition_preserves_subscription_priority() {
        let mut pool = pool(4);
        pool.subscribe_markets(&[
            market("z", "zy", "zn"),
            market("a", "ay", "an"),
            market("y", "yy", "yn"),
            market("b", "by", "bn"),
        ])
        .await
        .unwrap();

        assert_eq!(pool.market_to_conn["z"], pool.market_to_conn["a"]);
        assert_eq!(pool.market_to_conn["y"], pool.market_to_conn["b"]);
        assert!(pool.market_to_conn["z"] < pool.market_to_conn["y"]);
    }

    #[tokio::test]
    async fn later_batch_fills_existing_capacity_before_spawning() {
        let mut pool = pool(6);
        pool.subscribe_markets(&[market("m1", "a1y", "a1n"), market("m2", "a2y", "a2n")])
            .await
            .unwrap();
        let first_conn = pool.market_to_conn["m1"];

        pool.subscribe_markets(&[
            market("m3", "a3y", "a3n"),
            market("m4", "a4y", "a4n"),
            market("m5", "a5y", "a5n"),
        ])
        .await
        .unwrap();

        assert_eq!(pool.connection_count(), 2);
        assert_eq!(pool.market_to_conn["m3"], first_conn);
        assert_ne!(pool.market_to_conn["m4"], first_conn);
        assert_eq!(pool.market_to_conn["m4"], pool.market_to_conn["m5"]);
    }

    #[tokio::test]
    async fn allocation_after_swap_remove_keeps_stable_connection_id() {
        let mut pool = pool(4);
        let markets = [
            market("m1", "a1y", "a1n"),
            market("m2", "a2y", "a2n"),
            market("m3", "a3y", "a3n"),
            market("m4", "a4y", "a4n"),
            market("m5", "a5y", "a5n"),
        ];
        pool.subscribe_markets(&markets).await.unwrap();
        let last_conn = pool.market_to_conn["m5"];

        for market in &markets[2..4] {
            pool.unsubscribe_market(&market.hash, &market.assets)
                .await
                .unwrap();
        }
        pool.subscribe_markets(&[market("m6", "a6y", "a6n")])
            .await
            .unwrap();

        assert_eq!(pool.connection_count(), 2);
        assert_eq!(pool.market_to_conn["m6"], last_conn);
        assert!(pool.connections.iter().any(|handle| {
            handle.conn_id == last_conn
                && handle.assets.contains("a5y")
                && handle.assets.contains("a6y")
        }));
    }

    #[tokio::test]
    async fn rejects_capacity_that_cannot_hold_a_whole_market() {
        let mut pool = pool(1);
        assert!(pool
            .subscribe_markets(&[market("m1", "yes", "no")])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn duplicate_market_is_skipped() {
        let mut pool = pool(200);
        pool.subscribe_markets(&[market("m1", "a1y", "a1n")])
            .await
            .unwrap();
        let before = pool.connection_count();
        pool.subscribe_markets(&[market("m1", "a1y", "a1n")])
            .await
            .unwrap();
        assert_eq!(pool.connection_count(), before);
        assert_eq!(pool.market_to_conn.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_market_in_one_capacity_sized_batch_has_one_route() {
        let mut pool = pool(2);
        let duplicate = market("m1", "a1y", "a1n");
        pool.subscribe_markets(&[duplicate.clone(), duplicate])
            .await
            .unwrap();

        assert_eq!(pool.connection_count(), 1);
        assert_eq!(pool.market_to_conn.len(), 1);
        assert_eq!(pool.connections[0].assets.len(), 2);
    }

    #[tokio::test]
    async fn asset_collision_is_rejected_before_routing() {
        let mut pool = pool(4);
        let result = pool
            .subscribe_markets(&[market("m1", "shared", "a1n"), market("m2", "shared", "a2n")])
            .await;

        assert!(result.is_err());
        assert_eq!(pool.connection_count(), 0);
        assert_eq!(pool.market_to_conn.len(), 0);
    }

    #[tokio::test]
    async fn unsubscribe_keeps_empty_lifecycle_anchor() {
        let mut pool = pool(200);
        let subscribed = market("m1", "a1y", "a1n");
        pool.subscribe_markets(std::slice::from_ref(&subscribed))
            .await
            .unwrap();
        pool.unsubscribe_market(&subscribed.hash, &subscribed.assets)
            .await
            .unwrap();
        assert_eq!(pool.market_to_conn.len(), 0);
        assert!(pool.asset_to_conn.is_empty());
        assert_eq!(pool.connection_count(), 1);
        assert!(pool.connections[0].assets.is_empty());
        assert_eq!(pool.lifecycle_conn_id, Some(pool.connections[0].conn_id));
    }

    #[tokio::test]
    async fn unsubscribe_fails_when_connection_command_channel_is_closed() {
        let mut pool = pool(2);
        let mut commands = add_test_connection(&mut pool, 42);
        let subscribed = market("m1", "a1y", "a1n");
        pool.subscribe_markets(std::slice::from_ref(&subscribed))
            .await
            .unwrap();
        assert!(matches!(commands.recv().await, Some(Command::Subscribe(_))));
        drop(commands);

        let error = pool
            .unsubscribe_market(&subscribed.hash, &subscribed.assets)
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("send unsubscribe command to connection 42"));
        assert!(!pool.market_to_conn.contains_key(&subscribed.hash));
        assert!(!pool.asset_to_conn.contains_key(&subscribed.assets[0]));
        assert!(!pool.asset_to_conn.contains_key(&subscribed.assets[1]));
        assert!(pool.connections[0].assets.is_empty());
    }

    #[tokio::test]
    async fn unsubscribe_rejects_mismatched_assets_before_mutation() {
        let mut pool = pool(200);
        let subscribed = market("m1", "a1y", "a1n");
        pool.subscribe_markets(std::slice::from_ref(&subscribed))
            .await
            .unwrap();

        let mismatched = ["a1y".into(), "other".into()];
        assert!(pool
            .unsubscribe_market(&subscribed.hash, &mismatched)
            .await
            .is_err());
        assert_eq!(pool.market_to_conn.len(), 1);
        assert_eq!(pool.asset_to_conn.len(), 2);
        assert_eq!(pool.connections[0].assets.len(), 2);
    }

    #[tokio::test]
    async fn start_creates_one_empty_lifecycle_anchor() {
        let mut pool = pool(200);
        pool.start().await;
        pool.start().await;

        assert_eq!(pool.connection_count(), 1);
        assert!(pool.connections[0].assets.is_empty());
        assert_eq!(pool.lifecycle_conn_id, Some(pool.connections[0].conn_id));
    }

    #[tokio::test]
    async fn first_three_connections_listen_for_global_lifecycle_events() {
        let (event_tx, _event_rx) = mpsc::channel(8);
        let (lifecycle_tx, _lifecycle_rx) = mpsc::channel(8);
        let mut pool = Pool::new_with_lifecycle(2, event_tx, 0, lifecycle_tx);

        let markets = [
            market("m1", "a1y", "a1n"),
            market("m2", "a2y", "a2n"),
            market("m3", "a3y", "a3n"),
            market("m4", "a4y", "a4n"),
        ];
        pool.subscribe_markets(&markets).await.unwrap();

        assert_eq!(pool.connection_count(), 4);
        assert!(pool.connections[0].lifecycle_listener);
        assert!(pool.connections[1].lifecycle_listener);
        assert!(pool.connections[2].lifecycle_listener);
        assert!(!pool.connections[3].lifecycle_listener);

        for market in &markets[..3] {
            pool.unsubscribe_market(&market.hash, &market.assets)
                .await
                .unwrap();
        }
        assert_eq!(pool.connection_count(), 4);
        assert_eq!(pool.pool_stats().lifecycle_listener_count, 3);

        pool.unsubscribe_market(&markets[3].hash, &markets[3].assets)
            .await
            .unwrap();
        assert_eq!(pool.connection_count(), 3);
        assert!(pool
            .connections
            .iter()
            .all(|connection| connection.lifecycle_listener));
    }

    #[tokio::test]
    async fn shutdown_cancels_connections_before_waiting_for_them() {
        let mut pool = pool(2);
        let (cmd_tx, _cmd_rx) = mpsc::channel(COMMAND_CHANNEL_SIZE);
        let join = tokio::spawn(std::future::pending::<Result<()>>());
        pool.connections.push(ConnHandle {
            conn_id: 0,
            assets: HashSet::new(),
            lifecycle_listener: true,
            cmd_tx,
            join,
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), pool.shutdown())
            .await
            .expect("pool shutdown should not wait for a connection task")
            .unwrap();
    }

    #[tokio::test]
    async fn drop_cancels_monitor_and_connection_tasks() {
        struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(signal) = self.0.take() {
                    let _ = signal.send(());
                }
            }
        }

        let mut pool = pool(2);
        let original_monitor = pool.monitor_join.take().unwrap();
        original_monitor.abort();
        let _ = original_monitor.await;

        let (monitor_started_tx, monitor_started_rx) = tokio::sync::oneshot::channel();
        let (monitor_dropped_tx, monitor_dropped_rx) = tokio::sync::oneshot::channel();
        pool.monitor_join = Some(tokio::spawn(async move {
            let _drop_signal = DropSignal(Some(monitor_dropped_tx));
            let _ = monitor_started_tx.send(());
            std::future::pending::<()>().await;
        }));

        let (connection_started_tx, connection_started_rx) = tokio::sync::oneshot::channel();
        let (connection_dropped_tx, connection_dropped_rx) = tokio::sync::oneshot::channel();
        let connection_join = tokio::spawn(async move {
            let _drop_signal = DropSignal(Some(connection_dropped_tx));
            let _ = connection_started_tx.send(());
            std::future::pending::<Result<()>>().await
        });
        let (cmd_tx, _cmd_rx) = mpsc::channel(COMMAND_CHANNEL_SIZE);
        pool.connections.push(ConnHandle {
            conn_id: 0,
            assets: HashSet::new(),
            lifecycle_listener: true,
            cmd_tx,
            join: connection_join,
        });

        monitor_started_rx.await.unwrap();
        connection_started_rx.await.unwrap();
        drop(pool);

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            monitor_dropped_rx.await.unwrap();
            connection_dropped_rx.await.unwrap();
        })
        .await
        .expect("dropping the pool should cancel all owned tasks");
    }
}
