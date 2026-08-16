//! WebSocket connection pool with one authoritative route per market.
//!
//! Both outcome assets of a binary market are always subscribed on the same
//! connection.  This matters because one `price_change` parent can contain
//! updates for both assets: splitting those assets across redundant sockets
//! creates two independently delivered copies whose relative order cannot be
//! merged correctly. V3 chooses correctness over seamless failover. After a
//! reconnect, fresh `book` snapshots replace the full local state.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::events::{Event, Market, MarketLifecycleObservation};
use crate::record::{CollectorContext, EventRecord};
use crate::ws::connection::{Command, ConnMetrics, Connection, HealthCounters};

const COMMAND_CHANNEL_SIZE: usize = 64;
const LIFECYCLE_LISTENER_CONNECTIONS: usize = 3;

struct ConnHandle {
    conn_id: usize,
    /// Number of assets routed here. Exact membership remains authoritative
    /// in `Pool::asset_to_conn`.
    asset_count: usize,
    metrics: Arc<ConnMetrics>,
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
        let conns_down = self
            .connections
            .iter()
            .filter(|connection| connection.asset_count > 0 && !connection.metrics.is_connected())
            .count();
        let assets_down = self
            .connections
            .iter()
            .map(|connection| {
                connection
                    .asset_count
                    .saturating_sub(connection.metrics.ready_assets())
            })
            .sum();
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
            conns_down,
            assets_down,
        }
    }

    /// Assign a sequence and enqueue one centrally accepted lifecycle event.
    pub fn admit_lifecycle(&self, event: Event, timestamp_received_ns: i64) -> Result<()> {
        let record = self.collector.record(event, timestamp_received_ns);
        match self.event_tx.try_send(record) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(anyhow!(
                "lifecycle event sink queue is full (available {}, maximum {}); stopping after rejected event",
                self.event_tx.capacity(),
                self.event_tx.max_capacity(),
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow!(
                    "lifecycle event sink is closed; stopping after rejected event"
                ))
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
        for market in new_markets {
            // Every market consumes two slots. Once a connection has fewer
            // than two free slots, no later market in this batch can use it.
            while conn_index < self.connections.len()
                && self.connections[conn_index].asset_count + pending_assets[conn_index].len() + 2
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
            }
        }

        let mut command_error = None;
        for (handle, assets) in self.connections.iter_mut().zip(pending_assets) {
            if assets.is_empty() {
                continue;
            }
            let conn_id = handle.conn_id;
            handle.asset_count += assets.len();
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
        ensure!(
            assets[0] != assets[1],
            "market {market_hash} cannot unsubscribe the same asset twice"
        );
        for asset in assets {
            ensure!(
                self.asset_to_conn.get(asset) == Some(&conn_id),
                "market {market_hash} asset {asset} is not routed to connection {conn_id}",
            );
        }

        let handle_index = self
            .connections
            .iter()
            .position(|handle| handle.conn_id == conn_id)
            .ok_or_else(|| {
                anyhow!("market {market_hash} routes to missing connection {conn_id}")
            })?;
        let remaining_asset_count = self.connections[handle_index]
            .asset_count
            .checked_sub(assets.len())
            .ok_or_else(|| {
                anyhow!(
                    "connection {conn_id} has {} assigned assets, cannot remove {} for market {market_hash}",
                    self.connections[handle_index].asset_count,
                    assets.len(),
                )
            })?;

        self.market_to_conn.remove(market_hash);
        for asset in assets {
            self.asset_to_conn.remove(asset);
        }
        let (command_error, remove_connection) = {
            let handle = &mut self.connections[handle_index];
            handle.asset_count = remaining_asset_count;
            let command_error = handle
                .cmd_tx
                .send(Command::Unsubscribe(assets.to_vec()))
                .await
                .err()
                .map(|error| anyhow!("send unsubscribe command to connection {conn_id}: {error}"));
            (
                command_error,
                handle.asset_count == 0 && !handle.lifecycle_listener,
            )
        };
        if remove_connection {
            let handle = self.connections.swap_remove(handle_index);
            drop(handle.cmd_tx);
            handle.join.abort();
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
        let metrics = Arc::new(ConnMetrics::default());
        let connection = Connection::new(
            conn_id,
            self.event_tx.clone(),
            Arc::clone(&self.collector),
            Arc::clone(&metrics),
            Arc::clone(&self.health_counters),
            lifecycle_listener,
            self.lifecycle_tx.clone(),
        );
        let join = tokio::spawn(connection.run(cmd_rx));
        self.connections.push(ConnHandle {
            conn_id,
            asset_count: 0,
            metrics,
            lifecycle_listener,
            cmd_tx,
            join,
        });
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market(hash: &str, yes: &str, no: &str) -> Market {
        Market::new(hash.into(), yes.into(), no.into())
    }

    fn lifecycle_event(market: &str) -> Event {
        Event::NewMarket {
            id: "1".into(),
            market: market.into(),
            timestamp: "1".into(),
            assets_ids: vec![format!("{market}-yes"), format!("{market}-no")],
            outcomes: vec!["Yes".into(), "No".into()],
            question: None,
            slug: None,
        }
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
            asset_count: 0,
            metrics: Arc::new(ConnMetrics::default()),
            lifecycle_listener: true,
            cmd_tx,
            join,
        });
        pool.next_conn_id = conn_id.saturating_add(1);
        pool.lifecycle_conn_id = Some(conn_id);
        cmd_rx
    }

    #[tokio::test]
    async fn lifecycle_admission_reports_a_full_sink_queue() {
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let pool = Pool::new(2, event_tx);

        pool.admit_lifecycle(lifecycle_event("first"), 1).unwrap();
        let error = pool
            .admit_lifecycle(lifecycle_event("second"), 2)
            .unwrap_err();

        assert!(error.to_string().contains("sink queue is full"));
        assert_eq!(event_rx.recv().await.unwrap().timestamp_received_ns, 1);
    }

    #[tokio::test]
    async fn lifecycle_admission_reports_a_closed_sink_queue() {
        let (event_tx, event_rx) = mpsc::channel(1);
        drop(event_rx);
        let pool = Pool::new(2, event_tx);

        let error = pool
            .admit_lifecycle(lifecycle_event("closed"), 1)
            .unwrap_err();

        assert!(error.to_string().contains("sink is closed"));
    }

    #[tokio::test]
    async fn stats_include_never_initialized_assignments_and_remove_routes_immediately() {
        let mut pool = pool(2);
        let _commands = add_test_connection(&mut pool, 7);
        let subscribed = market("m1", "yes", "no");

        pool.subscribe_markets(std::slice::from_ref(&subscribed))
            .await
            .unwrap();
        let stats = pool.pool_stats();
        assert_eq!(stats.conns_down, 1);
        assert_eq!(stats.assets_down, 2);

        pool.unsubscribe_market(&subscribed.hash, &subscribed.assets)
            .await
            .unwrap();
        let stats = pool.pool_stats();
        assert_eq!(stats.conns_down, 0);
        assert_eq!(stats.assets_down, 0);
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
        assert_eq!(pool.connections[0].asset_count, 2);
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
            assert!(handle.asset_count <= 4);
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
        assert_eq!(pool.connections[0].asset_count, 6);
        assert_eq!(pool.connections[1].asset_count, 4);
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
        assert_eq!(pool.asset_to_conn["a5y"], last_conn);
        assert_eq!(pool.asset_to_conn["a6y"], last_conn);
        assert_eq!(
            pool.connections
                .iter()
                .find(|handle| handle.conn_id == last_conn)
                .unwrap()
                .asset_count,
            4,
        );
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
        assert_eq!(pool.connections[0].asset_count, 2);
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
        assert_eq!(pool.connections[0].asset_count, 0);
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
        assert_eq!(pool.connections[0].asset_count, 0);
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
        assert_eq!(pool.connections[0].asset_count, 2);
    }

    #[tokio::test]
    async fn unsubscribe_rejects_asset_count_underflow_before_mutation() {
        let mut pool = pool(2);
        let mut commands = add_test_connection(&mut pool, 43);
        let subscribed = market("m1", "a1y", "a1n");
        pool.subscribe_markets(std::slice::from_ref(&subscribed))
            .await
            .unwrap();
        assert!(matches!(commands.recv().await, Some(Command::Subscribe(_))));
        pool.connections[0].asset_count = 1;

        let error = pool
            .unsubscribe_market(&subscribed.hash, &subscribed.assets)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("has 1 assigned assets"));
        assert_eq!(pool.market_to_conn[&subscribed.hash], 43);
        assert_eq!(pool.asset_to_conn[&subscribed.assets[0]], 43);
        assert_eq!(pool.asset_to_conn[&subscribed.assets[1]], 43);
        assert_eq!(pool.connections[0].asset_count, 1);
        assert!(commands.try_recv().is_err());
    }

    #[tokio::test]
    async fn unsubscribe_rejects_duplicate_assets_before_mutation() {
        let mut pool = pool(2);
        let subscribed = market("m1", "a1y", "a1n");
        pool.subscribe_markets(std::slice::from_ref(&subscribed))
            .await
            .unwrap();

        let duplicate = ["a1y".into(), "a1y".into()];
        let error = pool
            .unsubscribe_market(&subscribed.hash, &duplicate)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("same asset twice"));
        assert_eq!(pool.market_to_conn[&subscribed.hash], 0);
        assert_eq!(pool.asset_to_conn[&subscribed.assets[0]], 0);
        assert_eq!(pool.asset_to_conn[&subscribed.assets[1]], 0);
        assert_eq!(pool.connections[0].asset_count, 2);
    }

    #[tokio::test]
    async fn start_creates_one_empty_lifecycle_anchor() {
        let mut pool = pool(200);
        pool.start().await;
        pool.start().await;

        assert_eq!(pool.connection_count(), 1);
        assert_eq!(pool.connections[0].asset_count, 0);
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
            asset_count: 0,
            metrics: Arc::new(ConnMetrics::default()),
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
    async fn drop_cancels_connection_tasks() {
        struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for DropSignal {
            fn drop(&mut self) {
                if let Some(signal) = self.0.take() {
                    let _ = signal.send(());
                }
            }
        }

        let mut pool = pool(2);
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
            asset_count: 0,
            metrics: Arc::new(ConnMetrics::default()),
            lifecycle_listener: true,
            cmd_tx,
            join: connection_join,
        });

        connection_started_rx.await.unwrap();
        drop(pool);

        tokio::time::timeout(std::time::Duration::from_secs(1), connection_dropped_rx)
            .await
            .expect("dropping the pool should cancel all owned tasks")
            .unwrap();
    }
}
