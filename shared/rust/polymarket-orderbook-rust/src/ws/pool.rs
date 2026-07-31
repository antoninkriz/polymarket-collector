//! Connection pool: routes markets to individual [`Connection`]s with
//! randomized redundant asset assignment.
//!
//! ## Design
//!
//! Each asset is assigned to exactly **two** connections chosen randomly.
//! This gives cross-connection redundancy: if one connection drops, the
//! asset is still covered by the other (which is very likely on a
//! different connection). A global [`DedupForwarder`] deduplicates events
//! from both connections before they reach the sink.
//!
//! ## Allocation strategy
//!
//! On the initial bulk subscribe (startup):
//!
//! 1. Collect all asset IDs from the new markets.
//! 2. Compute how many connections are needed (each holds up to
//!    `max_assets_per_conn` assets across both assignment passes).
//! 3. **Shuffle #1**: randomize asset order, fill-first pack across
//!    connections. Each asset gets a first connection.
//! 4. **Shuffle #2**: randomize again, fill-first pack, skipping each
//!    asset's first connection. Each asset gets a second, different
//!    connection.
//! 5. Dispatch `Subscribe` commands to affected connections.
//!
//! Incremental subscribes (markets arriving via Redis stream) pick 2
//! random connections with capacity for each new asset.
//!
//! ## Health monitoring
//!
//! A monitor task watches every connection's [`ConnStatus`] and maintains
//! per-asset health derived from the reverse index `asset → [conn; 2]`:
//!
//! - **down**: both connections disconnected — data loss for this asset.
//! - **degraded**: exactly 1/2 connections disconnected — no data loss
//!   but no redundancy.
//!
//! Metrics are aggregate gauges/counters stored in shared atomics.
//! Per-asset detail goes to structured log lines.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use rand::seq::SliceRandom;
use rand::thread_rng;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use serde_json::json;

use crate::events::{Event, Market};
use crate::ws::connection::{Command, ConnStats, ConnStatus, Connection};
use crate::ws::dedup::DedupForwarder;

/// Mailbox capacity for each connection's command channel.
const COMMAND_CHANNEL_SIZE: usize = 64;

/// Handle to a single connection owned by the pool.
struct ConnHandle {
    conn_id: usize,
    assets: std::collections::HashSet<String>,
    #[allow(dead_code)] // read by future per-conn logging
    stats: Arc<ConnStats>,
    cmd_tx: mpsc::Sender<Command>,
    join: JoinHandle<Result<()>>,
}

/// Aggregate pool stats, read by the periodic logger.
pub struct PoolStats {
    pub market_count: usize,
    pub connection_count: usize,
    pub asset_down_events: u64,
    pub asset_degraded_events: u64,
    pub conn_down_events: u64,
    // Current gauge values (snapshot).
    pub conns_down: usize,
    pub assets_down: usize,
    pub assets_degraded: usize,
}

/// Shared counters updated by the health monitor, read by the stats loop.
#[derive(Default)]
pub struct HealthCounters {
    pub asset_down_events: AtomicU64,
    pub asset_down_total_ms: AtomicU64,
    pub asset_degraded_events: AtomicU64,
    pub asset_degraded_total_ms: AtomicU64,
    pub conn_down_events: AtomicU64,
    pub conn_down_total_ms: AtomicU64,
    // Current gauge snapshots (set by health monitor, read by stats loop).
    pub conns_down: AtomicU64,
    pub assets_down: AtomicU64,
    pub assets_degraded: AtomicU64,
}

pub struct Pool {
    max_assets_per_conn: usize,
    #[allow(dead_code)]
    dedup_ttl: Duration,
    #[allow(dead_code)]
    event_tx: mpsc::Sender<Event>,
    forwarder: Arc<DedupForwarder>,
    connections: Vec<ConnHandle>,
    /// Markets currently subscribed, keyed by market hash.
    markets: HashMap<String, Market>,
    /// Reverse index: asset_id → [conn_idx_a, conn_idx_b].
    asset_to_conns: HashMap<String, [usize; 2]>,
    next_conn_id: usize,
    health_counters: Arc<HealthCounters>,
    monitor_join: Option<JoinHandle<()>>,
    /// Shared sender for connection status events — cloned to each Connection.
    status_event_tx: mpsc::UnboundedSender<(usize, ConnStatus)>,
    /// Sender to push updated asset→conns mappings to the running monitor.
    asset_conns_tx: watch::Sender<HashMap<String, [usize; 2]>>,
}

impl Pool {
    pub fn new(
        max_assets_per_conn: usize,
        dedup_ttl: Duration,
        event_tx: mpsc::Sender<Event>,
    ) -> Self {
        let forwarder = DedupForwarder::new(dedup_ttl, event_tx.clone());
        let health_counters = Arc::new(HealthCounters::default());
        let (status_event_tx, status_event_rx) = mpsc::unbounded_channel();
        let (asset_conns_tx, asset_conns_rx) = watch::channel(HashMap::new());

        let counters = Arc::clone(&health_counters);
        let monitor_join = Some(tokio::spawn(run_health_monitor(
            status_event_rx,
            asset_conns_rx,
            counters,
        )));

        Self {
            max_assets_per_conn,
            dedup_ttl,
            event_tx,
            forwarder,
            connections: Vec::new(),
            markets: HashMap::new(),
            asset_to_conns: HashMap::new(),
            next_conn_id: 0,
            health_counters,
            monitor_join,
            status_event_tx,
            asset_conns_tx,
        }
    }

    pub fn subscribed_market_count(&self) -> usize {
        self.markets.len()
    }

    #[allow(dead_code)] // used in tests
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    pub fn pool_stats(&self) -> PoolStats {
        PoolStats {
            market_count: self.markets.len(),
            connection_count: self.connections.len(),
            asset_down_events: self.health_counters.asset_down_events.load(Ordering::Relaxed),
            asset_degraded_events: self.health_counters.asset_degraded_events.load(Ordering::Relaxed),
            conn_down_events: self.health_counters.conn_down_events.load(Ordering::Relaxed),
            conns_down: self.health_counters.conns_down.load(Ordering::Relaxed) as usize,
            assets_down: self.health_counters.assets_down.load(Ordering::Relaxed) as usize,
            assets_degraded: self.health_counters.assets_degraded.load(Ordering::Relaxed) as usize,
        }
    }

    /// Subscribe to a batch of markets, allocating to existing or new
    /// connections with randomized two-pass assignment.
    pub async fn subscribe_markets(&mut self, markets: Vec<Market>) -> Result<()> {
        // Collect new assets.
        let mut new_assets: Vec<String> = Vec::new();
        let mut to_register: Vec<Market> = Vec::new();

        for market in markets {
            if self.markets.contains_key(&market.hash) {
                continue;
            }
            new_assets.push(market.assets[0].clone());
            new_assets.push(market.assets[1].clone());
            to_register.push(market);
        }

        if new_assets.is_empty() {
            return Ok(());
        }

        let new_market_count = to_register.len();
        info!(new_markets = new_market_count, new_assets = new_assets.len(), "subscribing markets");

        // Register markets.
        for m in to_register {
            self.markets.insert(m.hash.clone(), m);
        }

        // Each asset appears on 2 different connections (one per shuffle
        // pass). We need:
        //   1. Enough total capacity: 2 * new_assets slots.
        //   2. At least 2 connections (so the second pass has somewhere
        //      different to place each asset).
        let existing_free: usize = self.connections.iter()
            .map(|c| self.max_assets_per_conn.saturating_sub(c.assets.len()))
            .sum();
        let total_demand = new_assets.len() * 2;
        let mut new_conn_count = if total_demand > existing_free {
            (total_demand - existing_free + self.max_assets_per_conn - 1) / self.max_assets_per_conn
        } else {
            0
        };
        // Guarantee at least 2 connections so pass 2 can always find
        // a different connection for each asset.
        let total_after = self.connections.len() + new_conn_count;
        if total_after < 2 {
            new_conn_count += 2 - total_after;
        }

        // Spawn new connections. Each `spawn_connection` call is
        // non-blocking — it creates a tokio task that connects on its own.
        // No sleep between spawns; actual TCP connects are naturally
        // staggered by the runtime scheduler and network latency.
        if new_conn_count > 0 {
            info!(new_connections = new_conn_count, "spawning connections for new assets");
            for _ in 0..new_conn_count {
                self.spawn_connection().await;
            }
        }

        // -- Shuffle pass 1: assign each asset to its first connection --
        let mut shuffled = new_assets.clone();
        {
            let mut rng = thread_rng();
            shuffled.shuffle(&mut rng);
        }

        // Track pending additions per connection index (not yet sent).
        let mut pending: HashMap<usize, Vec<String>> = HashMap::new();
        let mut first_conn: HashMap<String, usize> = HashMap::new();

        for asset in &shuffled {
            let conn_idx = self.find_conn_with_capacity(&pending, None);
            let conn_idx = conn_idx.expect("not enough connection capacity after spawning");
            pending.entry(conn_idx).or_default().push(asset.clone());
            first_conn.insert(asset.clone(), conn_idx);
        }

        // -- Shuffle pass 2: assign each asset to a second, different connection --
        {
            let mut rng = thread_rng();
            shuffled.shuffle(&mut rng);
        }

        let mut overflow: Vec<String> = Vec::new();
        for asset in &shuffled {
            let exclude = first_conn[asset];
            let conn_idx = self.find_conn_with_capacity(&pending, Some(exclude));
            match conn_idx {
                Some(idx) => {
                    pending.entry(idx).or_default().push(asset.clone());
                    self.asset_to_conns.insert(asset.clone(), [first_conn[asset], idx]);
                }
                None => overflow.push(asset.clone()),
            }
        }

        // -- Pass 3: spawn one extra connection for overflow assets ----------
        if !overflow.is_empty() {
            warn!(overflow = overflow.len(), "pass 2 capacity exceeded, spawning overflow connection");
            self.spawn_connection().await;
            let overflow_idx = self.connections.len() - 1;
            for asset in &overflow {
                pending.entry(overflow_idx).or_default().push(asset.clone());
                self.asset_to_conns.insert(asset.clone(), [first_conn[asset], overflow_idx]);
            }
        }

        // -- Dispatch Subscribe commands to affected connections ----------
        for (conn_idx, assets) in pending {
            let handle = &mut self.connections[conn_idx];
            for a in &assets {
                handle.assets.insert(a.clone());
            }
            if let Err(e) = handle.cmd_tx.send(Command::Subscribe(assets)).await {
                warn!(conn = handle.conn_id, error = %e, "subscribe send failed");
            }
        }

        // Restart the health monitor with updated connection/asset state.
        self.update_monitor_mappings();

        info!(
            total_markets = self.markets.len(),
            total_connections = self.connections.len(),
            total_assets_tracked = self.asset_to_conns.len(),
            "subscribe_markets complete",
        );
        Ok(())
    }

    /// Unsubscribe from markets by hash. Garbage-collects connections that
    /// become empty.
    pub async fn unsubscribe_markets(&mut self, market_hashes: Vec<String>) -> Result<()> {
        let mut conn_unsubs: HashMap<usize, Vec<String>> = HashMap::new();
        let mut removed_count = 0_usize;

        for hash in &market_hashes {
            let Some(market) = self.markets.remove(hash) else { continue };
            removed_count += 1;

            for asset in &market.assets {
                if let Some([c1, c2]) = self.asset_to_conns.remove(asset) {
                    conn_unsubs.entry(c1).or_default().push(asset.clone());
                    conn_unsubs.entry(c2).or_default().push(asset.clone());
                }
            }
        }

        // Send unsubscribes and update local tracking.
        for (conn_idx, assets) in &conn_unsubs {
            if let Some(handle) = self.connections.get_mut(*conn_idx) {
                for a in assets {
                    handle.assets.remove(a);
                }
                if let Err(e) = handle.cmd_tx.send(Command::Unsubscribe(assets.clone())).await {
                    warn!(conn = handle.conn_id, error = %e, "unsubscribe send failed");
                }
            }
        }

        // GC empty connections (iterate in reverse to maintain indices).
        let mut to_gc: Vec<usize> = self.connections.iter()
            .enumerate()
            .filter(|(_, h)| h.assets.is_empty())
            .map(|(i, _)| i)
            .collect();
        to_gc.sort_unstable_by(|a, b| b.cmp(a)); // reverse order
        for idx in to_gc {
            let handle = self.connections.swap_remove(idx);
            // If swap_remove moved the last element to `idx`, update the
            // reverse index for all assets that pointed to the old last index.
            let old_last = self.connections.len(); // this was the index before removal
            if idx < old_last {
                // The element that was at `old_last` is now at `idx`.
                for (_, conns) in self.asset_to_conns.iter_mut() {
                    if conns[0] == old_last { conns[0] = idx; }
                    if conns[1] == old_last { conns[1] = idx; }
                }
            }
            drop(handle.cmd_tx);
            info!(conn = handle.conn_id, remaining = self.connections.len(), "removed empty connection");
            // Await join in background to not block.
            tokio::spawn(async move {
                let _ = handle.join.await;
            });
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

    /// Drain shutdown — close every command channel and await every task.
    pub async fn shutdown(mut self) -> Result<()> {
        let count = self.connections.len();
        info!(connections = count, "shutting down pool");

        // Stop the monitor first.
        if let Some(join) = self.monitor_join.take() {
            join.abort();
            let _ = join.await;
        }

        for handle in self.connections.drain(..) {
            let conn_id = handle.conn_id;
            drop(handle.cmd_tx);
            match handle.join.await {
                Ok(Ok(())) => debug!(conn = conn_id, "connection joined"),
                Ok(Err(e)) => warn!(conn = conn_id, error = %e, "connection ended with error"),
                Err(e) => warn!(conn = conn_id, error = %e, "connection panicked"),
            }
        }
        Ok(())
    }

    // -- internal --------------------------------------------------------

    /// Find a connection index with room for at least 1 more asset,
    /// accounting for pending (not-yet-sent) additions. Optionally
    /// excludes one connection index (used in shuffle pass 2).
    fn find_conn_with_capacity(
        &self,
        pending: &HashMap<usize, Vec<String>>,
        exclude: Option<usize>,
    ) -> Option<usize> {
        for (idx, handle) in self.connections.iter().enumerate() {
            if Some(idx) == exclude {
                continue;
            }
            let pending_count = pending.get(&idx).map(Vec::len).unwrap_or(0);
            if handle.assets.len() + pending_count + 1 <= self.max_assets_per_conn {
                return Some(idx);
            }
        }
        None
    }

    /// Spawn a new connection with no initial assets.
    async fn spawn_connection(&mut self) {
        let conn_id = self.next_conn_id;
        self.next_conn_id += 1;

        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(COMMAND_CHANNEL_SIZE);
        let status_tx = self.status_event_tx.clone();

        let conn = Connection::new(conn_id, Arc::clone(&self.forwarder), status_tx);
        let stats = Arc::clone(&conn.stats);
        let join = tokio::spawn(conn.run(cmd_rx));

        self.connections.push(ConnHandle {
            conn_id,
            assets: std::collections::HashSet::new(),
            stats,
            cmd_tx,
            join,
        });

        info!(conn = conn_id, total = self.connections.len(), "spawned connection");
    }

    /// Push updated asset→conns mappings to the running monitor task.
    fn update_monitor_mappings(&self) {
        let _ = self.asset_conns_tx.send(self.asset_to_conns.clone());
    }
}

// ---- Health monitor ---------------------------------------------------

/// Per-asset health state tracked by the monitor.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AssetState {
    Healthy,
    Degraded,
    Down,
}

/// Monitor task: receives every connection status transition via a single
/// mpsc channel and derives per-asset health. Asset→connection mappings
/// are pushed via a watch channel so the monitor never needs restarting.
async fn run_health_monitor(
    mut status_rx: mpsc::UnboundedReceiver<(usize, ConnStatus)>,
    mut asset_conns_rx: watch::Receiver<HashMap<String, [usize; 2]>>,
    counters: Arc<HealthCounters>,
) {
    // Current asset→conns mapping (updated via watch channel).
    let mut asset_conns: HashMap<String, [usize; 2]> = HashMap::new();
    // Reverse index: conn_idx → list of assets on that connection.
    let mut conn_to_assets: HashMap<usize, Vec<String>> = HashMap::new();

    // Current connection states.
    let mut conn_status: HashMap<usize, ConnStatus> = HashMap::new();

    // Per-asset health tracking.
    let mut asset_states: HashMap<String, AssetState> = HashMap::new();
    let mut asset_transition_time: HashMap<String, Instant> = HashMap::new();
    let mut conn_down_since: HashMap<usize, Option<Instant>> = HashMap::new();

    // Startup phase: suppress per-asset log events until all connections
    // are up for the first time.  Counters and gauges still update normally.
    let mut startup = true;
    let startup_start = Instant::now();

    info!(
        grafana = true,
        event = "container_start",
        annotation = "Process started",
        "container_start",
    );

    let compute_asset_state = |asset: &str, conn_status: &HashMap<usize, ConnStatus>, asset_conns: &HashMap<String, [usize; 2]>| -> AssetState {
        let Some([c1, c2]) = asset_conns.get(asset) else {
            return AssetState::Healthy;
        };
        let s1 = conn_status.get(c1).copied().unwrap_or(ConnStatus::Disconnected);
        let s2 = conn_status.get(c2).copied().unwrap_or(ConnStatus::Disconnected);
        match (s1, s2) {
            (ConnStatus::Connected, ConnStatus::Connected) => AssetState::Healthy,
            (ConnStatus::Disconnected, ConnStatus::Disconnected) => AssetState::Down,
            _ => AssetState::Degraded,
        }
    };

    /// Rebuild `conn_to_assets` and recompute all `asset_states` from scratch
    /// after the asset→conns mapping changes.
    fn rebuild_derived(
        asset_conns: &HashMap<String, [usize; 2]>,
        conn_to_assets: &mut HashMap<usize, Vec<String>>,
        asset_states: &mut HashMap<String, AssetState>,
        conn_status: &HashMap<usize, ConnStatus>,
        counters: &HealthCounters,
    ) {
        conn_to_assets.clear();
        for (asset, [c1, c2]) in asset_conns {
            conn_to_assets.entry(*c1).or_default().push(asset.clone());
            conn_to_assets.entry(*c2).or_default().push(asset.clone());
        }

        // Remove assets no longer in the mapping.
        asset_states.retain(|a, _| asset_conns.contains_key(a));

        // Recompute states for all current assets.
        for (asset, [c1, c2]) in asset_conns {
            let s1 = conn_status.get(c1).copied().unwrap_or(ConnStatus::Disconnected);
            let s2 = conn_status.get(c2).copied().unwrap_or(ConnStatus::Disconnected);
            let state = match (s1, s2) {
                (ConnStatus::Connected, ConnStatus::Connected) => AssetState::Healthy,
                (ConnStatus::Disconnected, ConnStatus::Disconnected) => AssetState::Down,
                _ => AssetState::Degraded,
            };
            asset_states.insert(asset.clone(), state);
        }

        update_aggregate_gauges(counters, asset_states, conn_status);
    }

    loop {
        tokio::select! {
            // -- Connection status event ------------------------------------
            recv = status_rx.recv() => {
                let Some((changed_idx, new_status)) = recv else {
                    // All senders dropped — pool is shutting down.
                    break;
                };

                let old_status = conn_status.insert(changed_idx, new_status).unwrap_or(ConnStatus::Disconnected);
                if old_status == new_status {
                    continue;
                }

                let now = Instant::now();

                // -- Connection-level transitions ---------------------------
                let total_conns = conn_status.len();
                let total_conns_down = conn_status.values().filter(|s| matches!(s, ConnStatus::Disconnected)).count();

                match (old_status, new_status) {
                    (ConnStatus::Connected, ConnStatus::Disconnected) => {
                        conn_down_since.insert(changed_idx, Some(now));
                        counters.conn_down_events.fetch_add(1, Ordering::Relaxed);
                        warn!(
                            grafana = true,
                            event = "conn_down",
                            conn = changed_idx,
                            meta = %json!({
                                "total_conns": total_conns,
                                "total_conns_down": total_conns_down,
                            }),
                            "conn_down",
                        );
                    }
                    (ConnStatus::Disconnected, ConnStatus::Connected) => {
                        if let Some(Some(since)) = conn_down_since.remove(&changed_idx) {
                            let dur = since.elapsed();
                            counters.conn_down_total_ms.fetch_add(dur.as_millis() as u64, Ordering::Relaxed);
                            info!(
                                grafana = true,
                                event = "conn_up",
                                conn = changed_idx,
                                duration_seconds = dur.as_secs_f64(),
                                meta = %json!({
                                    "total_conns_down": total_conns_down,
                                }),
                                "conn_up",
                            );
                        }
                    }
                    _ => {}
                }

                // -- Asset-level transitions --------------------------------
                let affected_assets = conn_to_assets.get(&changed_idx).cloned().unwrap_or_default();
                for asset in &affected_assets {
                    let new_state = compute_asset_state(asset, &conn_status, &asset_conns);
                    let old_state = asset_states.insert(asset.clone(), new_state).unwrap_or(AssetState::Down);

                    if old_state == new_state {
                        continue;
                    }

                    let [c1, c2] = asset_conns[asset];
                    let s1 = conn_status.get(&c1).copied().unwrap_or(ConnStatus::Disconnected);
                    let s2 = conn_status.get(&c2).copied().unwrap_or(ConnStatus::Disconnected);
                    let status_str = |s: ConnStatus| match s {
                        ConnStatus::Connected => "connected",
                        ConnStatus::Disconnected => "disconnected",
                    };

                    // Record exit from previous state.
                    match old_state {
                        AssetState::Down => {
                            if let Some(since) = asset_transition_time.remove(asset) {
                                let dur = since.elapsed();
                                counters.asset_down_total_ms.fetch_add(dur.as_millis() as u64, Ordering::Relaxed);
                                if !startup {
                                    let total_assets_down = asset_states.values().filter(|s| **s == AssetState::Down).count();
                                    let recovered_via = if matches!(s1, ConnStatus::Connected) { c1 } else { c2 };
                                    if dur >= Duration::from_secs(5) {
                                        warn!(asset = %asset, duration_seconds = dur.as_secs_f64(), "[ASSET-DATA-GAP] recovered from down");
                                    } else {
                                        info!(asset = %asset, duration_seconds = dur.as_secs_f64(), "[ASSET-DATA-GAP] recovered from down");
                                    }
                                    let recovered_at = chrono::Utc::now();
                                    let down_since = recovered_at - chrono::Duration::from_std(dur).unwrap_or_default();
                                    info!(
                                        grafana = true,
                                        event = "asset_data_gap",
                                        asset = %asset,
                                        duration_seconds = dur.as_secs_f64(),
                                        meta = %json!({
                                            "conn_a": c1,
                                            "conn_b": c2,
                                            "conn_a_status": status_str(s1),
                                            "conn_b_status": status_str(s2),
                                            "recovered_via": recovered_via,
                                            "total_assets_down": total_assets_down,
                                            "down_since": down_since.to_rfc3339(),
                                            "recovered_at": recovered_at.to_rfc3339(),
                                        }),
                                        "asset_data_gap",
                                    );
                                }
                            }
                        }
                        AssetState::Degraded => {
                            if let Some(since) = asset_transition_time.remove(asset) {
                                let dur = since.elapsed();
                                counters.asset_degraded_total_ms.fetch_add(dur.as_millis() as u64, Ordering::Relaxed);
                                if !startup {
                                    let total_assets_degraded = asset_states.values().filter(|s| **s == AssetState::Degraded).count();
                                    info!(
                                        grafana = true,
                                        event = "asset_healthy",
                                        asset = %asset,
                                        duration_seconds = dur.as_secs_f64(),
                                        meta = %json!({
                                            "conn_a": c1,
                                            "conn_b": c2,
                                            "conn_a_status": status_str(s1),
                                            "conn_b_status": status_str(s2),
                                            "total_assets_degraded": total_assets_degraded,
                                        }),
                                        "asset_healthy",
                                    );
                                }
                            }
                        }
                        AssetState::Healthy => {}
                    }

                    // Record entry into new state.
                    match new_state {
                        AssetState::Down => {
                            asset_transition_time.insert(asset.clone(), now);
                            counters.asset_down_events.fetch_add(1, Ordering::Relaxed);
                            if !startup {
                                let total_assets_down = asset_states.values().filter(|s| **s == AssetState::Down).count();
                                warn!(asset = %asset, "[ASSET-DOWN] both connections disconnected");
                                warn!(
                                    grafana = true,
                                    event = "asset_down",
                                    asset = %asset,
                                    meta = %json!({
                                        "conn_a": c1,
                                        "conn_b": c2,
                                        "conn_a_status": status_str(s1),
                                        "conn_b_status": status_str(s2),
                                        "total_conns_down": total_conns_down,
                                        "total_assets_down": total_assets_down,
                                    }),
                                    "asset_down",
                                );
                            }
                        }
                        AssetState::Degraded => {
                            asset_transition_time.insert(asset.clone(), now);
                            counters.asset_degraded_events.fetch_add(1, Ordering::Relaxed);
                            if !startup {
                                let total_assets_degraded = asset_states.values().filter(|s| **s == AssetState::Degraded).count();
                                let failed_conn = if matches!(s1, ConnStatus::Disconnected) { c1 } else { c2 };
                                info!(asset = %asset, "[ASSET-DEGRADED] 1/2 connections down");
                                info!(
                                    grafana = true,
                                    event = "asset_degraded",
                                    asset = %asset,
                                    meta = %json!({
                                        "conn_a": c1,
                                        "conn_b": c2,
                                        "conn_a_status": status_str(s1),
                                        "conn_b_status": status_str(s2),
                                        "failed_conn": failed_conn,
                                        "total_assets_degraded": total_assets_degraded,
                                    }),
                                    "asset_degraded",
                                );
                            }
                        }
                        AssetState::Healthy => {
                            // No entry tracking needed for healthy.
                        }
                    }
                }

                // Update aggregate gauges.
                update_aggregate_gauges(&counters, &asset_states, &conn_status);

                // Transition to runtime once all known connections are up.
                // Use conn_to_assets (built from the asset mapping) as the
                // authoritative set — conn_status only has connections that
                // have reported in so far.
                if startup && !conn_to_assets.is_empty() {
                    let expected_conns = conn_to_assets.len();
                    let all_connected = conn_to_assets.keys().all(|idx| {
                        matches!(conn_status.get(idx), Some(ConnStatus::Connected))
                    });
                    if all_connected {
                        startup = false;
                        let elapsed = startup_start.elapsed();
                        let total_conns = expected_conns;
                        let total_assets = asset_conns.len();
                        info!(
                            grafana = true,
                            event = "startup_complete",
                            annotation = %format!(
                                "All {total_conns} conns up in {:.1}s — {total_assets} assets",
                                elapsed.as_secs_f64(),
                            ),
                            "startup_complete",
                        );
                    }
                }
            }

            // -- Asset mapping update from pool -----------------------------
            Ok(()) = asset_conns_rx.changed() => {
                asset_conns = asset_conns_rx.borrow_and_update().clone();
                rebuild_derived(&asset_conns, &mut conn_to_assets, &mut asset_states, &conn_status, &counters);
            }
        }
    }
}

fn update_aggregate_gauges(
    counters: &HealthCounters,
    asset_states: &HashMap<String, AssetState>,
    conn_status: &HashMap<usize, ConnStatus>,
) {
    let assets_down = asset_states.values().filter(|s| **s == AssetState::Down).count();
    let assets_degraded = asset_states.values().filter(|s| **s == AssetState::Degraded).count();
    let conns_down = conn_status.values().filter(|s| matches!(s, ConnStatus::Disconnected)).count();

    counters.assets_down.store(assets_down as u64, Ordering::Relaxed);
    counters.assets_degraded.store(assets_degraded as u64, Ordering::Relaxed);
    counters.conns_down.store(conns_down as u64, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market(hash: &str, yes: &str, no: &str) -> Market {
        Market::new(hash.into(), yes.into(), no.into())
    }

    fn pool(max_assets: usize) -> Pool {
        let (tx, _rx) = mpsc::channel::<Event>(1024);
        Pool::new(max_assets, Duration::from_secs(10), tx)
    }

    #[tokio::test]
    async fn each_asset_gets_two_different_connections() {
        let mut p = pool(200);
        p.subscribe_markets(vec![
            market("m1", "a1y", "a1n"),
            market("m2", "a2y", "a2n"),
            market("m3", "a3y", "a3n"),
        ])
        .await
        .unwrap();

        // Every asset should be in the reverse index with two distinct conns.
        for asset in ["a1y", "a1n", "a2y", "a2n", "a3y", "a3n"] {
            let [c1, c2] = p.asset_to_conns[asset];
            assert_ne!(c1, c2, "asset {asset} assigned to same connection twice");
        }
    }

    #[tokio::test]
    async fn connections_dont_exceed_capacity() {
        // max_assets=4, 3 markets = 6 assets, each assigned twice = 12 slots.
        // 12 / 4 = 3 connections minimum.
        let mut p = pool(4);
        p.subscribe_markets(vec![
            market("m1", "a1y", "a1n"),
            market("m2", "a2y", "a2n"),
            market("m3", "a3y", "a3n"),
        ])
        .await
        .unwrap();

        for handle in &p.connections {
            assert!(
                handle.assets.len() <= 4,
                "conn {} has {} assets, max is 4",
                handle.conn_id,
                handle.assets.len(),
            );
        }
        assert_eq!(p.subscribed_market_count(), 3);
    }

    #[tokio::test]
    async fn duplicate_market_is_skipped() {
        let mut p = pool(200);
        p.subscribe_markets(vec![market("m1", "a1y", "a1n")]).await.unwrap();
        let conns_before = p.connection_count();
        p.subscribe_markets(vec![market("m1", "a1y", "a1n")]).await.unwrap();
        assert_eq!(p.connection_count(), conns_before);
        assert_eq!(p.subscribed_market_count(), 1);
    }

    #[tokio::test]
    async fn unsubscribe_removes_market_and_gcs_empty_connection() {
        let mut p = pool(200);
        p.subscribe_markets(vec![market("m1", "a1y", "a1n")]).await.unwrap();
        let initial_conns = p.connection_count();
        assert!(initial_conns > 0);

        p.unsubscribe_markets(vec!["m1".into()]).await.unwrap();
        assert_eq!(p.subscribed_market_count(), 0);
        assert!(p.asset_to_conns.is_empty());
    }

    #[tokio::test]
    async fn unsubscribe_keeps_connections_with_remaining_assets() {
        let mut p = pool(200);
        p.subscribe_markets(vec![
            market("m1", "a1y", "a1n"),
            market("m2", "a2y", "a2n"),
        ])
        .await
        .unwrap();

        p.unsubscribe_markets(vec!["m1".into()]).await.unwrap();
        assert_eq!(p.subscribed_market_count(), 1);
        // m2 assets should still be tracked.
        assert!(p.asset_to_conns.contains_key("a2y"));
        assert!(p.asset_to_conns.contains_key("a2n"));
    }
}
