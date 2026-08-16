//! Single WebSocket connection to the Polymarket CLOB.
//!
//! Owns one socket and the subscription state for the assets routed to it.
//! All state mutation happens inside the [`run`](Connection::run) task — no
//! locks shared with the pool.
//!
//! ## Heartbeat
//!
//! Polymarket uses **application-level** PING/PONG text messages, not
//! WebSocket protocol ping frames. Per the Polymarket docs, the client
//! sends `"PING"` every **10 s** and the server responds `"PONG"`. Any frame
//! received after a PING proves the transport is alive; this matters when a
//! busy market-data stream delays the textual PONG behind data frames. If no
//! frame arrives before the heartbeat deadline, the socket is closed and the
//! run loop reconnects after a short per-connection jitter (no base backoff).
//! Connection handshakes are bounded so an upstream half-open attempt cannot
//! hold an authoritative route until the operating system times it out.
//! Other failures use exponential backoff (1 → 60 s) plus the same jitter so
//! an upstream batch close does not become a synchronized reconnect storm.
//!
//! ## Subscription state
//!
//! - `desired: HashMap<String, DesiredAsset>` — durable intent plus each
//!   asset's current-session subscription, fresh-book readiness, and recovery
//!   timing. Session-local fields are reset on reconnect.
//!
//! ## Wire protocol
//!
//! - First subscription on a fresh socket: `{"assets_ids": [...], "type": "market", "custom_feature_enabled": true}`
//! - Subsequent subscribes:                 `{"assets_ids": [...], "operation": "subscribe", "custom_feature_enabled": true}`
//! - Unsubscribes:                          `{"assets_ids": [...], "operation": "unsubscribe"}`
//!
//! Messages can arrive as a single JSON object or as a JSON array of objects;
//! the connection parser normalizes both forms before deserialization.
//!
//! ## Failure mode for the event channel
//!
//! Events are stamped at receipt and pushed directly to the global channel.
//! Each market has one authoritative connection, so no payload merge or
//! deduplication occurs here. If the channel is **full**, the consumer is too slow and we
//! exit the process. If the channel is **closed**, the sink has shut down
//! and we return cleanly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::events::{explode, Event, MarketLifecycleObservation, WireMessage};
use crate::record::{now_ns, CollectorContext, EventRecord};
use crate::ws::WS_MARKET_URL;

type MarketWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Default)]
pub(crate) struct ConnMetrics {
    connected: AtomicBool,
    ready_assets: AtomicUsize,
}

impl ConnMetrics {
    pub(crate) fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub(crate) fn ready_assets(&self) -> usize {
        self.ready_assets.load(Ordering::Relaxed)
    }
}

#[derive(Default)]
pub(crate) struct HealthCounters {
    pub(crate) asset_down_events: AtomicU64,
    recovery: Mutex<RecoveryCounters>,
    pub(crate) conn_down_events: AtomicU64,
}

#[derive(Default)]
struct RecoveryCounters {
    total: u64,
    window: RecoveryWindow,
}

#[derive(Default)]
pub(crate) struct RecoveryWindow {
    pub(crate) count: u64,
    pub(crate) latency_us: u64,
    pub(crate) latency_us_max: u64,
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

    pub(crate) fn take_recovery_window(&self) -> (u64, RecoveryWindow) {
        let mut recovery = self
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let total = recovery.total;
        (total, std::mem::take(&mut recovery.window))
    }
}

/// Per Polymarket docs (Market & User channels): client sends `"PING"`
/// every 10 seconds; see the module-level heartbeat documentation.
const PING_INTERVAL: Duration = Duration::from_secs(10);
const PONG_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(60);
const RECONNECT_JITTER_MAX_MS: u64 = 750;

/// Deterministic jitter spreads a batch of reconnects without adding a random
/// dependency or making tests non-reproducible. The reconnect round changes
/// the offset so the same connection does not always occupy the same slot.
fn reconnect_jitter(index: usize, reconnect_round: u64) -> Duration {
    let mixed = (index as u64)
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(reconnect_round.wrapping_mul(1_442_695_040_888_963_407));
    Duration::from_millis(mixed % (RECONNECT_JITTER_MAX_MS + 1))
}

async fn connect_websocket(url: &str, timeout: Duration) -> Result<MarketWebSocket> {
    let result = tokio::time::timeout(timeout, tokio_tungstenite::connect_async(url))
        .await
        .context("WebSocket connection timed out")?;
    let (stream, _response) = result.context("WebSocket connection failed")?;
    Ok(stream)
}

/// Commands sent from the pool into a connection task. Shutdown is signalled
/// by closing the command channel (dropping every sender), which makes
/// `recv()` return `None`; no explicit `Shutdown` variant is needed.
#[derive(Debug)]
pub(crate) enum Command {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

pub(crate) struct Connection {
    index: usize,
    event_tx: mpsc::Sender<EventRecord>,
    collector: Arc<CollectorContext>,
    metrics: Arc<ConnMetrics>,
    health_counters: Arc<HealthCounters>,
    /// Whether this socket is one of the redundant listeners for global
    /// `new_market` notifications.
    lifecycle_listener: bool,
    lifecycle_tx: Option<mpsc::Sender<MarketLifecycleObservation>>,
}

impl Connection {
    pub(crate) fn new(
        index: usize,
        event_tx: mpsc::Sender<EventRecord>,
        collector: Arc<CollectorContext>,
        metrics: Arc<ConnMetrics>,
        health_counters: Arc<HealthCounters>,
        lifecycle_listener: bool,
        lifecycle_tx: Option<mpsc::Sender<MarketLifecycleObservation>>,
    ) -> Self {
        Self {
            index,
            event_tx,
            collector,
            metrics,
            health_counters,
            lifecycle_listener,
            lifecycle_tx,
        }
    }

    /// Run the connect → listen → reconnect loop until the command channel
    /// is closed. Returns an error only
    /// if the event channel sender is dropped (i.e. the sink died) — in
    /// that case the caller should shut down all connections.
    pub(crate) async fn run(self, mut commands: mpsc::Receiver<Command>) -> Result<()> {
        let Connection {
            index,
            event_tx,
            collector,
            metrics,
            health_counters,
            lifecycle_listener,
            lifecycle_tx,
        } = self;
        let mut sub = SubState::new(metrics, health_counters);
        let mut backoff = Duration::from_secs(1);
        let mut last_message_time: Option<Instant> = None;
        let mut fast_reconnect_reason: Option<&'static str> = None;
        let mut first_attempt = true;
        let mut reconnect_round = 0_u64;
        loop {
            // Skip delay on the first attempt. Every reconnect gets a small
            // deterministic jitter so a source-side batch close does not make
            // hundreds of sockets reconnect in the same millisecond.
            if first_attempt {
                first_attempt = false;
            } else {
                let jitter = reconnect_jitter(index, reconnect_round);
                reconnect_round = reconnect_round.wrapping_add(1);
                let (delay, reason) = if let Some(reason) = fast_reconnect_reason.take() {
                    (jitter, reason)
                } else {
                    let delay = backoff.saturating_add(jitter);
                    backoff = (backoff * 2).min(RECONNECT_DELAY_MAX);
                    (delay, "connection error")
                };
                info!(
                    conn = index,
                    delay_ms = delay.as_millis() as u64,
                    reason,
                    "reconnecting"
                );
                tokio::time::sleep(delay).await;
            }

            // Allow shutdown to short-circuit during the reconnect delay.
            // Shutdown is signalled by the command channel being closed
            // (all senders dropped) AND the buffer drained. We use a
            // non-consuming check (`is_closed` + `is_empty`) because
            // `try_recv()` would consume buffered commands like the
            // pool's pre-loaded initial Subscribe and silently drop them.
            if commands.is_closed() && commands.is_empty() {
                sub.stop();
                return Ok(());
            }

            // Connect.
            let ws_stream = match connect_websocket(WS_MARKET_URL, CONNECT_TIMEOUT).await {
                Ok(stream) => {
                    backoff = Duration::from_secs(1);
                    stream
                }
                Err(error) => {
                    warn!(conn = index, %error, "ws connect failed; will retry");
                    continue;
                }
            };
            // Reset session flags while durable desired entries survive.
            sub.reset_wire_session();

            // Record reconnect gap if this isn't the first connection.
            let connect_time = Instant::now();
            if let Some(last) = last_message_time {
                let gap = connect_time.duration_since(last);
                warn!(
                    conn = index,
                    gap_seconds = gap.as_secs_f64(),
                    desired_count = sub.desired.len(),
                    "[CONN-GAP] authoritative connection reconnect gap",
                );
            }

            sub.connected();

            // Run a single connected session. A heartbeat timeout skips the
            // next reconnect backoff.
            let context = SessionContext {
                index,
                event_tx: &event_tx,
                collector: &collector,
                lifecycle_listener,
                lifecycle_tx: lifecycle_tx.as_ref(),
            };
            let outcome = run_session(
                ws_stream,
                context,
                &mut sub,
                &mut commands,
                &mut last_message_time,
            )
            .await;

            match outcome {
                SessionOutcome::HeartbeatTimeout => {
                    log_connection_gap(index, &mut sub);
                    fast_reconnect_reason = Some("heartbeat timeout");
                }
                SessionOutcome::Closed => {
                    log_connection_gap(index, &mut sub);
                    fast_reconnect_reason = Some("session ended");
                }
                SessionOutcome::ChannelClosed => {
                    sub.stop();
                    return Ok(());
                }
                SessionOutcome::Shutdown => {
                    sub.stop();
                    return Ok(());
                }
                SessionOutcome::Error(e) => {
                    log_connection_gap(index, &mut sub);
                    warn!(conn = index, error = %e, "session error, will reconnect");
                }
            }
        }
    }
}

#[derive(Default)]
struct DesiredAsset {
    subscribed: bool,
    initialized: bool,
    recovery_started: Option<Instant>,
}

/// Subscription intent and reconstruction state, owned by the connection task.
struct SubState {
    desired: HashMap<String, DesiredAsset>,
    /// Whether the initial subscribe message has been sent on the current
    /// session. The first message uses `{"type": "market"}`; subsequent
    /// ones use `{"operation": "subscribe"}`.
    initial_sent: bool,
    metrics: Arc<ConnMetrics>,
    health_counters: Arc<HealthCounters>,
}

impl SubState {
    fn new(metrics: Arc<ConnMetrics>, health_counters: Arc<HealthCounters>) -> Self {
        Self {
            desired: HashMap::new(),
            initial_sent: false,
            metrics,
            health_counters,
        }
    }

    fn reset_wire_session(&mut self) {
        for asset in self.desired.values_mut() {
            asset.subscribed = false;
        }
        self.initial_sent = false;
    }

    fn connected(&self) {
        self.metrics.connected.store(true, Ordering::Relaxed);
    }

    /// End one failed wire session and retain the first recovery timestamp for
    /// every asset that remains uninitialized across further failed sessions.
    fn disconnected(&mut self) -> usize {
        self.disconnected_at(Instant::now())
    }

    fn disconnected_at(&mut self, now: Instant) -> usize {
        self.reset_wire_session();
        if !self.metrics.connected.swap(false, Ordering::Relaxed) {
            return 0;
        }

        self.health_counters
            .conn_down_events
            .fetch_add(1, Ordering::Relaxed);
        let mut invalidated = 0;
        for asset in self.desired.values_mut() {
            if asset.initialized {
                asset.initialized = false;
                asset.recovery_started = Some(now);
                invalidated += 1;
            }
        }
        self.decrement_ready(invalidated);
        self.health_counters
            .asset_down_events
            .fetch_add(invalidated as u64, Ordering::Relaxed);
        invalidated
    }

    /// Stop intentionally without turning shutdown into a data-gap event.
    fn stop(&self) {
        self.metrics.connected.store(false, Ordering::Relaxed);
        self.metrics.ready_assets.store(0, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn subscribe(&mut self, asset_id: String) {
        self.desired.entry(asset_id).or_default();
    }

    fn unsubscribe(&mut self, asset_id: &str) -> bool {
        let Some(asset) = self.desired.remove(asset_id) else {
            return false;
        };
        if asset.initialized {
            self.decrement_ready(1);
        }
        true
    }

    fn contains(&self, asset_id: &str) -> bool {
        self.desired.contains_key(asset_id)
    }

    fn initialized(&self, asset_id: &str) -> bool {
        self.desired
            .get(asset_id)
            .is_some_and(|asset| asset.initialized)
    }

    fn subscribe_for_session(&mut self, asset_id: String) -> bool {
        !std::mem::replace(
            &mut self.desired.entry(asset_id).or_default().subscribed,
            true,
        )
    }

    fn mark_all_subscribed(&mut self) {
        for asset in self.desired.values_mut() {
            asset.subscribed = true;
        }
    }

    fn require_fresh_snapshot(&mut self, asset_id: &str) {
        let Some(asset) = self.desired.get_mut(asset_id) else {
            return;
        };
        if asset.initialized {
            asset.initialized = false;
            asset.recovery_started = None;
            self.decrement_ready(1);
        }
    }

    fn initialize(&mut self, asset_id: &str) {
        self.initialize_at(asset_id, Instant::now());
    }

    fn initialize_at(&mut self, asset_id: &str, now: Instant) {
        let Some(asset) = self.desired.get_mut(asset_id) else {
            return;
        };
        if asset.initialized {
            return;
        }
        asset.initialized = true;
        self.metrics.ready_assets.fetch_add(1, Ordering::Relaxed);
        if let Some(started) = asset.recovery_started.take() {
            let latency_us = now
                .saturating_duration_since(started)
                .as_micros()
                .min(u64::MAX as u128) as u64;
            self.health_counters.record_recovery(latency_us);
        }
    }

    fn decrement_ready(&self, count: usize) {
        if count == 0 {
            return;
        }
        let ready = self.metrics.ready_assets.load(Ordering::Relaxed);
        self.metrics
            .ready_assets
            .store(ready.saturating_sub(count), Ordering::Relaxed);
    }
}

#[cfg(test)]
impl Default for SubState {
    fn default() -> Self {
        Self::new(
            Arc::new(ConnMetrics::default()),
            Arc::new(HealthCounters::default()),
        )
    }
}

fn log_connection_gap(index: usize, sub: &mut SubState) {
    let invalidated_assets = sub.disconnected();
    if invalidated_assets != 0 {
        warn!(
            conn = index,
            invalidated_assets,
            assigned_assets = sub.desired.len(),
            "[CONNECTION-DATA-GAP] authoritative connection down"
        );
    }
}

#[derive(Debug)]
enum SessionOutcome {
    /// Session ended because no frame followed a heartbeat. Reconnect immediately.
    HeartbeatTimeout,
    /// Session ended due to a remote close or read EOF. Reconnect immediately.
    Closed,
    /// Event channel closed (sink died). Connection should shut down.
    ChannelClosed,
    /// The pool dropped the command channel.
    Shutdown,
    /// Some other error during the session — reconnect with backoff.
    Error(anyhow::Error),
}

#[derive(Clone, Copy)]
struct SessionContext<'a> {
    index: usize,
    event_tx: &'a mpsc::Sender<EventRecord>,
    collector: &'a CollectorContext,
    lifecycle_listener: bool,
    lifecycle_tx: Option<&'a mpsc::Sender<MarketLifecycleObservation>>,
}

/// Run a single connected WebSocket session: send the initial subscription,
/// then multiplex reads, ping ticker, and pool commands until something
/// breaks. Updates `last_message_time` whenever a frame is received.
async fn run_session(
    ws_stream: MarketWebSocket,
    context: SessionContext<'_>,
    sub: &mut SubState,
    commands: &mut mpsc::Receiver<Command>,
    last_message_time: &mut Option<Instant>,
) -> SessionOutcome {
    let index = context.index;
    let (mut write, mut read) = ws_stream.split();

    // (Re-)subscribe everything we want.
    if !sub.desired.is_empty() || context.lifecycle_listener {
        let assets: Vec<String> = sub.desired.keys().cloned().collect();
        if let Err(e) = send_subscribe(&mut write, sub, &assets).await {
            return SessionOutcome::Error(e);
        }
        // Re-subscribing doesn't add new assets, but it does mean we've
        // sent everything we know about for this session.
        sub.mark_all_subscribed();
    }

    let mut ping_interval = tokio::time::interval(PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the first immediate tick.
    ping_interval.tick().await;

    let mut last_inbound = Instant::now();
    let mut last_ping_sent: Option<Instant> = None;
    let heartbeat_deadline = tokio::time::sleep(PONG_TIMEOUT);
    tokio::pin!(heartbeat_deadline);

    let mut events_buf: Vec<EventRecord> = Vec::new();

    loop {
        tokio::select! {
            // -- Read next frame from the socket -------------------------
            msg = read.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    // Capture wall time before parsing, filtering, queueing, or
                    // any downstream transport work.
                    let timestamp_received_ns = now_ns();
                    let received_at = Instant::now();
                    *last_message_time = Some(received_at);
                    last_inbound = received_at;
                    let stripped = text.trim();
                    if stripped.is_empty() {
                        continue;
                    }
                    if stripped == "PONG" {
                        debug!(conn = index, "PONG received");
                        continue;
                    }
                    match handle_text(
                        stripped,
                        sub,
                        &mut events_buf,
                        &context,
                        timestamp_received_ns,
                    ) {
                        Ok(()) => {
                            for ev in events_buf.drain(..) {
                                match context.event_tx.try_send(ev) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        error!(
                                            conn = index,
                                            capacity = context.event_tx.capacity(),
                                            max_capacity = context.event_tx.max_capacity(),
                                            "[QUEUE-OVERFLOW] event channel full, exiting"
                                        );
                                        std::process::exit(1);
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        return SessionOutcome::ChannelClosed;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                conn = index,
                                error = %e,
                                snippet = %stripped.chars().take(200).collect::<String>(),
                                "parse failed",
                            );
                        }
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    last_inbound = Instant::now();
                    // Protocol-level ping. Reply per spec; this is independent
                    // of the application-level "PING"/"PONG" text exchange.
                    let _ = write.send(Message::Pong(p)).await;
                }
                Some(Ok(Message::Pong(_))) => {
                    last_inbound = Instant::now();
                }
                Some(Ok(Message::Binary(_))) => {
                    last_inbound = Instant::now();
                    debug!(conn = index, "ignoring binary frame");
                }
                Some(Ok(Message::Close(frame))) => {
                    warn!(conn = index, ?frame, "ws closed by peer");
                    return SessionOutcome::Closed;
                }
                Some(Ok(Message::Frame(_))) => {
                    last_inbound = Instant::now();
                    // Raw frames are not expected from a tungstenite stream
                    // unless we explicitly ask. Ignore defensively.
                }
                Some(Err(e)) => {
                    warn!(conn = index, error = %e, "ws read error");
                    return SessionOutcome::Closed;
                }
                None => {
                    warn!(conn = index, "ws stream ended");
                    return SessionOutcome::Closed;
                }
            },

            // -- Heartbeat deadline ------------------------------------
            _ = &mut heartbeat_deadline, if last_ping_sent.is_some() => {
                let sent_at = last_ping_sent
                    .take()
                    .expect("heartbeat deadline is armed only after a ping");
                if heartbeat_expired(last_inbound, sent_at, Instant::now()) {
                    warn!(
                        conn = index,
                        since_inbound_secs = last_inbound.elapsed().as_secs_f64(),
                        "heartbeat timeout, closing dead connection",
                    );
                    let _ = write.send(Message::Close(None)).await;
                    return SessionOutcome::HeartbeatTimeout;
                }
            }

            // -- PING ticker --------------------------------------------
            _ = ping_interval.tick() => {
                if let Err(e) = write.send(Message::Text("PING".into())).await {
                    warn!(conn = index, error = %e, "send PING failed");
                    return SessionOutcome::Closed;
                }
                let sent_at = Instant::now();
                last_ping_sent = Some(sent_at);
                heartbeat_deadline
                    .as_mut()
                    .reset(tokio::time::Instant::now() + PONG_TIMEOUT);
            }

            // -- Pool commands ------------------------------------------
            cmd = commands.recv() => match cmd {
                Some(command) => {
                    if let Err(error) = apply_command(&mut write, sub, command).await {
                        warn!(conn = index, %error, "WebSocket command write failed; reconnecting");
                        return SessionOutcome::Closed;
                    }
                }
                None => {
                    let _ = write.send(Message::Close(None)).await;
                    return SessionOutcome::Shutdown;
                }
            }
        }
    }
}

async fn apply_command<S>(write: &mut S, sub: &mut SubState, command: Command) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::fmt::Display,
{
    match command {
        Command::Subscribe(assets) => {
            if assets.is_empty() {
                return Ok(());
            }
            // Update durable intent and compute the current-session diff.
            let mut new_in_session = Vec::new();
            for asset in &assets {
                if sub.subscribe_for_session(asset.clone()) {
                    // A new subscription needs its own fresh snapshot, even if
                    // this asset was subscribed earlier in the same socket
                    // session and then removed.
                    sub.require_fresh_snapshot(asset);
                    new_in_session.push(asset.clone());
                }
            }
            if new_in_session.is_empty() {
                return Ok(());
            }
            send_subscribe(write, sub, &new_in_session)
                .await
                .context("subscribe command write failed")
        }
        Command::Unsubscribe(assets) => {
            if assets.is_empty() {
                return Ok(());
            }
            let mut removed = Vec::new();
            for asset in &assets {
                if sub.unsubscribe(asset) {
                    removed.push(asset.clone());
                }
            }
            if removed.is_empty() {
                return Ok(());
            }
            send_unsubscribe(write, &removed)
                .await
                .context("unsubscribe command write failed")
        }
    }
}

fn heartbeat_expired(last_inbound: Instant, ping_sent: Instant, now: Instant) -> bool {
    last_inbound < ping_sent && now.duration_since(ping_sent) >= PONG_TIMEOUT
}

/// Parse a text frame, explode it, filter by `desired`, and append the
/// surviving events to `events_buf`.
fn handle_text(
    text: &str,
    sub: &mut SubState,
    events_buf: &mut Vec<EventRecord>,
    context: &SessionContext<'_>,
    timestamp_received_ns: i64,
) -> Result<()> {
    let frame: serde_json::Value = serde_json::from_str(text).context("parse wire frame")?;
    let messages = match frame {
        serde_json::Value::Array(messages) => messages,
        message => vec![message],
    };
    for (message_index, raw_value) in messages.into_iter().enumerate() {
        let msg: WireMessage = match serde_json::from_value(raw_value) {
            Ok(message) => message,
            Err(error) => {
                error!(
                    conn = context.index,
                    message_index,
                    %error,
                    "wire parent rejected",
                );
                continue;
            }
        };
        let mut staged: Vec<Event> = Vec::new();
        explode(msg, &mut staged);
        for ev in staged {
            if let Some(asset_id) = ev.asset_id() {
                if !sub.contains(asset_id) {
                    continue;
                }
                match &ev {
                    Event::Book { .. } => {
                        sub.initialize(asset_id);
                    }
                    Event::PriceChange { .. } if !sub.initialized(asset_id) => {
                        debug!(
                            conn = context.index,
                            asset_id, "dropping price delta before fresh book snapshot"
                        );
                        continue;
                    }
                    _ => {}
                }
                events_buf.push(context.collector.record(ev, timestamp_received_ns));
                continue;
            }

            let authoritative = match &ev {
                Event::NewMarket { .. } => context.lifecycle_listener,
                Event::MarketResolved { assets_ids, .. } => {
                    assets_ids.iter().any(|asset| sub.contains(asset))
                        || (assets_ids.is_empty() && context.lifecycle_listener)
                }
                _ => unreachable!("every token event has an asset ID"),
            };
            if !authoritative {
                continue;
            }

            if let Some(lifecycle_tx) = context.lifecycle_tx {
                match lifecycle_tx.try_send(MarketLifecycleObservation {
                    event: ev,
                    timestamp_received_ns,
                }) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        error!(
                            conn = context.index,
                            capacity = lifecycle_tx.capacity(),
                            max_capacity = lifecycle_tx.max_capacity(),
                            "[QUEUE-OVERFLOW] market lifecycle channel full, exiting"
                        );
                        std::process::exit(1);
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        error!(
                            conn = context.index,
                            "market lifecycle controller stopped; exiting before losing data"
                        );
                        std::process::exit(1);
                    }
                }
            } else {
                events_buf.push(context.collector.record(ev, timestamp_received_ns));
            }
        }
    }
    Ok(())
}

async fn send_subscribe<S>(write: &mut S, sub: &mut SubState, assets: &[String]) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::fmt::Display,
{
    let payload = subscribe_payload(sub, assets);
    let text = payload.to_string();
    write
        .send(Message::Text(text.into()))
        .await
        .map_err(|e| anyhow::anyhow!("ws send: {e}"))?;
    Ok(())
}

fn subscribe_payload(sub: &mut SubState, assets: &[String]) -> serde_json::Value {
    if !sub.initial_sent {
        sub.initial_sent = true;
        serde_json::json!({
            "assets_ids": assets,
            "type": "market",
            "custom_feature_enabled": true,
        })
    } else {
        serde_json::json!({
            "assets_ids": assets,
            "operation": "subscribe",
            "custom_feature_enabled": true,
        })
    }
}

async fn send_unsubscribe<S>(write: &mut S, assets: &[String]) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::fmt::Display,
{
    let payload = serde_json::json!({"assets_ids": assets, "operation": "unsubscribe"});
    let text = payload.to_string();
    write
        .send(Message::Text(text.into()))
        .await
        .map_err(|e| anyhow::anyhow!("ws send: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingSink;

    impl futures_util::Sink<Message> for FailingSink {
        type Error = &'static str;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            let _ = self;
            std::task::Poll::Ready(Ok(()))
        }

        fn start_send(
            self: std::pin::Pin<&mut Self>,
            _item: Message,
        ) -> std::result::Result<(), Self::Error> {
            let _ = self;
            Err("simulated write failure")
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            let _ = self;
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            let _ = self;
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn handle(text: &str, sub: &mut SubState, events: &mut Vec<EventRecord>) -> Result<()> {
        let (event_tx, _event_rx) = mpsc::channel(1);
        let collector = CollectorContext::new();
        let context = SessionContext {
            index: 0,
            event_tx: &event_tx,
            collector: &collector,
            lifecycle_listener: false,
            lifecycle_tx: None,
        };
        handle_text(text, sub, events, &context, 123)
    }

    fn sub_state(desired: &[&str]) -> SubState {
        let mut s = SubState::default();
        s.connected();
        for a in desired {
            s.subscribe((*a).into());
            s.initialize(a);
        }
        s
    }

    fn health_state() -> (SubState, Arc<ConnMetrics>, Arc<HealthCounters>) {
        let metrics = Arc::new(ConnMetrics::default());
        let counters = Arc::new(HealthCounters::default());
        (
            SubState::new(Arc::clone(&metrics), Arc::clone(&counters)),
            metrics,
            counters,
        )
    }

    #[test]
    fn readiness_and_recovery_follow_fresh_books_across_repeated_failures() {
        let (mut sub, metrics, counters) = health_state();
        sub.subscribe("a".into());
        sub.subscribe("b".into());
        let start = Instant::now();

        sub.connected();
        sub.initialize_at("a", start);
        sub.initialize_at("a", start);
        assert_eq!(
            metrics.ready_assets(),
            1,
            "a repeated book is not a transition"
        );

        assert_eq!(sub.disconnected_at(start + Duration::from_micros(10)), 1);
        assert_eq!(metrics.ready_assets(), 0);
        assert_eq!(counters.conn_down_events.load(Ordering::Relaxed), 1);
        assert_eq!(counters.asset_down_events.load(Ordering::Relaxed), 1);

        assert_eq!(
            sub.disconnected_at(start + Duration::from_micros(20)),
            0,
            "a repeated failure cannot restart recovery clocks"
        );
        sub.connected();
        sub.initialize_at("b", start + Duration::from_micros(20));
        assert_eq!(metrics.ready_assets(), 1);
        assert_eq!(sub.disconnected_at(start + Duration::from_micros(30)), 1);
        assert_eq!(counters.conn_down_events.load(Ordering::Relaxed), 2);
        assert_eq!(counters.asset_down_events.load(Ordering::Relaxed), 2);

        sub.connected();
        sub.initialize_at("a", start + Duration::from_micros(310));
        sub.initialize_at("b", start + Duration::from_micros(130));
        assert_eq!(metrics.ready_assets(), 2);
        let (total, window) = counters.take_recovery_window();
        assert_eq!(total, 2);
        assert_eq!(window.count, 2);
        assert_eq!(window.latency_us, 400);
        assert_eq!(window.latency_us_max, 300);
        let (total, next_window) = counters.take_recovery_window();
        assert_eq!(total, 2);
        assert_eq!(next_window.count, 0);
        assert_eq!(next_window.latency_us, 0);
        assert_eq!(next_window.latency_us_max, 0);
    }

    #[test]
    fn intentional_stop_does_not_count_as_a_data_gap() {
        let (mut sub, metrics, counters) = health_state();
        sub.subscribe_for_session("a".into());
        sub.connected();
        sub.initialize("a");

        sub.stop();

        assert!(!metrics.is_connected());
        assert_eq!(metrics.ready_assets(), 0);
        assert!(sub.desired["a"].subscribed);
        assert!(sub.initialized("a"));
        assert_eq!(counters.conn_down_events.load(Ordering::Relaxed), 0);
        assert_eq!(counters.asset_down_events.load(Ordering::Relaxed), 0);
        let (total, window) = counters.take_recovery_window();
        assert_eq!(total, 0);
        assert_eq!(window.count, 0);
    }

    #[tokio::test]
    async fn websocket_handshake_is_bounded() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let stalled_server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let error =
            match connect_websocket(&format!("ws://{address}"), Duration::from_millis(25)).await {
                Ok(_) => panic!("stalled WebSocket handshake unexpectedly succeeded"),
                Err(error) => error,
            };
        assert!(error.to_string().contains("connection timed out"));

        stalled_server.abort();
        let _ = stalled_server.await;
    }

    #[test]
    fn heartbeat_expires_without_an_inbound_frame() {
        let now = Instant::now();
        let sent_at = now - PONG_TIMEOUT;
        let last_inbound = sent_at - Duration::from_millis(1);

        assert!(heartbeat_expired(last_inbound, sent_at, now));
    }

    #[test]
    fn market_frame_after_ping_keeps_heartbeat_alive() {
        let now = Instant::now();
        let sent_at = now - PONG_TIMEOUT;
        let last_inbound = sent_at + Duration::from_millis(1);

        assert!(!heartbeat_expired(last_inbound, sent_at, now));
    }

    #[test]
    fn heartbeat_waits_for_its_deadline() {
        let now = Instant::now();
        let sent_at = now - PONG_TIMEOUT + Duration::from_millis(1);
        let last_inbound = sent_at - Duration::from_millis(1);

        assert!(!heartbeat_expired(last_inbound, sent_at, now));
    }

    #[test]
    fn handle_text_filters_undesired_book_event() {
        let mut buf = Vec::new();
        let mut sub = sub_state(&["wanted"]);
        let raw = r#"{
            "event_type": "book", "asset_id": "not-wanted", "market": "m",
            "bids": [], "asks": [], "timestamp": "1", "hash": "h"
        }"#;
        handle(raw, &mut sub, &mut buf).unwrap();
        assert_eq!(buf.len(), 0, "undesired asset should be filtered");
    }

    #[test]
    fn handle_text_keeps_desired_book_event() {
        let mut buf = Vec::new();
        let mut sub = sub_state(&["wanted"]);
        let raw = r#"{
            "event_type": "book", "asset_id": "wanted", "market": "m",
            "bids": [{"price": "0.4", "size": "10"}],
            "asks": [{"price": "0.5", "size": "10"}],
            "timestamp": "1", "hash": "h"
        }"#;
        handle(raw, &mut sub, &mut buf).unwrap();
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn handle_text_explodes_price_change_and_filters_per_entry() {
        let mut buf = Vec::new();
        let mut sub = sub_state(&["a1"]);
        let raw = r#"{
            "event_type": "price_change", "market": "m", "timestamp": "1",
            "price_changes": [
                {"asset_id": "a1", "price": "0.4", "size": "10", "side": "BUY", "hash": "h1"},
                {"asset_id": "a2", "price": "0.5", "size": "10", "side": "SELL", "hash": "h2"},
                {"asset_id": "a1", "price": "0.6", "size": "10", "side": "BUY", "hash": "h3"}
            ]
        }"#;
        handle(raw, &mut sub, &mut buf).unwrap();
        assert_eq!(buf.len(), 2, "only a1 entries should survive filter");
    }

    #[test]
    fn handle_text_requires_fresh_book_before_price_changes() {
        let mut buf = Vec::new();
        let mut sub = sub_state(&["a"]);
        sub.disconnected();

        let price_change = r#"{
            "event_type": "price_change", "market": "m", "timestamp": "1",
            "price_changes": [
                {"asset_id": "a", "price": "0.4", "size": "10", "side": "BUY", "hash": "h"}
            ]
        }"#;
        handle(price_change, &mut sub, &mut buf).unwrap();
        assert!(buf.is_empty(), "delta before snapshot must be discarded");

        let book = r#"{
            "event_type": "book", "asset_id": "a", "market": "m",
            "bids": [], "asks": [], "timestamp": "2", "hash": "h"
        }"#;
        sub.connected();
        handle(book, &mut sub, &mut buf).unwrap();
        handle(price_change, &mut sub, &mut buf).unwrap();
        assert_eq!(
            buf.len(),
            2,
            "snapshot followed by delta is reconstructible"
        );

        sub.disconnected();
        handle(price_change, &mut sub, &mut buf).unwrap();
        assert_eq!(buf.len(), 2, "reconnect must require another snapshot");
    }

    #[test]
    fn handle_text_handles_array_frame() {
        let mut buf = Vec::new();
        let mut sub = sub_state(&["a"]);
        let raw = r#"[
            {"event_type": "tick_size_change", "asset_id": "a", "market": "m",
             "old_tick_size": "0.01", "new_tick_size": "0.001", "timestamp": "1"},
            {"event_type": "tick_size_change", "asset_id": "a", "market": "m",
             "old_tick_size": "0.02", "new_tick_size": "0.002", "timestamp": "2"}
        ]"#;
        handle(raw, &mut sub, &mut buf).unwrap();
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn handle_text_keeps_valid_parents_around_an_unsupported_parent() {
        let mut buf = Vec::new();
        let mut sub = sub_state(&["a"]);
        let raw = r#"[
            {"event_type": "tick_size_change", "asset_id": "a", "market": "m",
             "old_tick_size": "0.01", "new_tick_size": "0.001", "timestamp": "1"},
            {"event_type": "future_event", "asset_id": "a", "market": "m",
             "timestamp": "2"},
            {"event_type": "tick_size_change", "asset_id": "a", "market": "m",
             "old_tick_size": "0.02", "new_tick_size": "0.002", "timestamp": "3"}
        ]"#;

        handle(raw, &mut sub, &mut buf).unwrap();

        assert_eq!(buf.len(), 2);
        assert_eq!(buf[1].sequence, buf[0].sequence + 1);
    }

    #[test]
    fn handle_text_keeps_best_bid_ask_for_desired_asset() {
        let mut buf = Vec::new();
        let mut sub = sub_state(&["a"]);
        let raw = r#"{
            "event_type": "best_bid_ask", "asset_id": "a", "market": "m",
            "best_bid": "0.4", "best_ask": "0.5", "spread": "0.1",
            "timestamp": "1"
        }"#;

        handle(raw, &mut sub, &mut buf).unwrap();

        assert_eq!(buf.len(), 1);
        assert!(matches!(buf[0].event, Event::BestBidAsk { .. }));
    }

    #[test]
    fn only_lifecycle_listener_keeps_new_market_events() {
        let raw = r#"{
            "event_type": "new_market", "id": "1", "market": "m",
            "assets_ids": ["yes", "no"], "outcomes": ["Yes", "No"],
            "timestamp": "1"
        }"#;
        let mut sub = sub_state(&[]);
        let mut buf = Vec::new();
        handle(raw, &mut sub, &mut buf).unwrap();
        assert!(buf.is_empty());

        let (event_tx, _event_rx) = mpsc::channel(1);
        let collector = CollectorContext::new();
        let context = SessionContext {
            index: 0,
            event_tx: &event_tx,
            collector: &collector,
            lifecycle_listener: true,
            lifecycle_tx: None,
        };
        handle_text(raw, &mut sub, &mut buf, &context, 123).unwrap();
        assert_eq!(buf.len(), 1);
        assert!(matches!(buf[0].event, Event::NewMarket { .. }));
    }

    #[test]
    fn lifecycle_listener_forwards_full_observation() {
        let raw = r#"{
            "event_type": "new_market", "id": "1", "market": "m",
            "assets_ids": ["yes", "no"], "outcomes": ["Yes", "No"],
            "timestamp": "1"
        }"#;
        let mut sub = sub_state(&[]);
        let mut buf = Vec::new();
        let (event_tx, _event_rx) = mpsc::channel(1);
        let (lifecycle_tx, mut lifecycle_rx) = mpsc::channel(8);
        let collector = CollectorContext::new();
        let context = SessionContext {
            index: 0,
            event_tx: &event_tx,
            collector: &collector,
            lifecycle_listener: true,
            lifecycle_tx: Some(&lifecycle_tx),
        };

        handle_text(raw, &mut sub, &mut buf, &context, 123).unwrap();

        let observation = lifecycle_rx.try_recv().unwrap();
        assert_eq!(observation.timestamp_received_ns, 123);
        assert!(matches!(
            observation.event,
            Event::NewMarket { ref market, .. } if market == "m"
        ));
        assert!(buf.is_empty(), "the central controller stores the event");
    }

    #[test]
    fn subscribed_asset_socket_forwards_market_resolved() {
        let raw = r#"{
            "event_type": "market_resolved", "id": "1", "market": "m",
            "assets_ids": ["yes", "no"], "winning_asset_id": "yes",
            "winning_outcome": "Yes", "timestamp": "1"
        }"#;
        let mut sub = sub_state(&["yes", "no"]);
        let mut buf = Vec::new();
        let (event_tx, _event_rx) = mpsc::channel(1);
        let (lifecycle_tx, mut lifecycle_rx) = mpsc::channel(8);
        let collector = CollectorContext::new();
        let context = SessionContext {
            index: 17,
            event_tx: &event_tx,
            collector: &collector,
            lifecycle_listener: false,
            lifecycle_tx: Some(&lifecycle_tx),
        };

        handle_text(raw, &mut sub, &mut buf, &context, 456).unwrap();

        let observation = lifecycle_rx.try_recv().unwrap();
        assert_eq!(observation.timestamp_received_ns, 456);
        assert!(matches!(observation.event, Event::MarketResolved { .. }));
        assert!(buf.is_empty());
    }

    #[test]
    fn unrelated_asset_socket_drops_market_resolved() {
        let raw = r#"{
            "event_type": "market_resolved", "id": "1", "market": "m",
            "assets_ids": ["yes", "no"], "winning_asset_id": "yes",
            "winning_outcome": "Yes", "timestamp": "1"
        }"#;
        let mut sub = sub_state(&["other"]);
        let mut buf = Vec::new();
        let (event_tx, _event_rx) = mpsc::channel(1);
        let (lifecycle_tx, mut lifecycle_rx) = mpsc::channel(8);
        let collector = CollectorContext::new();
        let context = SessionContext {
            index: 18,
            event_tx: &event_tx,
            collector: &collector,
            lifecycle_listener: false,
            lifecycle_tx: Some(&lifecycle_tx),
        };

        handle_text(raw, &mut sub, &mut buf, &context, 456).unwrap();

        assert!(lifecycle_rx.try_recv().is_err());
        assert!(buf.is_empty());
    }

    #[test]
    fn every_subscription_enables_custom_market_events() {
        let mut sub = SubState::default();
        let initial = subscribe_payload(&mut sub, &["a".into()]);
        assert_eq!(initial["type"], "market");
        assert_eq!(initial["custom_feature_enabled"], true);

        let dynamic = subscribe_payload(&mut sub, &["b".into()]);
        assert_eq!(dynamic["operation"], "subscribe");
        assert_eq!(dynamic["custom_feature_enabled"], true);
    }

    #[test]
    fn lifecycle_listener_can_subscribe_without_assets() {
        let mut sub = SubState::default();
        let initial = subscribe_payload(&mut sub, &[]);
        assert_eq!(initial["assets_ids"], serde_json::json!([]));
        assert_eq!(initial["type"], "market");
        assert_eq!(initial["custom_feature_enabled"], true);
    }

    #[tokio::test]
    async fn failed_live_subscribe_preserves_desired_for_reconnect() {
        let mut write = FailingSink;
        let mut sub = SubState::default();
        sub.connected();
        sub.subscribe("a".into());
        sub.initialize("a");

        let error = apply_command(&mut write, &mut sub, Command::Subscribe(vec!["a".into()]))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("subscribe command write failed"));
        assert!(sub.contains("a"));
        assert!(sub.desired["a"].subscribed);
        assert!(!sub.initialized("a"));
        sub.disconnected();
        assert!(sub.contains("a"));
        assert!(!sub.desired["a"].subscribed);
        assert!(!sub.initialized("a"));
        assert!(!sub.initial_sent);
    }

    #[tokio::test]
    async fn failed_live_unsubscribe_reconstructs_remaining_desired_assets() {
        let mut write = FailingSink;
        let mut sub = sub_state(&["a", "b"]);
        sub.subscribe_for_session("a".into());
        sub.subscribe_for_session("b".into());
        sub.initial_sent = true;

        let error = apply_command(&mut write, &mut sub, Command::Unsubscribe(vec!["a".into()]))
            .await
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("unsubscribe command write failed"));
        assert!(!sub.contains("a"));
        assert!(sub.contains("b"));
        sub.disconnected();
        assert!(!sub.contains("a"));
        assert!(sub.contains("b"));
        assert!(!sub.desired["b"].subscribed);
        assert!(!sub.initialized("b"));
        assert!(!sub.initial_sent);
    }

    #[tokio::test]
    async fn outer_loop_check_does_not_consume_buffered_subscribe() {
        // Regression: an earlier version used `try_recv()` to detect
        // shutdown during the reconnect delay, which silently consumed the
        // pool's pre-loaded initial `Subscribe` command. The connection
        // would then connect to Polymarket but never subscribe, and the
        // server would reset the socket after ~5s. The check must use a
        // non-consuming primitive.
        let (tx, mut rx) = mpsc::channel::<Command>(8);
        tx.send(Command::Subscribe(vec!["a1".into(), "a2".into()]))
            .await
            .unwrap();
        // Simulate the outer-loop shutdown check from `Connection::run`.
        let should_exit = rx.is_closed() && rx.is_empty();
        assert!(!should_exit, "channel has buffered work and a live sender");
        // The buffered Subscribe must still be there.
        match rx.try_recv() {
            Ok(Command::Subscribe(assets)) => assert_eq!(assets, vec!["a1", "a2"]),
            other => panic!("expected buffered Subscribe, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn outer_loop_check_exits_when_senders_dropped_and_empty() {
        let (tx, rx) = mpsc::channel::<Command>(8);
        drop(tx);
        let should_exit = rx.is_closed() && rx.is_empty();
        assert!(should_exit);
    }

    #[tokio::test]
    async fn outer_loop_check_keeps_running_with_buffered_work_after_shutdown() {
        // Pool may drop the sender while messages are still buffered. We
        // must NOT exit early in that case — the inner loop will drain
        // them and then return when recv() yields None.
        let (tx, rx) = mpsc::channel::<Command>(8);
        tx.send(Command::Unsubscribe(vec!["a1".into()]))
            .await
            .unwrap();
        drop(tx);
        let should_exit = rx.is_closed() && rx.is_empty();
        assert!(
            !should_exit,
            "must not exit while buffered work remains, even with senders dropped",
        );
    }

    #[test]
    fn reconnect_jitter_is_bounded_stable_and_distributed() {
        let first = reconnect_jitter(42, 3);
        assert_eq!(first, reconnect_jitter(42, 3));
        assert!(first <= Duration::from_millis(RECONNECT_JITTER_MAX_MS));
        assert_ne!(first, reconnect_jitter(43, 3));
        assert_ne!(first, reconnect_jitter(42, 4));
    }
}
