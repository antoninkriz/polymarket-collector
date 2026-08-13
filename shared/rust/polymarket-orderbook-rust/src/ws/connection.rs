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
//! run loop reconnects immediately (no backoff). Other failures use
//! exponential backoff (1 → 60 s).
//!
//! ## Subscription state
//!
//! - `desired: HashSet<String>` — durable intent across reconnects, used to
//!   re-subscribe after a drop and to filter incoming events.
//! - `subscribed: HashSet<String>` — what has been sent to the server in
//!   the current session. Cleared on every reconnect so the diff in
//!   `subscribe()` re-sends everything.
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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::events::{explode, Event, MarketLifecycle, WireMessage};
use crate::record::{now_ns, CollectorContext, EventRecord};
use crate::ws::WS_MARKET_URL;

/// Liveness state of a single WebSocket connection. Published by the
/// connection task on every transition and observed by the pool's health
/// monitor to track asset-level up/down status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnStatus {
    /// Socket is open and (re-)subscribe has been written. Events should be
    /// flowing (or are about to).
    Connected,
    /// No live socket. Either we haven't connected yet, the previous session
    /// ended, or we're inside the reconnect backoff.
    Disconnected,
}

/// Per Polymarket docs (Market & User channels): client sends `"PING"`
/// every 10 seconds. We deviated to 5 s historically, matching a stale
/// docstring in the Python service — see the module-level docs.
const PING_INTERVAL: Duration = Duration::from_secs(10);
const PONG_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(60);

/// Commands sent from the pool into a connection task. Shutdown is signalled
/// by closing the command channel (dropping every sender), which makes
/// `recv()` return `None`; no explicit `Shutdown` variant is needed.
#[derive(Debug)]
pub enum Command {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

pub struct Connection {
    pub index: usize,
    pub event_tx: mpsc::Sender<EventRecord>,
    pub collector: Arc<CollectorContext>,
    pub status_tx: mpsc::UnboundedSender<(usize, ConnStatus)>,
    /// Lifecycle messages are broadcast to every custom-enabled socket. Only
    /// the pool's designated leader admits them to storage.
    pub lifecycle_leader: bool,
    pub lifecycle_tx: Option<mpsc::UnboundedSender<MarketLifecycle>>,
}

impl Connection {
    pub fn new(
        index: usize,
        event_tx: mpsc::Sender<EventRecord>,
        collector: Arc<CollectorContext>,
        status_tx: mpsc::UnboundedSender<(usize, ConnStatus)>,
        lifecycle_leader: bool,
        lifecycle_tx: Option<mpsc::UnboundedSender<MarketLifecycle>>,
    ) -> Self {
        Self {
            index,
            event_tx,
            collector,
            status_tx,
            lifecycle_leader,
            lifecycle_tx,
        }
    }

    /// Run the connect → listen → reconnect loop until the command channel
    /// is closed. Returns an error only
    /// if the event channel sender is dropped (i.e. the sink died) — in
    /// that case the caller should shut down all connections.
    ///
    /// Publishes [`ConnStatus`] transitions on `status_tx` so the pool's
    /// health monitor can track asset-level health. The watch is
    /// *edge-triggered*: we only `send` on a genuine transition, not on
    /// every loop iteration.
    pub async fn run(self, mut commands: mpsc::Receiver<Command>) -> Result<()> {
        let Connection {
            index,
            event_tx,
            collector,
            status_tx,
            lifecycle_leader,
            lifecycle_tx,
        } = self;
        let mut sub = SubState::default();
        let mut backoff = Duration::from_secs(1);
        let mut last_message_time: Option<Instant> = None;
        let mut heartbeat_timed_out = false;
        let mut first_attempt = true;
        loop {
            // Reconnect delay (skip on first attempt and on PONG timeout).
            if first_attempt {
                first_attempt = false;
            } else if heartbeat_timed_out {
                heartbeat_timed_out = false;
                info!(conn = index, "heartbeat timeout — reconnecting immediately");
            } else {
                info!(
                    conn = index,
                    delay_ms = backoff.as_millis() as u64,
                    "reconnecting"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_DELAY_MAX);
            }

            // Allow shutdown to short-circuit during the reconnect delay.
            // Shutdown is signalled by the command channel being closed
            // (all senders dropped) AND the buffer drained. We use a
            // non-consuming check (`is_closed` + `is_empty`) because
            // `try_recv()` would consume buffered commands like the
            // pool's pre-loaded initial Subscribe and silently drop them.
            if commands.is_closed() && commands.is_empty() {
                info!(conn = index, "shutdown requested during reconnect delay");
                let _ = status_tx.send((index, ConnStatus::Disconnected));
                return Ok(());
            }

            // Connect.
            let ws_stream = match tokio_tungstenite::connect_async(WS_MARKET_URL).await {
                Ok((stream, _resp)) => {
                    info!(conn = index, "ws connected");
                    stream
                }
                Err(e) => {
                    error!(conn = index, error = %e, "ws connect failed");
                    // Stay in Disconnected; loop and retry with backoff.
                    let _ = status_tx.send((index, ConnStatus::Disconnected));
                    continue;
                }
            };
            // Reset session state. `desired` survives, `subscribed` does not.
            sub.reset_session();

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

            // We're connected; publish before entering the session loop so
            // the pool's health monitor sees us go up immediately.
            let _ = status_tx.send((index, ConnStatus::Connected));

            // Run a single connected session. A heartbeat timeout skips the
            // next reconnect backoff.
            let context = SessionContext {
                index,
                event_tx: &event_tx,
                collector: &collector,
                lifecycle_leader,
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

            // Session ended for any reason → we're no longer Connected.
            // Always publish before deciding what to do next.
            let _ = status_tx.send((index, ConnStatus::Disconnected));

            match outcome {
                SessionOutcome::HeartbeatTimeout => {
                    heartbeat_timed_out = true;
                    backoff = Duration::from_secs(1);
                }
                SessionOutcome::Closed => {
                    backoff = Duration::from_secs(1);
                }
                SessionOutcome::ChannelClosed => {
                    info!(conn = index, "event sink closed, shutting down connection");
                    return Ok(());
                }
                SessionOutcome::Shutdown => {
                    info!(conn = index, "shutdown requested");
                    return Ok(());
                }
                SessionOutcome::Error(e) => {
                    error!(conn = index, error = %e, "session error, will reconnect");
                }
            }
        }
    }
}

/// Subscription state: durable intent and per-session reconstruction state.
#[derive(Default)]
pub(crate) struct SubState {
    pub desired: HashSet<String>,
    pub subscribed: HashSet<String>,
    /// Assets for which this socket session has delivered a fresh book.
    /// Price deltas are not reconstructible until that snapshot arrives.
    pub initialized: HashSet<String>,
    /// Whether the initial subscribe message has been sent on the current
    /// session. The first message uses `{"type": "market"}`; subsequent
    /// ones use `{"operation": "subscribe"}`.
    pub initial_sent: bool,
}

impl SubState {
    pub fn reset_session(&mut self) {
        self.subscribed.clear();
        self.initialized.clear();
        self.initial_sent = false;
    }
}

#[derive(Debug)]
enum SessionOutcome {
    /// Session ended because no frame followed a heartbeat. Reconnect immediately.
    HeartbeatTimeout,
    /// Session ended due to a remote close or read EOF. Reconnect with backoff.
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
    lifecycle_leader: bool,
    lifecycle_tx: Option<&'a mpsc::UnboundedSender<MarketLifecycle>>,
}

/// Run a single connected WebSocket session: send the initial subscription,
/// then multiplex reads, ping ticker, and pool commands until something
/// breaks. Updates `last_message_time` whenever a frame is received.
async fn run_session(
    ws_stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    context: SessionContext<'_>,
    sub: &mut SubState,
    commands: &mut mpsc::Receiver<Command>,
    last_message_time: &mut Option<Instant>,
) -> SessionOutcome {
    let index = context.index;
    let (mut write, mut read) = ws_stream.split();

    // (Re-)subscribe everything we want.
    if !sub.desired.is_empty() || context.lifecycle_leader {
        let assets: Vec<String> = sub.desired.iter().cloned().collect();
        if let Err(e) = send_subscribe(&mut write, sub, &assets, index).await {
            return SessionOutcome::Error(e);
        }
        // Re-subscribing doesn't add new assets, but it does mean we've
        // sent everything we know about for this session.
        sub.subscribed.extend(sub.desired.iter().cloned());
    }

    let mut ping_interval = tokio::time::interval(PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the first immediate tick.
    ping_interval.tick().await;

    let mut last_inbound = Instant::now();
    let mut last_ping_sent: Option<Instant> = None;

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

            // -- PING ticker --------------------------------------------
            _ = ping_interval.tick() => {
                // A PONG is the normal response, but any later inbound frame
                // proves this socket is alive. Requiring the textual PONG
                // alone creates false reconnects on busy market streams.
                if let Some(sent_at) = last_ping_sent {
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
                if let Err(e) = write.send(Message::Text("PING".into())).await {
                    warn!(conn = index, error = %e, "send PING failed");
                    return SessionOutcome::Closed;
                }
                last_ping_sent = Some(Instant::now());
            }

            // -- Pool commands ------------------------------------------
            cmd = commands.recv() => match cmd {
                Some(Command::Subscribe(assets)) => {
                    if assets.is_empty() { continue; }
                    // Update desired (durable) and compute the diff against
                    // the per-session subscribed set.
                    let mut new_in_session: Vec<String> = Vec::new();
                    for a in &assets {
                        if sub.desired.insert(a.clone()) {
                            // newly desired
                        }
                        if sub.subscribed.insert(a.clone()) {
                            // A new subscription needs its own fresh snapshot,
                            // even if this asset was subscribed earlier in the
                            // same socket session and then removed.
                            sub.initialized.remove(a);
                            new_in_session.push(a.clone());
                        }
                    }
                    if new_in_session.is_empty() {
                        continue;
                    }
                    if let Err(e) = send_subscribe(&mut write, sub, &new_in_session, index).await {
                        warn!(conn = index, error = %e, "subscribe send failed; will retry on reconnect");
                    }
                }
                Some(Command::Unsubscribe(assets)) => {
                    if assets.is_empty() { continue; }
                    let mut removed: Vec<String> = Vec::new();
                    for a in &assets {
                        if sub.desired.remove(a) {
                            sub.subscribed.remove(a);
                            sub.initialized.remove(a);
                            removed.push(a.clone());
                        }
                    }
                    if removed.is_empty() {
                        continue;
                    }
                    if let Err(e) = send_unsubscribe(&mut write, &removed, index).await {
                        warn!(conn = index, error = %e, "unsubscribe send failed");
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
            let lifecycle = if let Some(asset_id) = ev.asset_id() {
                if !sub.desired.contains(asset_id) {
                    continue;
                }
                match &ev {
                    Event::Book { .. } => {
                        sub.initialized.insert(asset_id.to_owned());
                    }
                    Event::PriceChange { .. } if !sub.initialized.contains(asset_id) => {
                        debug!(
                            conn = context.index,
                            asset_id, "dropping price delta before fresh book snapshot"
                        );
                        continue;
                    }
                    _ => {}
                }
                None
            } else if !context.lifecycle_leader {
                // Lifecycle events are connection-wide when the custom
                // feature is enabled. Admitting them on every data socket
                // would duplicate one upstream notification N times.
                continue;
            } else {
                ev.market_lifecycle()
            };
            events_buf.push(context.collector.record(ev, timestamp_received_ns));
            if let (Some(lifecycle_tx), Some(lifecycle)) = (context.lifecycle_tx, lifecycle) {
                if lifecycle_tx.send(lifecycle).is_err() {
                    warn!(
                        conn = context.index,
                        "market lifecycle controller stopped; event remains stored"
                    );
                }
            }
        }
    }
    Ok(())
}

async fn send_subscribe<S>(
    write: &mut S,
    sub: &mut SubState,
    assets: &[String],
    index: usize,
) -> Result<()>
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
    info!(conn = index, count = assets.len(), "subscribed assets");
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

async fn send_unsubscribe<S>(write: &mut S, assets: &[String], index: usize) -> Result<()>
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
    info!(conn = index, count = assets.len(), "unsubscribed assets");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(text: &str, sub: &mut SubState, events: &mut Vec<EventRecord>) -> Result<()> {
        let (event_tx, _event_rx) = mpsc::channel(1);
        let collector = CollectorContext::new();
        let context = SessionContext {
            index: 0,
            event_tx: &event_tx,
            collector: &collector,
            lifecycle_leader: false,
            lifecycle_tx: None,
        };
        handle_text(text, sub, events, &context, 123)
    }

    fn sub_state(desired: &[&str]) -> SubState {
        let mut s = SubState::default();
        for a in desired {
            s.desired.insert((*a).into());
            s.initialized.insert((*a).into());
        }
        s
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
        sub.reset_session();
        sub.desired.insert("a".into());

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
        handle(book, &mut sub, &mut buf).unwrap();
        handle(price_change, &mut sub, &mut buf).unwrap();
        assert_eq!(
            buf.len(),
            2,
            "snapshot followed by delta is reconstructible"
        );

        sub.reset_session();
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
    fn only_lifecycle_leader_keeps_connection_wide_events() {
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
            lifecycle_leader: true,
            lifecycle_tx: None,
        };
        handle_text(raw, &mut sub, &mut buf, &context, 123).unwrap();
        assert_eq!(buf.len(), 1);
        assert!(matches!(buf[0].event, Event::NewMarket { .. }));
    }

    #[test]
    fn lifecycle_leader_forwards_pool_update() {
        let raw = r#"{
            "event_type": "market_resolved", "id": "1", "market": "m",
            "assets_ids": ["yes", "no"], "winning_asset_id": "yes",
            "winning_outcome": "Yes", "timestamp": "1"
        }"#;
        let mut sub = sub_state(&[]);
        let mut buf = Vec::new();
        let (event_tx, _event_rx) = mpsc::channel(1);
        let (lifecycle_tx, mut lifecycle_rx) = mpsc::unbounded_channel();
        let collector = CollectorContext::new();
        let context = SessionContext {
            index: 0,
            event_tx: &event_tx,
            collector: &collector,
            lifecycle_leader: true,
            lifecycle_tx: Some(&lifecycle_tx),
        };

        handle_text(raw, &mut sub, &mut buf, &context, 123).unwrap();

        assert_eq!(
            lifecycle_rx.try_recv().unwrap(),
            MarketLifecycle::MarketResolved { market: "m".into() }
        );
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
    fn lifecycle_leader_can_subscribe_without_assets() {
        let mut sub = SubState::default();
        let initial = subscribe_payload(&mut sub, &[]);
        assert_eq!(initial["assets_ids"], serde_json::json!([]));
        assert_eq!(initial["type"], "market");
        assert_eq!(initial["custom_feature_enabled"], true);
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
}
