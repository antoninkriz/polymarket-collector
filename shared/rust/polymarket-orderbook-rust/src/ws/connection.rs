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
//! sends `"PING"` every **10 s** and the server responds `"PONG"`. We wait
//! 5 s for the PONG; if it's missed, the socket is closed and the run loop
//! reconnects immediately (no backoff). Other failures use exponential
//! backoff (1 → 60 s).
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
//! - First subscription on a fresh socket: `{"assets_ids": [...], "type": "market"}`
//! - Subsequent subscribes:                 `{"assets_ids": [...], "operation": "subscribe"}`
//! - Unsubscribes:                          `{"assets_ids": [...], "operation": "unsubscribe"}`
//!
//! Messages can arrive as a single JSON object or as a JSON array of objects;
//! [`crate::events::WireFrame`] handles both.
//!
//! ## Failure mode for the event channel
//!
//! Events are stamped at receipt and pushed directly to the global channel.
//! Each market has one authoritative connection, so no payload merge or
//! deduplication occurs here. If the channel is **full**, the consumer is too slow and we
//! exit the process. If the channel is **closed**, the sink has shut down
//! and we return cleanly.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

use crate::events::{explode, Event, WireMessage};
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

/// Stats exposed to the pool for periodic logging. All counters are
/// atomically updated by the connection's task; the pool reads them via
/// `Arc::clone`.
#[derive(Default, Debug)]
pub struct ConnStats {
    pub books: AtomicU64,
    pub price_changes: AtomicU64,
    pub trades: AtomicU64,
    pub tick_changes: AtomicU64,
    pub reconnects: AtomicU64,
    /// Current size of `desired` (durable subscription set).
    pub desired_count: AtomicUsize,
}

impl ConnStats {
    /// Drain per-period counters and return their previous values. Lifetime
    /// counters (`reconnects`, `desired_count`) are not affected.
    ///
    /// Currently unused — `main::stats_loop` no longer logs per-conn stats
    /// because that line spammed at 100+ connections. Kept for the day we
    /// re-enable per-conn logging.
    #[allow(dead_code)]
    pub fn drain_period(&self) -> PeriodStats {
        PeriodStats {
            books: self.books.swap(0, Ordering::Relaxed),
            price_changes: self.price_changes.swap(0, Ordering::Relaxed),
            trades: self.trades.swap(0, Ordering::Relaxed),
            tick_changes: self.tick_changes.swap(0, Ordering::Relaxed),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Default)]
pub struct PeriodStats {
    pub books: u64,
    pub price_changes: u64,
    pub trades: u64,
    pub tick_changes: u64,
}

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
    pub stats: Arc<ConnStats>,
}

impl Connection {
    pub fn new(
        index: usize,
        event_tx: mpsc::Sender<EventRecord>,
        collector: Arc<CollectorContext>,
        status_tx: mpsc::UnboundedSender<(usize, ConnStatus)>,
    ) -> Self {
        Self {
            index,
            event_tx,
            collector,
            status_tx,
            stats: Arc::new(ConnStats::default()),
        }
    }

    /// Run the connect → listen → reconnect loop until `Command::Shutdown`
    /// is received or the command channel is closed. Returns an error only
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
            stats,
        } = self;
        let mut sub = SubState::default();
        let mut backoff = Duration::from_secs(1);
        let mut last_message_time: Option<Instant> = None;
        let mut pong_timed_out = false;
        let mut first_attempt = true;
        let mut connection_epoch = 0_u64;
        let mut frame_sequence = 0_u64;
        loop {
            // Reconnect delay (skip on first attempt and on PONG timeout).
            if first_attempt {
                first_attempt = false;
            } else if pong_timed_out {
                pong_timed_out = false;
                info!(conn = index, "PONG timeout — reconnecting immediately");
            } else {
                info!(conn = index, delay_ms = backoff.as_millis() as u64, "reconnecting");
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
            connection_epoch += 1;

            // Reset session state. `desired` survives, `subscribed` does not.
            sub.reset_session();

            // Record reconnect gap if this isn't the first connection.
            let connect_time = Instant::now();
            if let Some(last) = last_message_time {
                let gap = connect_time.duration_since(last);
                stats.reconnects.fetch_add(1, Ordering::Relaxed);
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

            // Run a single connected session. Returns true if the session
            // ended due to a PONG timeout (so we skip the next backoff).
            let context = SessionContext {
                index,
                connection_id: index as u32,
                connection_epoch,
                event_tx: &event_tx,
                collector: &collector,
                stats: &stats,
            };
            let outcome = run_session(
                ws_stream,
                context,
                &mut sub,
                &mut commands,
                &mut last_message_time,
                &mut frame_sequence,
            )
            .await;

            // Session ended for any reason → we're no longer Connected.
            // Always publish before deciding what to do next.
            let _ = status_tx.send((index, ConnStatus::Disconnected));

            match outcome {
                SessionOutcome::PongTimeout => {
                    pong_timed_out = true;
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

/// Subscription state: durable intent and per-session sent set.
#[derive(Default)]
pub(crate) struct SubState {
    pub desired: HashSet<String>,
    pub subscribed: HashSet<String>,
    /// Whether the initial subscribe message has been sent on the current
    /// session. The first message uses `{"type": "market"}`; subsequent
    /// ones use `{"operation": "subscribe"}`.
    pub initial_sent: bool,
}

impl SubState {
    pub fn reset_session(&mut self) {
        self.subscribed.clear();
        self.initial_sent = false;
    }
}

#[derive(Debug)]
enum SessionOutcome {
    /// Session ended because the PONG heartbeat timed out. Reconnect with no delay.
    PongTimeout,
    /// Session ended due to a remote close or read EOF. Reconnect with backoff.
    Closed,
    /// Event channel closed (sink died). Connection should shut down.
    ChannelClosed,
    /// Pool sent `Command::Shutdown` or the command channel was dropped.
    Shutdown,
    /// Some other error during the session — reconnect with backoff.
    Error(anyhow::Error),
}

#[derive(Clone, Copy)]
struct SessionContext<'a> {
    index: usize,
    connection_id: u32,
    connection_epoch: u64,
    event_tx: &'a mpsc::Sender<EventRecord>,
    collector: &'a CollectorContext,
    stats: &'a ConnStats,
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
    frame_sequence: &mut u64,
) -> SessionOutcome {
    let index = context.index;
    let (mut write, mut read) = ws_stream.split();

    // (Re-)subscribe everything we want.
    if !sub.desired.is_empty() {
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

    let mut last_pong = Instant::now();
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
                    *last_message_time = Some(Instant::now());
                    let stripped = text.trim();
                    if stripped.is_empty() {
                        continue;
                    }
                    if stripped == "PONG" {
                        last_pong = Instant::now();
                        debug!(conn = index, "PONG received");
                        continue;
                    }
                    let this_frame_sequence = *frame_sequence;
                    *frame_sequence = frame_sequence.wrapping_add(1);
                    match handle_text(
                        stripped,
                        sub,
                        &mut events_buf,
                        &context,
                        this_frame_sequence,
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
                    // Protocol-level ping. Reply per spec; this is independent
                    // of the application-level "PING"/"PONG" text exchange.
                    let _ = write.send(Message::Pong(p)).await;
                }
                Some(Ok(Message::Pong(_))) => {
                    // Protocol-level pong. Ignore.
                }
                Some(Ok(Message::Binary(_))) => {
                    debug!(conn = index, "ignoring binary frame");
                }
                Some(Ok(Message::Close(frame))) => {
                    warn!(conn = index, ?frame, "ws closed by peer");
                    return SessionOutcome::Closed;
                }
                Some(Ok(Message::Frame(_))) => {
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
                // Before sending the next PING, check whether the previous
                // PING ever got its PONG. If we sent at T and `last_pong`
                // is still earlier than T after PONG_TIMEOUT, the connection
                // is dead.
                if let Some(sent_at) = last_ping_sent {
                    if last_pong < sent_at && sent_at.elapsed() >= PONG_TIMEOUT {
                        warn!(
                            conn = index,
                            since_pong_secs = last_pong.elapsed().as_secs_f64(),
                            "PONG timeout, closing dead connection",
                        );
                        let _ = write.send(Message::Close(None)).await;
                        return SessionOutcome::PongTimeout;
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
                            new_in_session.push(a.clone());
                        }
                    }
                    context
                        .stats
                        .desired_count
                        .store(sub.desired.len(), Ordering::Relaxed);
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
                            removed.push(a.clone());
                        }
                    }
                    context
                        .stats
                        .desired_count
                        .store(sub.desired.len(), Ordering::Relaxed);
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

/// Parse a text frame, explode it, filter by `desired`, and append the
/// surviving events to `events_buf`. Updates per-period stats.
fn handle_text(
    text: &str,
    sub: &SubState,
    events_buf: &mut Vec<EventRecord>,
    context: &SessionContext<'_>,
    frame_sequence: u64,
    timestamp_received_ns: i64,
) -> Result<()> {
    let frame: serde_json::Value = serde_json::from_str(text).context("parse wire frame")?;
    let messages = match frame {
        serde_json::Value::Array(messages) => messages,
        message => vec![message],
    };
    let message_count = u32::try_from(messages.len()).context("too many messages in frame")?;

    for (message_index, raw_value) in messages.into_iter().enumerate() {
        // Serialize from Value rather than from the typed struct so fields a
        // newer server adds remain available in raw_message.
        let raw_message = serde_json::to_string(&raw_value).context("serialize raw message")?;
        // Allocate the collector merge order before typed parsing or event
        // expansion. Parsing failures therefore leave an observable sequence
        // gap in the process logs rather than allowing a later message to take
        // this parent's position.
        let message = context.collector.next_message(
            context.connection_id,
            context.connection_epoch,
            frame_sequence,
            message_index as u32,
            message_count,
            timestamp_received_ns,
        );
        let msg: WireMessage = match serde_json::from_value(raw_value) {
            Ok(message) => message,
            Err(error) => {
                error!(
                    conn = context.index,
                    message_index,
                    %error,
                    raw_message,
                    "wire parent rejected; receive_sequence gap retained",
                );
                continue;
            }
        };
        // Per-type counters need to be incremented from the wire message
        // before fan-out (price_change still counts as one wire arrival per
        // entry, matching `connection.py::_handle_price_change`).
        match &msg {
            WireMessage::Book(_) => {
                context.stats.books.fetch_add(1, Ordering::Relaxed);
            }
            WireMessage::PriceChange(pc) => {
                context.stats
                    .price_changes
                    .fetch_add(pc.price_changes.len() as u64, Ordering::Relaxed);
            }
            WireMessage::LastTradePrice(_) => {
                context.stats.trades.fetch_add(1, Ordering::Relaxed);
            }
            WireMessage::TickSizeChange(_) => {
                context.stats.tick_changes.fetch_add(1, Ordering::Relaxed);
            }
        }
        let mut staged: Vec<Event> = Vec::new();
        explode(msg, &mut staged);
        let row_count = u32::try_from(staged.len()).context("too many rows in message")?;

        // Preserve the original exploded index even when an unexpected asset
        // is filtered. A non-contiguous row_index then exposes the omission.
        for (row_index, ev) in staged.into_iter().enumerate() {
            if sub.desired.contains(ev.asset_id()) {
                events_buf.push(message.record(
                    ev,
                    row_index as u32,
                    row_count,
                    raw_message.clone(),
                ));
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
    let payload = if !sub.initial_sent {
        sub.initial_sent = true;
        serde_json::json!({"assets_ids": assets, "type": "market"})
    } else {
        serde_json::json!({"assets_ids": assets, "operation": "subscribe"})
    };
    let text = payload.to_string();
    write
        .send(Message::Text(text))
        .await
        .map_err(|e| anyhow::anyhow!("ws send: {e}"))?;
    info!(conn = index, count = assets.len(), "subscribed assets");
    Ok(())
}

async fn send_unsubscribe<S>(write: &mut S, assets: &[String], index: usize) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::fmt::Display,
{
    let payload = serde_json::json!({"assets_ids": assets, "operation": "unsubscribe"});
    let text = payload.to_string();
    write
        .send(Message::Text(text))
        .await
        .map_err(|e| anyhow::anyhow!("ws send: {e}"))?;
    info!(conn = index, count = assets.len(), "unsubscribed assets");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(
        text: &str,
        sub: &SubState,
        stats: &ConnStats,
        events: &mut Vec<EventRecord>,
    ) -> Result<()> {
        let (event_tx, _event_rx) = mpsc::channel(1);
        let collector = CollectorContext::new();
        let context = SessionContext {
            index: 0,
            connection_id: 0,
            connection_epoch: 1,
            event_tx: &event_tx,
            collector: &collector,
            stats,
        };
        handle_text(
            text,
            sub,
            events,
            &context,
            0,
            123,
        )
    }

    fn sub_state(desired: &[&str]) -> SubState {
        let mut s = SubState::default();
        for a in desired {
            s.desired.insert((*a).into());
        }
        s
    }

    #[test]
    fn handle_text_filters_undesired_book_event() {
        let stats = ConnStats::default();
        let mut buf = Vec::new();
        let sub = sub_state(&["wanted"]);
        let raw = r#"{
            "event_type": "book", "asset_id": "not-wanted", "market": "m",
            "bids": [], "asks": [], "timestamp": "1", "hash": "h"
        }"#;
        handle(raw, &sub, &stats, &mut buf).unwrap();
        assert_eq!(buf.len(), 0, "undesired asset should be filtered");
        // The book counter still increments because the wire message arrived.
        assert_eq!(stats.books.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn handle_text_keeps_desired_book_event() {
        let stats = ConnStats::default();
        let mut buf = Vec::new();
        let sub = sub_state(&["wanted"]);
        let raw = r#"{
            "event_type": "book", "asset_id": "wanted", "market": "m",
            "bids": [{"price": "0.4", "size": "10"}],
            "asks": [{"price": "0.5", "size": "10"}],
            "timestamp": "1", "hash": "h"
        }"#;
        handle(raw, &sub, &stats, &mut buf).unwrap();
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn handle_text_explodes_price_change_and_filters_per_entry() {
        let stats = ConnStats::default();
        let mut buf = Vec::new();
        let sub = sub_state(&["a1"]);
        let raw = r#"{
            "event_type": "price_change", "market": "m", "timestamp": "1",
            "price_changes": [
                {"asset_id": "a1", "price": "0.4", "size": "10", "side": "BUY", "hash": "h1"},
                {"asset_id": "a2", "price": "0.5", "size": "10", "side": "SELL", "hash": "h2"},
                {"asset_id": "a1", "price": "0.6", "size": "10", "side": "BUY", "hash": "h3"}
            ]
        }"#;
        handle(raw, &sub, &stats, &mut buf).unwrap();
        assert_eq!(buf.len(), 2, "only a1 entries should survive filter");
        assert_eq!(stats.price_changes.load(Ordering::Relaxed), 3, "wire count is 3");
    }

    #[test]
    fn handle_text_handles_array_frame() {
        let stats = ConnStats::default();
        let mut buf = Vec::new();
        let sub = sub_state(&["a"]);
        let raw = r#"[
            {"event_type": "tick_size_change", "asset_id": "a", "market": "m",
             "old_tick_size": "0.01", "new_tick_size": "0.001", "timestamp": "1"},
            {"event_type": "tick_size_change", "asset_id": "a", "market": "m",
             "old_tick_size": "0.02", "new_tick_size": "0.002", "timestamp": "2"}
        ]"#;
        handle(raw, &sub, &stats, &mut buf).unwrap();
        assert_eq!(buf.len(), 2);
        assert_eq!(stats.tick_changes.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn handle_text_keeps_valid_parents_around_an_unsupported_parent() {
        let stats = ConnStats::default();
        let mut buf = Vec::new();
        let sub = sub_state(&["a"]);
        let raw = r#"[
            {"event_type": "tick_size_change", "asset_id": "a", "market": "m",
             "old_tick_size": "0.01", "new_tick_size": "0.001", "timestamp": "1"},
            {"event_type": "future_event", "asset_id": "a", "market": "m",
             "timestamp": "2"},
            {"event_type": "tick_size_change", "asset_id": "a", "market": "m",
             "old_tick_size": "0.02", "new_tick_size": "0.002", "timestamp": "3"}
        ]"#;

        handle(raw, &sub, &stats, &mut buf).unwrap();

        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].message_index, 0);
        assert_eq!(buf[1].message_index, 2);
        assert_eq!(buf[0].message_count, 3);
        assert_eq!(buf[1].receive_sequence, buf[0].receive_sequence + 2);
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
        tx.send(Command::Unsubscribe(vec!["a1".into()])).await.unwrap();
        drop(tx);
        let should_exit = rx.is_closed() && rx.is_empty();
        assert!(
            !should_exit,
            "must not exit while buffered work remains, even with senders dropped",
        );
    }

    #[test]
    fn drain_period_resets_counters() {
        let stats = ConnStats::default();
        stats.books.store(5, Ordering::Relaxed);
        stats.price_changes.store(7, Ordering::Relaxed);
        stats.reconnects.store(3, Ordering::Relaxed);
        let p = stats.drain_period();
        assert_eq!(p.books, 5);
        assert_eq!(p.price_changes, 7);
        assert_eq!(stats.books.load(Ordering::Relaxed), 0);
        // Lifetime counter is unaffected.
        assert_eq!(stats.reconnects.load(Ordering::Relaxed), 3);
    }
}
