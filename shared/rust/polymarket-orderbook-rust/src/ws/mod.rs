//! WebSocket layer: connection pool, subscription management, and the
//! per-connection task that owns one socket to Polymarket's CLOB.
//!
//! Module split:
//! - [`connection`] — single WebSocket session: connect, ping/pong heartbeat,
//!   parse incoming frames into [`crate::events::Event`]s, automatic
//!   reconnect with subscription recovery.
//! - [`dedup`] — global dedup cache + the [`dedup::DedupForwarder`] that all
//!   Connections push events into.
//! - [`pool`] — connection pool: randomized two-pass asset allocation across
//!   connections, batched startup, market lifecycle (subscribe / unsubscribe),
//!   and asset-level health monitoring.

pub mod connection;
pub mod dedup;
pub mod pool;

pub const WS_MARKET_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
