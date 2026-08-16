//! WebSocket layer: connection pool, subscription management, and the
//! per-connection task that owns one socket to Polymarket's CLOB.
//!
//! Module split:
//! - [`connection`] — single WebSocket session: connect, ping/pong heartbeat,
//!   parse incoming frames into [`crate::record::EventRecord`]s, automatic
//!   reconnect with subscription recovery.
//! - [`pool`] — connection pool: keeps both assets of each market on one
//!   authoritative connection so parent-message order is not merged across
//!   independent sockets.

pub mod connection;
pub mod pool;

pub const WS_MARKET_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
