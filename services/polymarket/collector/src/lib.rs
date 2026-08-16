//! Library façade for the Redis Stream Polymarket orderbook service.

pub mod config;
pub mod events;
pub mod gamma_reconcile;
pub mod lease;
pub mod market_lifecycle;
pub mod markets;
pub mod record;
pub mod redis_stream;
pub mod sequence_watermark;
pub mod ws;
