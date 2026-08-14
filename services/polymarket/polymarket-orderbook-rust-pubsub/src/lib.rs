//! Library façade for the Redis Stream Polymarket orderbook service.

pub mod config;
pub mod lease;
pub mod market_lifecycle;
pub mod pubsub_sink;
pub mod sequence_watermark;
