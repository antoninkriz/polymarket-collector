//! Library façade for the Polymarket orderbook streaming service.
//!
//! Declares the module tree so it can be consumed by both `main.rs` and
//! the integration tests in `tests/`.

pub mod events;
pub mod markets;
pub mod record;
pub mod ws;
