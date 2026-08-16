//! Market discovery and lifecycle.
//!
//! The publisher loads an optional Redis restart cache, then reconciles active,
//! newly created, and resolved markets directly from Gamma while WebSocket
//! lifecycle events remain the primary low-latency source.

pub mod gamma;
pub mod lifecycle;
pub mod redis_cache;

use crate::events::Market;

pub fn binary_market_from_outcomes(
    market_hash: String,
    outcomes: &[String],
    asset_ids: &[String],
) -> Option<Market> {
    if outcomes.len() != 2 || asset_ids.len() != 2 {
        return None;
    }
    let (yes_idx, no_idx) = if outcomes[0] == "Yes" { (0, 1) } else { (1, 0) };
    Some(Market {
        hash: market_hash,
        assets: [asset_ids[yes_idx].clone(), asset_ids[no_idx].clone()],
    })
}
