//! Market discovery and lifecycle.
//!
//! Two phases:
//!
//! - **Pre-load** at startup: read the active-markets cache from Redis (or
//!   fall back to the Gamma API), produce an initial list of [`Market`]s to
//!   subscribe to. Skipped under `--new-only`.
//! - **Runtime**: consume the `polymarket:market_events` Redis stream as a
//!   consumer-group member. Dispatch `new_market` events to the pool's
//!   `subscribe_markets`, and `market_resolved` events to `unsubscribe_markets`.

pub mod gamma;
pub mod lifecycle;
pub mod redis_cache;
pub mod stream;

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
