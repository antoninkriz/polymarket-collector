//! Commands sent to the publisher's authoritative market lifecycle controller.

use tokio::sync::oneshot;

use crate::events::{Market, MarketLifecycleObservation};
use crate::markets::gamma::GammaMarket;
use crate::ws::pool::PoolStats;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleSource {
    WebSocket,
    Gamma,
}

pub type LifecycleCompletion = oneshot::Sender<Result<(), String>>;

#[derive(Debug, Clone)]
pub struct ActiveMarketSnapshot {
    pub revision: u64,
    pub markets: Vec<Market>,
}

pub type LifecycleSnapshotCompletion = oneshot::Sender<ActiveMarketSnapshot>;
pub type PoolStatsCompletion = oneshot::Sender<PoolStats>;

/// Lower-priority lifecycle work. WebSocket observations use their own
/// bounded, high-priority channel so reconciliation cannot delay an immediate
/// `new_market` subscription.
pub enum LifecycleRequest {
    Bootstrap {
        markets: Vec<Market>,
        completion: LifecycleCompletion,
    },
    Observation {
        source: LifecycleSource,
        observation: MarketLifecycleObservation,
        completion: LifecycleCompletion,
    },
    Snapshot {
        completion: LifecycleSnapshotCompletion,
    },
    PoolStats {
        completion: PoolStatsCompletion,
    },
    GammaPage {
        cold_start: bool,
        markets: Vec<GammaMarket>,
        completion: LifecycleCompletion,
    },
}
