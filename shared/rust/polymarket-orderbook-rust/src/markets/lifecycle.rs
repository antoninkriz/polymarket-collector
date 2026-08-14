//! Commands sent to the publisher's authoritative market lifecycle controller.

use tokio::sync::oneshot;

use crate::events::{Market, MarketLifecycleObservation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleSource {
    WebSocket,
    RedisStream,
}

pub type LifecycleCompletion = oneshot::Sender<Result<(), String>>;

#[derive(Debug, Clone)]
pub struct ActiveMarketSnapshot {
    pub active_count: usize,
    pub markets: Vec<Market>,
}

pub type LifecycleSnapshotCompletion = oneshot::Sender<ActiveMarketSnapshot>;

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
}
