//! Commands sent to the publisher's authoritative market lifecycle controller.

use tokio::sync::oneshot;

use crate::events::{Market, MarketLifecycleObservation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleSource {
    WebSocket,
    RedisStream,
    Gamma,
}

pub type LifecycleCompletion = oneshot::Sender<Result<(), String>>;

#[derive(Debug, Clone)]
pub struct ActiveMarketSnapshot {
    pub revision: u64,
    pub active_count: usize,
    pub markets: Vec<Market>,
}

pub struct ScannedActiveMarket {
    pub market: Market,
    pub observation: Option<MarketLifecycleObservation>,
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
    ScanStart {
        scan_id: u64,
        cold_start: bool,
        completion: LifecycleCompletion,
    },
    ScanPage {
        scan_id: u64,
        markets: Vec<ScannedActiveMarket>,
        completion: LifecycleCompletion,
    },
    ScanFinish {
        scan_id: u64,
        completion: LifecycleCompletion,
    },
    ScanAbort {
        scan_id: u64,
        completion: LifecycleCompletion,
    },
}
