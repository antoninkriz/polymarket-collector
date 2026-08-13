//! Minimal metadata attached at the WebSocket receive boundary.
//!
//! Polymarket does not publish a sequence number or a unique public fill ID.
//! The collector therefore stores the order it actually observes. One compact
//! [`EventRecord::sequence`] is both the replay order and the idempotency key
//! for collector-owned Redis/ClickHouse retries.

use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::events::Event;

const SEQUENCE_LOCAL_BITS: u32 = 48;
const SEQUENCE_LOCAL_MASK: u64 = (1_u64 << SEQUENCE_LOCAL_BITS) - 1;
const SEQUENCE_GENERATION_MAX: u64 = u64::MAX >> SEQUENCE_LOCAL_BITS;

/// Process-scoped sequencer. The Redis-issued publisher generation occupies
/// the high bits, so a restarted authoritative collector continues after the
/// previous process without storing a separate session or generation field.
#[derive(Debug)]
pub struct CollectorContext {
    sequence_prefix: u64,
    next_local_sequence: AtomicU64,
}

impl CollectorContext {
    pub fn new() -> Self {
        Self::with_publisher_generation(0)
    }

    pub fn with_publisher_generation(generation: u64) -> Self {
        assert!(
            generation <= SEQUENCE_GENERATION_MAX,
            "publisher generation {generation} exceeds compact sequence capacity",
        );
        Self {
            sequence_prefix: generation << SEQUENCE_LOCAL_BITS,
            next_local_sequence: AtomicU64::new(0),
        }
    }

    /// Attach the actual socket receive time and allocate the next observed
    /// event order. Exploded children call this in their wire array order.
    pub fn record(&self, event: Event, timestamp_received_ns: i64) -> EventRecord {
        let local_sequence = self.next_local_sequence.fetch_add(1, Ordering::SeqCst);
        assert!(
            local_sequence <= SEQUENCE_LOCAL_MASK,
            "collector emitted more than 2^48 events in one publisher generation",
        );
        EventRecord {
            timestamp_received_ns,
            sequence: self.sequence_prefix | local_sequence,
            event,
        }
    }
}

impl Default for CollectorContext {
    fn default() -> Self {
        Self::new()
    }
}

/// One normalized event and the two values needed for correct replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    /// UTC Unix epoch nanoseconds sampled when tungstenite yields its frame.
    pub timestamp_received_ns: i64,
    /// Monotonic observed order and retry identity.
    pub sequence: u64,
    #[serde(flatten)]
    pub event: Event,
}

/// Sample UTC wall time without truncating to ClickHouse insertion precision.
pub fn now_ns() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .expect("current UTC timestamp must fit in i64 nanoseconds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn tick(timestamp: &str) -> Event {
        Event::TickSizeChange {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: timestamp.into(),
            old_tick_size: Decimal::new(1, 2),
            new_tick_size: Decimal::new(1, 3),
        }
    }

    #[test]
    fn exploded_rows_keep_allocation_order() {
        let collector = CollectorContext::new();
        let first = collector.record(tick("1"), 123);
        let second = collector.record(tick("1"), 123);

        assert_eq!(second.sequence, first.sequence + 1);
        assert_eq!(first.timestamp_received_ns, second.timestamp_received_ns);
    }

    #[test]
    fn identical_public_events_are_distinct_observations() {
        let collector = CollectorContext::new();
        let first = collector.record(tick("1"), 100);
        let second = collector.record(tick("1"), 101);

        assert_ne!(first.sequence, second.sequence);
    }

    #[test]
    fn publisher_generation_orders_process_restarts() {
        let old = CollectorContext::with_publisher_generation(7).record(tick("1"), 100);
        let new = CollectorContext::with_publisher_generation(8).record(tick("1"), 90);

        assert!(old.sequence < new.sequence);
        assert_eq!(old.sequence, 7_u64 << SEQUENCE_LOCAL_BITS);
        assert_eq!(new.sequence, 8_u64 << SEQUENCE_LOCAL_BITS);
    }
}
