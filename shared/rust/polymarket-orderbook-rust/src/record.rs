//! Collector metadata attached at the WebSocket receive boundary.
//!
//! Polymarket does not publish a sequence number or a unique public fill ID.
//! The collector therefore records the order it actually observes.  A
//! [`CollectorContext`] is shared by all WebSocket tasks in one process and
//! assigns a process-wide `receive_sequence` to each parent market message.
//! Children produced by `price_changes[]` share `message_id` and are ordered
//! by `row_index`.
//!
//! `timestamp_received_ns` is sampled as soon as tungstenite yields a text
//! frame.  It is a wall-clock observation, not an ordering key; system clocks
//! can step, while `receive_sequence` is monotonic for the collector session.

use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::events::Event;

pub const SCHEMA_VERSION: u8 = 3;

/// Process-scoped identity and sequencer shared by all WebSocket connections.
#[derive(Debug)]
pub struct CollectorContext {
    session_id: Uuid,
    session_started_at_ns: i64,
    publisher_fence: u64,
    next_receive_sequence: AtomicU64,
}

impl CollectorContext {
    pub fn new() -> Self {
        Self::with_publisher_fence(0)
    }

    /// Create a collector bound to an externally acquired publisher fence.
    pub fn with_publisher_fence(publisher_fence: u64) -> Self {
        Self {
            session_id: Uuid::new_v4(),
            session_started_at_ns: now_ns(),
            publisher_fence,
            next_receive_sequence: AtomicU64::new(0),
        }
    }

    /// Allocate metadata for one parent message from a WebSocket frame.
    pub fn next_message(
        &self,
        connection_id: u32,
        connection_epoch: u64,
        frame_sequence: u64,
        message_index: u32,
        message_count: u32,
        timestamp_received_ns: i64,
    ) -> MessageContext {
        MessageContext {
            collector_session_id: self.session_id,
            collector_session_started_at_ns: self.session_started_at_ns,
            publisher_fence: self.publisher_fence,
            connection_id,
            connection_epoch,
            frame_sequence,
            receive_sequence: self.next_receive_sequence.fetch_add(1, Ordering::SeqCst),
            message_id: Uuid::new_v4(),
            message_index,
            message_count,
            timestamp_received_ns,
        }
    }
}

impl Default for CollectorContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Metadata shared by all exploded rows from one parent market message.
#[derive(Debug, Clone)]
pub struct MessageContext {
    collector_session_id: Uuid,
    collector_session_started_at_ns: i64,
    publisher_fence: u64,
    connection_id: u32,
    connection_epoch: u64,
    frame_sequence: u64,
    receive_sequence: u64,
    message_id: Uuid,
    message_index: u32,
    message_count: u32,
    timestamp_received_ns: i64,
}

impl MessageContext {
    pub fn record(
        &self,
        event: Event,
        row_index: u32,
        row_count: u32,
        raw_message: String,
    ) -> EventRecord {
        EventRecord {
            schema_version: SCHEMA_VERSION,
            collector_session_id: self.collector_session_id,
            collector_session_started_at_ns: self.collector_session_started_at_ns,
            publisher_fence: self.publisher_fence,
            connection_id: self.connection_id,
            connection_epoch: self.connection_epoch,
            frame_sequence: self.frame_sequence,
            receive_sequence: self.receive_sequence,
            message_id: self.message_id,
            message_index: self.message_index,
            message_count: self.message_count,
            row_index,
            row_count,
            timestamp_received_ns: self.timestamp_received_ns,
            raw_message,
            event,
        }
    }
}

/// A normalized event plus the ordering and provenance needed for replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRecord {
    pub schema_version: u8,
    pub collector_session_id: Uuid,
    pub collector_session_started_at_ns: i64,
    /// Monotonic Redis-issued generation of the authoritative publisher.
    pub publisher_fence: u64,
    pub connection_id: u32,
    pub connection_epoch: u64,
    pub frame_sequence: u64,
    pub receive_sequence: u64,
    pub message_id: Uuid,
    pub message_index: u32,
    pub message_count: u32,
    pub row_index: u32,
    pub row_count: u32,
    /// UTC Unix epoch nanoseconds sampled at socket receipt.
    pub timestamp_received_ns: i64,
    /// The complete parent JSON object, retained for audit and forward parsing.
    pub raw_message: String,
    #[serde(flatten)]
    pub event: Event,
}

impl EventRecord {
    /// Stable identity for transport retries.  It intentionally does not use
    /// any market payload fields: identical payloads can be distinct events.
    pub fn identity(&self) -> (Uuid, u32) {
        (self.message_id, self.row_index)
    }

    /// Build a record for legacy callers and tests that do not originate at
    /// the WebSocket boundary. Production v3 ingestion uses
    /// [`CollectorContext`] instead.
    pub fn synthetic(event: Event) -> Self {
        let raw_message = serde_json::to_string(&event).unwrap_or_else(|_| "{}".into());
        Self {
            schema_version: SCHEMA_VERSION,
            collector_session_id: Uuid::nil(),
            collector_session_started_at_ns: 0,
            publisher_fence: 0,
            connection_id: 0,
            connection_epoch: 0,
            frame_sequence: 0,
            receive_sequence: 0,
            message_id: Uuid::nil(),
            message_index: 0,
            message_count: 1,
            row_index: 0,
            row_count: 1,
            timestamp_received_ns: 0,
            raw_message,
            event,
        }
    }
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
    use crate::events::Event;
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
    fn parent_rows_share_identity_and_keep_child_order() {
        let collector = CollectorContext::new();
        let message = collector.next_message(7, 2, 11, 0, 1, 123);
        let first = message.record(tick("1"), 0, 2, "{}".into());
        let second = message.record(tick("1"), 1, 2, "{}".into());

        assert_eq!(first.message_id, second.message_id);
        assert_eq!(first.receive_sequence, second.receive_sequence);
        assert_eq!(first.row_index, 0);
        assert_eq!(second.row_index, 1);
        assert_ne!(first.identity(), second.identity());
    }

    #[test]
    fn identical_payloads_receive_distinct_message_ids() {
        let collector = CollectorContext::new();
        let first = collector
            .next_message(0, 1, 0, 0, 1, 100)
            .record(tick("1"), 0, 1, "{}".into());
        let second =
            collector
                .next_message(0, 1, 1, 0, 1, 101)
                .record(tick("1"), 0, 1, "{}".into());

        assert_ne!(first.message_id, second.message_id);
        assert_ne!(first.identity(), second.identity());
        assert!(first.receive_sequence < second.receive_sequence);
    }
}
