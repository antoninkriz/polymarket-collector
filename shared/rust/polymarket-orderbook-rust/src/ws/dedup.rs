//! Idempotency identity for collector-owned transport retries.
//!
//! There is deliberately no payload-based WebSocket deduplicator in v3.
//! Polymarket's public market channel exposes neither an exchange sequence nor
//! a unique fill ID.  A transaction hash can contain multiple fills, and two
//! legitimate deliveries can otherwise have identical public fields.  Any
//! content key would therefore merge observations that cannot safely be
//! proven equal.
//!
//! The only duplicates v3 removes are retries created by this collector after
//! receipt.  Those retries retain the same `(message_id, row_index)` pair.

use uuid::Uuid;

use crate::record::EventRecord;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventIdentity {
    pub message_id: Uuid,
    pub row_index: u32,
}

pub fn event_identity(record: &EventRecord) -> EventIdentity {
    let (message_id, row_index) = record.identity();
    EventIdentity { message_id, row_index }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::Event;
    use crate::record::CollectorContext;
    use rust_decimal::Decimal;

    fn trade() -> Event {
        Event::LastTradePrice {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            price: Decimal::new(5, 1),
            size: Decimal::new(10, 0),
            side: "BUY".into(),
            fee_rate_bps: "10".into(),
            transaction_hash: "0xsame".into(),
        }
    }

    #[test]
    fn transport_retry_has_the_same_identity() {
        let collector = CollectorContext::new();
        let record = collector
            .next_message(0, 1, 0, 0, 1, 1)
            .record(trade(), 0, 1, "{}".into());
        let retry = record.clone();
        assert_eq!(event_identity(&record), event_identity(&retry));
    }

    #[test]
    fn identical_public_fills_are_not_deduplicated() {
        let collector = CollectorContext::new();
        let first = collector
            .next_message(0, 1, 0, 0, 1, 1)
            .record(trade(), 0, 1, "{}".into());
        let second = collector
            .next_message(0, 1, 1, 0, 1, 2)
            .record(trade(), 0, 1, "{}".into());
        assert_ne!(event_identity(&first), event_identity(&second));
    }
}
