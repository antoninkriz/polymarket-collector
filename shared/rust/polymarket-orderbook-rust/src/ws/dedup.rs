//! Retry identity for v3 records.
//!
//! Public payloads are never deduplicated: Polymarket exposes neither a fill
//! ID nor an exchange sequence, so identical values can be distinct events.
//! Only collector-owned transport retries share the same compact sequence.

use crate::record::EventRecord;

pub fn event_identity(record: &EventRecord) -> u64 {
    record.sequence
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
        let record = CollectorContext::new().record(trade(), 1);
        let retry = record.clone();
        assert_eq!(event_identity(&record), event_identity(&retry));
    }

    #[test]
    fn identical_public_fills_are_not_deduplicated() {
        let collector = CollectorContext::new();
        let first = collector.record(trade(), 1);
        let second = collector.record(trade(), 2);
        assert_ne!(event_identity(&first), event_identity(&second));
    }
}
