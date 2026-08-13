//! Wire-format event types for the Polymarket WSS market channel.
//!
//! Two layers:
//!
//! 1. [`WireMessage`] / [`WireFrame`] — what `serde` parses directly from
//!    the wire JSON. Mirrors Polymarket's payload shapes 1:1, including the
//!    `price_change` wrapper that contains an array of per-asset entries.
//!    A frame can be either a single message or a JSON array of messages.
//! 2. [`Event`] — the post-explode form that flows through the channel into
//!    the ClickHouse sink. Each [`Event`] becomes exactly one row in the
//!    `polymarket_orderbook` table. For `price_change`, one [`WireMessage`]
//!    fans out into N [`Event::PriceChange`] values (one per entry), matching
//!    what `sink.py::_serialize_event` does in the Python service.
//!
//! ## Wire vs storage shape (must match Python byte-for-byte)
//!
//! - Wire `bids`/`asks` come in as `[{"price": "0.41", "size": "100"}, ...]`
//!   but the storage shape is `[["0.41", "100"], ...]`. [`Level`]'s custom
//!   `Serialize` impl emits the array form; [`WireLevel`] parses the object form.
//! - `Event` includes the source `timestamp` so the durable v3 Redis record
//!   is self-contained. ClickHouse v3 also promotes its parsed value and
//!   retains the original string as `timestamp_raw`.
//! - Field order on each variant matches Python's dict insertion order in
//!   `sink.py::_serialize_event`.
//!
//! When `EXCLUDE_HASH=true`, callers null out the `hash` field on
//! `Event::Book` and `Event::PriceChange` before serializing.
//! `LastTradePrice.transaction_hash` is a different concept and is never stripped.

use std::fmt;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// =====================================================================
// Wire layer (deserialize-only)
// =====================================================================

/// A single WS frame from Polymarket. Frames are usually a single JSON
/// object but the server occasionally sends a JSON array of objects, so
/// we accept either.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum WireFrame {
    Many(Vec<WireMessage>),
    One(WireMessage),
}

impl WireFrame {
    /// Iterate the messages in this frame.
    pub fn into_iter(self) -> Box<dyn Iterator<Item = WireMessage>> {
        match self {
            WireFrame::One(m) => Box::new(std::iter::once(m)),
            WireFrame::Many(v) => Box::new(v.into_iter()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum WireMessage {
    Book(WireBook),
    PriceChange(WirePriceChange),
    LastTradePrice(WireLastTradePrice),
    TickSizeChange(WireTickSizeChange),
}

#[derive(Debug, Deserialize)]
pub struct WireLevel {
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
}

#[derive(Debug, Deserialize)]
pub struct WireBook {
    pub market: String,
    pub asset_id: String,
    pub timestamp: String,
    pub bids: Vec<WireLevel>,
    pub asks: Vec<WireLevel>,
    /// Polymarket content hash. Always present in real frames; required so the
    /// dedup path in the wrapper layer can rely on it without a fallback.
    pub hash: String,
}

#[derive(Debug, Deserialize)]
pub struct WirePriceChange {
    pub market: String,
    pub timestamp: String,
    pub price_changes: Vec<WirePriceChangeEntry>,
}

#[derive(Debug, Deserialize)]
pub struct WirePriceChangeEntry {
    pub asset_id: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
    pub side: String,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub best_bid: Option<Decimal>,
    #[serde(default, with = "rust_decimal::serde::str_option")]
    pub best_ask: Option<Decimal>,
    /// Polymarket content hash. Always present in real frames; required so the
    /// dedup path in the wrapper layer can rely on it without a fallback.
    pub hash: String,
}

#[derive(Debug, Deserialize)]
pub struct WireLastTradePrice {
    pub market: String,
    pub asset_id: String,
    pub timestamp: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub size: Decimal,
    pub side: String,
    pub fee_rate_bps: String,
    pub transaction_hash: String,
}

#[derive(Debug, Deserialize)]
pub struct WireTickSizeChange {
    pub market: String,
    pub asset_id: String,
    pub timestamp: String,
    #[serde(with = "rust_decimal::serde::str")]
    pub old_tick_size: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub new_tick_size: Decimal,
}

// =====================================================================
// Event layer (post-explode, serialize for sink)
// =====================================================================

/// Storage-format orderbook level. Custom `Serialize` emits a 2-element JSON
/// array `[price_str, size_str]` to match `sink.py::_serialize_event`.
/// Used only on the sink path; the WS parser uses [`WireLevel`] which
/// deserializes from the object form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Level {
    pub price: Decimal,
    pub size: Decimal,
}

impl Level {
    #[cfg(test)]
    pub fn new(price: Decimal, size: Decimal) -> Self {
        Self { price, size }
    }
}

impl From<WireLevel> for Level {
    fn from(w: WireLevel) -> Self {
        Self {
            price: w.price,
            size: w.size,
        }
    }
}

impl serde::Serialize for Level {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut t = s.serialize_tuple(2)?;
        t.serialize_element(&self.price.to_string())?;
        t.serialize_element(&self.size.to_string())?;
        t.end()
    }
}

impl<'de> serde::Deserialize<'de> for Level {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let [price_s, size_s]: [String; 2] = serde::Deserialize::deserialize(d)?;
        let price = price_s.parse::<Decimal>().map_err(D::Error::custom)?;
        let size = size_s.parse::<Decimal>().map_err(D::Error::custom)?;
        Ok(Level { price, size })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum Event {
    Book {
        market: String,
        asset_id: String,
        timestamp: String,
        bids: Vec<Level>,
        asks: Vec<Level>,
        #[serde(skip_serializing_if = "Option::is_none")]
        hash: Option<String>,
    },
    PriceChange {
        market: String,
        asset_id: String,
        timestamp: String,
        #[serde(
            skip_serializing_if = "Option::is_none",
            with = "rust_decimal::serde::str_option"
        )]
        best_bid: Option<Decimal>,
        #[serde(
            skip_serializing_if = "Option::is_none",
            with = "rust_decimal::serde::str_option"
        )]
        best_ask: Option<Decimal>,
        #[serde(skip_serializing_if = "Option::is_none")]
        hash: Option<String>,
        #[serde(with = "rust_decimal::serde::str")]
        price: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        size: Decimal,
        side: String,
    },
    LastTradePrice {
        market: String,
        asset_id: String,
        timestamp: String,
        #[serde(with = "rust_decimal::serde::str")]
        price: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        size: Decimal,
        side: String,
        fee_rate_bps: String,
        transaction_hash: String,
    },
    TickSizeChange {
        market: String,
        asset_id: String,
        timestamp: String,
        #[serde(with = "rust_decimal::serde::str")]
        old_tick_size: Decimal,
        #[serde(with = "rust_decimal::serde::str")]
        new_tick_size: Decimal,
    },
}

impl Event {
    /// Wire `event_type` value (also the value of the `event_type` ClickHouse column).
    pub fn kind(&self) -> &'static str {
        match self {
            Event::Book { .. } => "book",
            Event::PriceChange { .. } => "price_change",
            Event::LastTradePrice { .. } => "last_trade_price",
            Event::TickSizeChange { .. } => "tick_size_change",
        }
    }

    pub fn asset_id(&self) -> &str {
        match self {
            Event::Book { asset_id, .. }
            | Event::PriceChange { asset_id, .. }
            | Event::LastTradePrice { asset_id, .. }
            | Event::TickSizeChange { asset_id, .. } => asset_id,
        }
    }

    pub fn market(&self) -> &str {
        match self {
            Event::Book { market, .. }
            | Event::PriceChange { market, .. }
            | Event::LastTradePrice { market, .. }
            | Event::TickSizeChange { market, .. } => market,
        }
    }

    pub fn timestamp(&self) -> &str {
        match self {
            Event::Book { timestamp, .. }
            | Event::PriceChange { timestamp, .. }
            | Event::LastTradePrice { timestamp, .. }
            | Event::TickSizeChange { timestamp, .. } => timestamp,
        }
    }

    /// Strip orderbook hash fields. No-op for trades and tick changes.
    /// `LastTradePrice.transaction_hash` is a different concept and is never stripped.
    pub fn strip_hash(&mut self) {
        match self {
            Event::Book { hash, .. } => *hash = None,
            Event::PriceChange { hash, .. } => *hash = None,
            _ => {}
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Event::Book {
                market,
                asset_id,
                bids,
                asks,
                ..
            } => {
                write!(
                    f,
                    "book market={} asset={} bids={} asks={}",
                    market,
                    asset_id,
                    bids.len(),
                    asks.len()
                )
            }
            Event::PriceChange {
                market,
                asset_id,
                price,
                size,
                side,
                best_bid,
                best_ask,
                ..
            } => write!(
                f,
                "price_change market={} asset={} side={} price={} size={} bid={:?} ask={:?}",
                market, asset_id, side, price, size, best_bid, best_ask
            ),
            Event::LastTradePrice {
                market,
                asset_id,
                price,
                size,
                side,
                transaction_hash,
                ..
            } => write!(
                f,
                "last_trade_price market={} asset={} side={} price={} size={} tx={}",
                market, asset_id, side, price, size, transaction_hash
            ),
            Event::TickSizeChange {
                market,
                asset_id,
                old_tick_size,
                new_tick_size,
                ..
            } => write!(
                f,
                "tick_size_change market={} asset={} old={} new={}",
                market, asset_id, old_tick_size, new_tick_size
            ),
        }
    }
}

/// Convert a parsed wire message into one or more [`Event`]s, appending to `out`.
///
/// `book` / `last_trade_price` / `tick_size_change` are 1:1.
/// `price_change` fans out into one [`Event::PriceChange`] per entry, with
/// the parent `market` and `timestamp` copied onto each.
pub fn explode(msg: WireMessage, out: &mut Vec<Event>) {
    match msg {
        WireMessage::Book(b) => out.push(Event::Book {
            market: b.market,
            asset_id: b.asset_id,
            timestamp: b.timestamp,
            bids: b.bids.into_iter().map(Level::from).collect(),
            asks: b.asks.into_iter().map(Level::from).collect(),
            hash: Some(b.hash),
        }),
        WireMessage::PriceChange(pc) => {
            for e in pc.price_changes {
                out.push(Event::PriceChange {
                    market: pc.market.clone(),
                    asset_id: e.asset_id,
                    timestamp: pc.timestamp.clone(),
                    best_bid: e.best_bid,
                    best_ask: e.best_ask,
                    hash: Some(e.hash),
                    price: e.price,
                    size: e.size,
                    side: e.side,
                });
            }
        }
        WireMessage::LastTradePrice(t) => out.push(Event::LastTradePrice {
            market: t.market,
            asset_id: t.asset_id,
            timestamp: t.timestamp,
            price: t.price,
            size: t.size,
            side: t.side,
            fee_rate_bps: t.fee_rate_bps,
            transaction_hash: t.transaction_hash,
        }),
        WireMessage::TickSizeChange(c) => out.push(Event::TickSizeChange {
            market: c.market,
            asset_id: c.asset_id,
            timestamp: c.timestamp,
            old_tick_size: c.old_tick_size,
            new_tick_size: c.new_tick_size,
        }),
    }
}

// =====================================================================
// Shared types
// =====================================================================

/// Minimal market identity. Polymarket markets are always binary
/// (one YES asset, one NO asset).
#[derive(Debug, Clone)]
pub struct Market {
    pub hash: String,
    pub assets: [String; 2], // [yes, no]
}

impl Market {
    pub fn new(hash: String, yes_asset: String, no_asset: String) -> Self {
        Self {
            hash,
            assets: [yes_asset, no_asset],
        }
    }

    #[cfg(test)]
    pub fn yes(&self) -> &str {
        &self.assets[0]
    }

    #[cfg(test)]
    pub fn no(&self) -> &str {
        &self.assets[1]
    }
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    // ---- Wire parsing ---------------------------------------------------

    #[test]
    fn parse_wire_book() {
        let raw = r#"{
            "event_type": "book",
            "asset_id": "asset-1",
            "market": "0xmarket",
            "bids": [{"price": "0.41", "size": "100"}, {"price": "0.40", "size": "200"}],
            "asks": [{"price": "0.42", "size": "150"}],
            "timestamp": "1757908892351",
            "hash": "abc123"
        }"#;
        let msg: WireMessage = serde_json::from_str(raw).unwrap();
        let WireMessage::Book(b) = msg else {
            panic!("expected Book")
        };
        assert_eq!(b.market, "0xmarket");
        assert_eq!(b.asset_id, "asset-1");
        assert_eq!(b.bids.len(), 2);
        assert_eq!(b.bids[0].price, dec("0.41"));
        assert_eq!(b.bids[0].size, dec("100"));
        assert_eq!(b.asks.len(), 1);
        assert_eq!(b.timestamp, "1757908892351");
        assert_eq!(b.hash, "abc123");
    }

    #[test]
    fn parse_wire_price_change_with_multiple_entries() {
        let raw = r#"{
            "event_type": "price_change",
            "market": "0xmarket",
            "timestamp": "1757908892351",
            "price_changes": [
                {"asset_id": "a1", "price": "0.41", "size": "100", "side": "BUY",
                 "best_bid": "0.40", "best_ask": "0.42", "hash": "h1"},
                {"asset_id": "a1", "price": "0.43", "size": "200", "side": "SELL",
                 "best_bid": "0.42", "best_ask": "0.44", "hash": "h2"},
                {"asset_id": "a2", "price": "0.55", "size": "50", "side": "BUY",
                 "best_bid": "0.54", "best_ask": "0.56", "hash": "h3"}
            ]
        }"#;
        let msg: WireMessage = serde_json::from_str(raw).unwrap();
        let WireMessage::PriceChange(pc) = msg else {
            panic!("expected PriceChange")
        };
        assert_eq!(pc.market, "0xmarket");
        assert_eq!(pc.timestamp, "1757908892351");
        assert_eq!(pc.price_changes.len(), 3);
        assert_eq!(pc.price_changes[0].side, "BUY");
        assert_eq!(pc.price_changes[2].best_ask, Some(dec("0.56")));
        assert_eq!(pc.price_changes[2].hash, "h3");
    }

    #[test]
    fn parse_wire_last_trade_price() {
        let raw = r#"{
            "event_type": "last_trade_price",
            "asset_id": "a1",
            "market": "0xm",
            "price": "0.42",
            "size": "75",
            "side": "BUY",
            "fee_rate_bps": "10",
            "transaction_hash": "0xtx",
            "timestamp": "1757908892351"
        }"#;
        let msg: WireMessage = serde_json::from_str(raw).unwrap();
        let WireMessage::LastTradePrice(t) = msg else {
            panic!("expected trade")
        };
        assert_eq!(t.price, dec("0.42"));
        assert_eq!(t.fee_rate_bps, "10");
        assert_eq!(t.transaction_hash, "0xtx");
    }

    #[test]
    fn parse_wire_tick_size_change() {
        let raw = r#"{
            "event_type": "tick_size_change",
            "asset_id": "a1",
            "market": "0xm",
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
            "timestamp": "1757908892351"
        }"#;
        let msg: WireMessage = serde_json::from_str(raw).unwrap();
        let WireMessage::TickSizeChange(c) = msg else {
            panic!("expected tick")
        };
        assert_eq!(c.old_tick_size, dec("0.01"));
        assert_eq!(c.new_tick_size, dec("0.001"));
    }

    #[test]
    fn parse_wire_frame_single_object() {
        let raw = r#"{"event_type": "tick_size_change", "asset_id": "a", "market": "m",
            "old_tick_size": "0.01", "new_tick_size": "0.001", "timestamp": "1"}"#;
        let frame: WireFrame = serde_json::from_str(raw).unwrap();
        assert_eq!(frame.into_iter().count(), 1);
    }

    #[test]
    fn parse_wire_frame_array() {
        let raw = r#"[
            {"event_type": "tick_size_change", "asset_id": "a", "market": "m",
             "old_tick_size": "0.01", "new_tick_size": "0.001", "timestamp": "1"},
            {"event_type": "tick_size_change", "asset_id": "b", "market": "m",
             "old_tick_size": "0.02", "new_tick_size": "0.002", "timestamp": "2"}
        ]"#;
        let frame: WireFrame = serde_json::from_str(raw).unwrap();
        assert_eq!(frame.into_iter().count(), 2);
    }

    // ---- Explode --------------------------------------------------------

    #[test]
    fn explode_book_is_one_to_one() {
        let raw = r#"{
            "event_type": "book", "asset_id": "a", "market": "m",
            "bids": [{"price": "0.4", "size": "10"}],
            "asks": [{"price": "0.5", "size": "10"}],
            "timestamp": "1", "hash": "h"
        }"#;
        let msg: WireMessage = serde_json::from_str(raw).unwrap();
        let mut out = Vec::new();
        explode(msg, &mut out);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], Event::Book { .. }));
    }

    #[test]
    fn explode_price_change_is_one_to_n() {
        let raw = r#"{
            "event_type": "price_change", "market": "m", "timestamp": "1",
            "price_changes": [
                {"asset_id": "a1", "price": "0.4", "size": "10", "side": "BUY", "hash": "h1"},
                {"asset_id": "a1", "price": "0.5", "size": "20", "side": "SELL", "hash": "h2"},
                {"asset_id": "a2", "price": "0.6", "size": "30", "side": "BUY", "hash": "h3"}
            ]
        }"#;
        let msg: WireMessage = serde_json::from_str(raw).unwrap();
        let mut out = Vec::new();
        explode(msg, &mut out);
        assert_eq!(out.len(), 3);
        for ev in &out {
            assert_eq!(ev.market(), "m");
            assert_eq!(ev.timestamp(), "1");
            assert!(matches!(ev, Event::PriceChange { .. }));
        }
    }

    // ---- Sink serialization ---------------------------------------------

    /// Round-trip via `serde_json::Value` to compare structurally rather
    /// than by byte order; we still assert specific keys are present /
    /// present and that decimal formatting matches the wire representation.
    fn to_value(ev: &Event) -> Value {
        serde_json::to_value(ev).unwrap()
    }

    #[test]
    fn serialize_book_matches_python_shape() {
        let ev = Event::Book {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1757908892351".into(),
            bids: vec![Level::new(dec("0.41"), dec("100"))],
            asks: vec![Level::new(dec("0.42"), dec("200"))],
            hash: Some("h".into()),
        };
        let v = to_value(&ev);
        assert_eq!(v["event_type"], "book");
        assert_eq!(v["market"], "0xm");
        assert_eq!(v["asset_id"], "a1");
        // bids/asks must be array-of-arrays of strings (Python's _dec format).
        assert_eq!(v["bids"], serde_json::json!([["0.41", "100"]]));
        assert_eq!(v["asks"], serde_json::json!([["0.42", "200"]]));
        assert_eq!(v["hash"], "h");
        assert_eq!(v["timestamp"], "1757908892351");
    }

    #[test]
    fn serialize_book_omits_hash_when_none() {
        let ev = Event::Book {
            market: "0xm".into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            bids: vec![],
            asks: vec![],
            hash: None,
        };
        let v = to_value(&ev);
        assert!(v.get("hash").is_none());
    }

    #[test]
    fn serialize_price_change_matches_python_shape() {
        let ev = Event::PriceChange {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1".into(),
            best_bid: Some(dec("0.40")),
            best_ask: Some(dec("0.42")),
            hash: Some("h".into()),
            price: dec("0.41"),
            size: dec("100"),
            side: "BUY".into(),
        };
        let v = to_value(&ev);
        assert_eq!(v["event_type"], "price_change");
        assert_eq!(v["best_bid"], "0.40");
        assert_eq!(v["best_ask"], "0.42");
        assert_eq!(v["price"], "0.41");
        assert_eq!(v["size"], "100");
        assert_eq!(v["side"], "BUY");
        assert_eq!(v["hash"], "h");
        assert_eq!(v["timestamp"], "1");
    }

    #[test]
    fn serialize_price_change_omits_optional_when_none() {
        let ev = Event::PriceChange {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            best_bid: None,
            best_ask: None,
            hash: None,
            price: dec("0.5"),
            size: dec("10"),
            side: "SELL".into(),
        };
        let v = to_value(&ev);
        assert!(v.get("best_bid").is_none());
        assert!(v.get("best_ask").is_none());
        assert!(v.get("hash").is_none());
    }

    #[test]
    fn serialize_last_trade_price_matches_python_shape() {
        let ev = Event::LastTradePrice {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1".into(),
            price: dec("0.42"),
            size: dec("75"),
            side: "BUY".into(),
            fee_rate_bps: "10".into(),
            transaction_hash: "0xtx".into(),
        };
        let v = to_value(&ev);
        assert_eq!(v["event_type"], "last_trade_price");
        assert_eq!(v["price"], "0.42");
        assert_eq!(v["size"], "75");
        assert_eq!(v["side"], "BUY");
        assert_eq!(v["fee_rate_bps"], "10");
        assert_eq!(v["transaction_hash"], "0xtx");
        assert_eq!(v["timestamp"], "1");
    }

    #[test]
    fn serialize_tick_size_change_matches_python_shape() {
        let ev = Event::TickSizeChange {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1".into(),
            old_tick_size: dec("0.01"),
            new_tick_size: dec("0.001"),
        };
        let v = to_value(&ev);
        assert_eq!(v["event_type"], "tick_size_change");
        assert_eq!(v["old_tick_size"], "0.01");
        assert_eq!(v["new_tick_size"], "0.001");
        assert_eq!(v["timestamp"], "1");
    }

    // ---- strip_hash -----------------------------------------------------

    #[test]
    fn strip_hash_clears_book_and_price_change_only() {
        let mut book = Event::Book {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            bids: vec![],
            asks: vec![],
            hash: Some("h".into()),
        };
        book.strip_hash();
        assert!(matches!(book, Event::Book { hash: None, .. }));

        let mut pc = Event::PriceChange {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            best_bid: None,
            best_ask: None,
            hash: Some("h".into()),
            price: dec("0.5"),
            size: dec("10"),
            side: "BUY".into(),
        };
        pc.strip_hash();
        assert!(matches!(pc, Event::PriceChange { hash: None, .. }));

        // LastTradePrice's transaction_hash must NOT be stripped.
        let mut t = Event::LastTradePrice {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            price: dec("0.5"),
            size: dec("10"),
            side: "BUY".into(),
            fee_rate_bps: "10".into(),
            transaction_hash: "0xtx".into(),
        };
        t.strip_hash();
        if let Event::LastTradePrice {
            transaction_hash, ..
        } = &t
        {
            assert_eq!(
                transaction_hash, "0xtx",
                "transaction_hash should not be stripped"
            );
        }
    }

    // ---- accessors ------------------------------------------------------

    #[test]
    fn accessors_return_expected_values() {
        let ev = Event::TickSizeChange {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "12345".into(),
            old_tick_size: dec("0.01"),
            new_tick_size: dec("0.001"),
        };
        assert_eq!(ev.kind(), "tick_size_change");
        assert_eq!(ev.market(), "m");
        assert_eq!(ev.asset_id(), "a");
        assert_eq!(ev.timestamp(), "12345");
    }

    #[test]
    fn level_serializes_as_array_pair() {
        let l = Level::new(dec("0.4100"), dec("100"));
        let s = serde_json::to_string(&l).unwrap();
        // Trailing zeros preserved (matches Python str(Decimal('0.4100')) == '0.4100').
        assert_eq!(s, r#"["0.4100","100"]"#);
    }
}
