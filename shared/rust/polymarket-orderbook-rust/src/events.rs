//! Wire-format event types for the Polymarket WSS market channel.
//!
//! [`WireMessage`] mirrors the incoming payload, including the
//! `price_change` wrapper. [`Event`] is the normalized child stored in V3;
//! one parent price change fans out in source order. Wire `bids` and `asks`
//! use objects while [`Level`] stores compact two-element arrays.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

fn deserialize_decimal_string_option<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    Option::<String>::deserialize(deserializer)?
        .map(|value| value.parse::<Decimal>().map_err(D::Error::custom))
        .transpose()
}

// =====================================================================
// Wire layer (deserialize-only)
// =====================================================================

#[derive(Debug, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum WireMessage {
    Book(WireBook),
    PriceChange(WirePriceChange),
    LastTradePrice(WireLastTradePrice),
    TickSizeChange(WireTickSizeChange),
    BestBidAsk(WireBestBidAsk),
    NewMarket(WireNewMarket),
    MarketResolved(WireMarketResolved),
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
    #[serde(default, deserialize_with = "deserialize_decimal_string_option")]
    pub best_bid: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_decimal_string_option")]
    pub best_ask: Option<Decimal>,
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

#[derive(Debug, Deserialize)]
pub struct WireBestBidAsk {
    pub market: String,
    pub asset_id: String,
    pub timestamp: String,
    #[serde(default, deserialize_with = "deserialize_decimal_string_option")]
    pub best_bid: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_decimal_string_option")]
    pub best_ask: Option<Decimal>,
    #[serde(default, deserialize_with = "deserialize_decimal_string_option")]
    pub spread: Option<Decimal>,
}

#[derive(Debug, Deserialize)]
pub struct WireNewMarket {
    pub id: String,
    pub market: String,
    pub timestamp: String,
    #[serde(default)]
    pub assets_ids: Vec<String>,
    #[serde(default)]
    pub outcomes: Vec<String>,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct WireMarketResolved {
    pub id: String,
    pub market: String,
    pub timestamp: String,
    #[serde(default)]
    pub assets_ids: Vec<String>,
    #[serde(default)]
    pub winning_asset_id: Option<String>,
    #[serde(default)]
    pub winning_outcome: Option<String>,
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
    BestBidAsk {
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
        #[serde(
            skip_serializing_if = "Option::is_none",
            with = "rust_decimal::serde::str_option"
        )]
        spread: Option<Decimal>,
    },
    NewMarket {
        id: String,
        market: String,
        timestamp: String,
        assets_ids: Vec<String>,
        outcomes: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        question: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        slug: Option<String>,
    },
    MarketResolved {
        id: String,
        market: String,
        timestamp: String,
        assets_ids: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        winning_asset_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        winning_outcome: Option<String>,
    },
}

impl Event {
    /// Return the token ID for token-scoped events. Market lifecycle events
    /// intentionally return `None`: exploding one lifecycle notification
    /// into one row per token would fabricate duplicate events.
    pub fn asset_id(&self) -> Option<&str> {
        match self {
            Event::Book { asset_id, .. }
            | Event::PriceChange { asset_id, .. }
            | Event::LastTradePrice { asset_id, .. }
            | Event::TickSizeChange { asset_id, .. }
            | Event::BestBidAsk { asset_id, .. } => Some(asset_id),
            Event::NewMarket { .. } | Event::MarketResolved { .. } => None,
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
        }),
        WireMessage::PriceChange(pc) => {
            for e in pc.price_changes {
                out.push(Event::PriceChange {
                    market: pc.market.clone(),
                    asset_id: e.asset_id,
                    timestamp: pc.timestamp.clone(),
                    best_bid: e.best_bid,
                    best_ask: e.best_ask,
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
        WireMessage::BestBidAsk(b) => out.push(Event::BestBidAsk {
            market: b.market,
            asset_id: b.asset_id,
            timestamp: b.timestamp,
            best_bid: b.best_bid,
            best_ask: b.best_ask,
            spread: b.spread,
        }),
        WireMessage::NewMarket(m) => out.push(Event::NewMarket {
            id: m.id,
            market: m.market,
            timestamp: m.timestamp,
            assets_ids: m.assets_ids,
            outcomes: m.outcomes,
            question: m.question,
            slug: m.slug,
        }),
        WireMessage::MarketResolved(m) => out.push(Event::MarketResolved {
            id: m.id,
            market: m.market,
            timestamp: m.timestamp,
            assets_ids: m.assets_ids,
            winning_asset_id: m.winning_asset_id,
            winning_outcome: m.winning_outcome,
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
    fn parse_wire_best_bid_ask_with_empty_side() {
        let raw = r#"{
            "event_type": "best_bid_ask",
            "asset_id": "a1",
            "market": "0xm",
            "best_bid": "0.41",
            "best_ask": null,
            "spread": null,
            "timestamp": "1757908892351"
        }"#;
        let msg: WireMessage = serde_json::from_str(raw).unwrap();
        let WireMessage::BestBidAsk(b) = msg else {
            panic!("expected best bid/ask")
        };
        assert_eq!(b.best_bid, Some(dec("0.41")));
        assert_eq!(b.best_ask, None);
        assert_eq!(b.spread, None);
    }

    #[test]
    fn parse_wire_new_market() {
        let raw = r#"{
            "event_type": "new_market",
            "id": "123456",
            "question": "Will it rain?",
            "market": "0xmarket",
            "slug": "will-it-rain",
            "assets_ids": ["yes", "no"],
            "outcomes": ["Yes", "No"],
            "timestamp": "1757908892351"
        }"#;
        let msg: WireMessage = serde_json::from_str(raw).unwrap();
        let WireMessage::NewMarket(m) = msg else {
            panic!("expected new market")
        };
        assert_eq!(m.id, "123456");
        assert_eq!(m.assets_ids, ["yes", "no"]);
        assert_eq!(m.outcomes, ["Yes", "No"]);
    }

    #[test]
    fn parse_wire_market_resolved() {
        let raw = r#"{
            "event_type": "market_resolved",
            "id": "123456",
            "market": "0xmarket",
            "assets_ids": ["yes", "no"],
            "winning_asset_id": "yes",
            "winning_outcome": "Yes",
            "timestamp": "1757908892351"
        }"#;
        let msg: WireMessage = serde_json::from_str(raw).unwrap();
        let WireMessage::MarketResolved(m) = msg else {
            panic!("expected resolved market")
        };
        assert_eq!(m.winning_asset_id.as_deref(), Some("yes"));
        assert_eq!(m.winning_outcome.as_deref(), Some("Yes"));
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
            assert!(matches!(
                ev,
                Event::PriceChange {
                    market,
                    timestamp,
                    ..
                } if market == "m" && timestamp == "1"
            ));
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
    fn serialize_book_uses_compact_level_shape() {
        let ev = Event::Book {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1757908892351".into(),
            bids: vec![Level::new(dec("0.41"), dec("100"))],
            asks: vec![Level::new(dec("0.42"), dec("200"))],
        };
        let v = to_value(&ev);
        assert_eq!(v["event_type"], "book");
        assert_eq!(v["market"], "0xm");
        assert_eq!(v["asset_id"], "a1");
        // Stored depth is an array of [price, size] string pairs.
        assert_eq!(v["bids"], serde_json::json!([["0.41", "100"]]));
        assert_eq!(v["asks"], serde_json::json!([["0.42", "200"]]));
        assert!(v.get("hash").is_none());
        assert_eq!(v["timestamp"], "1757908892351");
    }

    #[test]
    fn serialize_price_change_matches_python_shape() {
        let ev = Event::PriceChange {
            market: "0xm".into(),
            asset_id: "a1".into(),
            timestamp: "1".into(),
            best_bid: Some(dec("0.40")),
            best_ask: Some(dec("0.42")),
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
        assert!(v.get("hash").is_none());
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
            price: dec("0.5"),
            size: dec("10"),
            side: "SELL".into(),
        };
        let v = to_value(&ev);
        assert!(v.get("best_bid").is_none());
        assert!(v.get("best_ask").is_none());
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

    #[test]
    fn asset_accessor_returns_expected_value() {
        let ev = Event::TickSizeChange {
            market: "m".into(),
            asset_id: "a".into(),
            timestamp: "12345".into(),
            old_tick_size: dec("0.01"),
            new_tick_size: dec("0.001"),
        };
        assert_eq!(ev.asset_id(), Some("a"));
    }

    #[test]
    fn lifecycle_event_has_no_synthetic_asset_id() {
        let ev = Event::MarketResolved {
            id: "123456".into(),
            market: "m".into(),
            timestamp: "1".into(),
            assets_ids: vec!["yes".into(), "no".into()],
            winning_asset_id: Some("yes".into()),
            winning_outcome: Some("Yes".into()),
        };
        assert_eq!(ev.asset_id(), None);
    }

    #[test]
    fn level_serializes_as_array_pair() {
        let l = Level::new(dec("0.4100"), dec("100"));
        let s = serde_json::to_string(&l).unwrap();
        // Trailing zeros preserved (matches Python str(Decimal('0.4100')) == '0.4100').
        assert_eq!(s, r#"["0.4100","100"]"#);
    }
}
