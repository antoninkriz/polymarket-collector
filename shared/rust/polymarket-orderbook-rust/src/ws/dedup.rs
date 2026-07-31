//! Global dedup: a two-generation rolling `HashSet<u64>` and the
//! [`DedupForwarder`] that all Connections push events into.
//!
//! ## Why this exists
//!
//! Each asset is subscribed on exactly two connections for redundancy.
//! The dedup layer ensures each event reaches the sink exactly once.
//! A single `Arc<DedupForwarder>` is shared by every connection in the
//! pool; duplicates are silently absorbed.
//!
//! ## Dedup keys
//!
//! Each event collapses to a `u64`. For the two event types Polymarket gives
//! us a content hash for, we just parse the first 16 hex chars of that hash
//! into a `u64` — the source is already a cryptographic hash, so any 64-bit
//! slice is uniformly distributed and there's no point re-hashing it. For
//! the other two we hash the identifying tuple with [`DefaultHasher`].
//!
//! | event              | key source                                  |
//! |--------------------|---------------------------------------------|
//! | `book`             | `hex_prefix_u64(hash)`                      |
//! | `price_change`     | `hex_prefix_u64(hash)`  (per exploded entry)|
//! | `last_trade_price` | `DefaultHasher(market, asset, timestamp, price, size, side, fee_rate, transaction_hash)` |
//! | `tick_size_change` | `DefaultHasher(asset, timestamp, old, new)` |
//!
//! `last_trade_price` cannot use `transaction_hash` alone because a single
//! Ethereum transaction can settle multiple fills.
//!
//! ## Two-generation cache
//!
//! Each `check_and_insert` looks in both `current` and `previous`. Every
//! `rotate_every` interval, `previous` is dropped, `current` becomes
//! `previous`, and `current` is reset to empty. Entries therefore live
//! between `rotate_every` and `2 * rotate_every` — i.e. the effective TTL
//! lower bound is `rotate_every` and the upper bound is `2 * rotate_every`.
//!
//! All connections share `Arc<DedupForwarder>`, so the cache is behind a
//! `Mutex`. Lock contention is negligible — the lock-and-insert path is
//! ~tens of nanoseconds.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use crate::events::Event;

/// Two-generation rolling dedup cache. See module docs.
pub struct DedupCache {
    current: HashSet<u64>,
    previous: HashSet<u64>,
    rotate_every: Duration,
    last_rotate: Instant,
}

impl DedupCache {
    pub fn new(ttl: Duration) -> Self {
        // Half the TTL is the rotation interval; effective lifetime of an
        // entry is then in [ttl/2, ttl].
        let rotate_every = ttl / 2;
        Self {
            current: HashSet::new(),
            previous: HashSet::new(),
            rotate_every,
            last_rotate: Instant::now(),
        }
    }

    /// Check whether `key` was already seen recently. Returns `true` if this
    /// is the **first** sighting (caller should forward the event), `false`
    /// if it's a duplicate (caller should drop it).
    pub fn check_and_insert(&mut self, key: u64) -> bool {
        if self.current.contains(&key) || self.previous.contains(&key) {
            return false;
        }
        self.current.insert(key);
        true
    }

    /// Rotate generations if enough time has elapsed. Cheap branch + maybe
    /// one swap; safe to call on every event.
    pub fn maybe_rotate(&mut self) {
        if self.last_rotate.elapsed() >= self.rotate_every {
            self.previous = std::mem::take(&mut self.current);
            self.last_rotate = Instant::now();
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.current.len() + self.previous.len()
    }
}

/// Compute the dedup key for an [`Event`]. See module docs for the per-kind
/// strategy. Panics if a `Book` or `PriceChange` reaches this point with a
/// missing hash, which the wire layer (where `hash: String` is required)
/// rules out.
pub fn dedup_key(ev: &Event) -> u64 {
    match ev {
        Event::Book { hash, .. } => hex_prefix_u64(
            hash.as_deref()
                .expect("Book.hash must be present (wire layer enforces this)"),
        ),
        Event::PriceChange { hash, .. } => hex_prefix_u64(
            hash.as_deref()
                .expect("PriceChange.hash must be present (wire layer enforces this)"),
        ),
        Event::LastTradePrice {
            market,
            asset_id,
            timestamp,
            price,
            size,
            side,
            fee_rate_bps,
            transaction_hash,
        } => {
            let mut h = DefaultHasher::new();
            "last_trade_price".hash(&mut h);
            market.hash(&mut h);
            asset_id.hash(&mut h);
            timestamp.hash(&mut h);
            // Decimal doesn't impl Hash; round-trip via its string form.
            price.to_string().hash(&mut h);
            size.to_string().hash(&mut h);
            side.hash(&mut h);
            fee_rate_bps.hash(&mut h);
            transaction_hash.hash(&mut h);
            h.finish()
        }
        Event::TickSizeChange {
            market,
            asset_id,
            timestamp,
            old_tick_size,
            new_tick_size,
        } => {
            let mut h = DefaultHasher::new();
            "tick_size_change".hash(&mut h);
            market.hash(&mut h);
            asset_id.hash(&mut h);
            timestamp.hash(&mut h);
            old_tick_size.to_string().hash(&mut h);
            new_tick_size.to_string().hash(&mut h);
            h.finish()
        }
    }
}

/// Parse the first 16 hex chars of a Polymarket-style hash into a `u64`.
/// Strips a `0x` prefix if present. Panics if the input is shorter than
/// 16 hex chars or contains non-hex characters — both indicate a Polymarket
/// protocol violation we want to surface loudly.
fn hex_prefix_u64(s: &str) -> u64 {
    let s = s.strip_prefix("0x").unwrap_or(s);
    let prefix = s
        .get(..16)
        .expect("polymarket hash must be at least 16 hex chars");
    u64::from_str_radix(prefix, 16).expect("polymarket hash must be valid hex")
}

/// Dedup-aware wrapper around the global event channel. Shared by all
/// Connections in the pool via `Arc`. The forwarder is the *only* path
/// from a Connection's `handle_text` loop to the sink — duplicates are
/// silently absorbed (and counted), new events flow through to the global
/// channel with the same `try_send` semantics as before (Full → caller
/// exits the process; Closed → caller shuts down its connection cleanly).
pub struct DedupForwarder {
    cache: Mutex<DedupCache>,
    global_tx: mpsc::Sender<Event>,
    /// Total events absorbed as duplicates by this forwarder.
    pub dropped: AtomicU64,
    /// Total events forwarded to the global channel.
    pub forwarded: AtomicU64,
}

impl DedupForwarder {
    pub fn new(ttl: Duration, global_tx: mpsc::Sender<Event>) -> Arc<Self> {
        Arc::new(Self {
            cache: Mutex::new(DedupCache::new(ttl)),
            global_tx,
            dropped: AtomicU64::new(0),
            forwarded: AtomicU64::new(0),
        })
    }

    /// Forward an event if it hasn't been seen recently. Mirrors the
    /// `mpsc::Sender::try_send` signature so the call site in
    /// [`crate::ws::connection`] can stay the same as before.
    pub fn try_send(&self, ev: Event) -> Result<(), mpsc::error::TrySendError<Event>> {
        let key = dedup_key(&ev);
        {
            let mut cache = self.cache.lock().expect("dedup cache mutex poisoned");
            cache.maybe_rotate();
            if !cache.check_and_insert(key) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }
        match self.global_tx.try_send(ev) {
            Ok(()) => {
                self.forwarded.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Capacity of the underlying global channel. Used by the
    /// `[QUEUE-OVERFLOW]` log path.
    pub fn capacity(&self) -> usize {
        self.global_tx.capacity()
    }

    pub fn max_capacity(&self) -> usize {
        self.global_tx.max_capacity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    fn book(asset: &str, hash: &str) -> Event {
        Event::Book {
            market: "m".into(),
            asset_id: asset.into(),
            timestamp: "1".into(),
            bids: vec![],
            asks: vec![],
            hash: Some(hash.into()),
        }
    }

    fn pc(asset: &str, hash: &str) -> Event {
        Event::PriceChange {
            market: "m".into(),
            asset_id: asset.into(),
            timestamp: "1".into(),
            best_bid: None,
            best_ask: None,
            hash: Some(hash.into()),
            price: dec("0.5"),
            size: dec("10"),
            side: "BUY".into(),
        }
    }

    fn trade(market: &str, tx: &str, price: &str) -> Event {
        Event::LastTradePrice {
            market: market.into(),
            asset_id: "a".into(),
            timestamp: "1".into(),
            price: dec(price),
            size: dec("10"),
            side: "BUY".into(),
            fee_rate_bps: "10".into(),
            transaction_hash: tx.into(),
        }
    }

    fn tick(asset: &str, ts: &str) -> Event {
        Event::TickSizeChange {
            market: "m".into(),
            asset_id: asset.into(),
            timestamp: ts.into(),
            old_tick_size: dec("0.01"),
            new_tick_size: dec("0.001"),
        }
    }

    // ---- hex_prefix_u64 ------------------------------------------------

    #[test]
    fn hex_prefix_parses_real_polymarket_hash() {
        // 40-char SHA-1 shaped hash from a real Polymarket frame.
        let h = "56621a121a47ed9333273e21c83b660cff37ae50";
        assert_eq!(hex_prefix_u64(h), 0x56621a121a47ed93);
    }

    #[test]
    fn hex_prefix_strips_0x() {
        assert_eq!(
            hex_prefix_u64("0x56621a121a47ed9333273e21c83b660cff37ae50"),
            0x56621a121a47ed93,
        );
    }

    #[test]
    #[should_panic(expected = "at least 16 hex chars")]
    fn hex_prefix_panics_on_short_input() {
        hex_prefix_u64("abc");
    }

    // ---- dedup_key per event kind --------------------------------------

    #[test]
    fn dedup_key_same_book_collides() {
        let h = "56621a121a47ed9333273e21c83b660cff37ae50";
        let k1 = dedup_key(&book("a", h));
        let k2 = dedup_key(&book("a", h));
        assert_eq!(k1, k2);
    }

    #[test]
    fn dedup_key_different_book_hashes_distinct() {
        let k1 = dedup_key(&book("a", "56621a121a47ed9333273e21c83b660cff37ae50"));
        let k2 = dedup_key(&book("a", "7733aabbccdd00112233445566778899aabbccdd"));
        assert_ne!(k1, k2);
    }

    #[test]
    fn dedup_key_price_change_uses_hash() {
        let k1 = dedup_key(&pc("a", "56621a121a47ed9333273e21c83b660cff37ae50"));
        let k2 = dedup_key(&pc("a", "56621a121a47ed9333273e21c83b660cff37ae50"));
        assert_eq!(k1, k2);
    }

    #[test]
    fn dedup_key_trade_includes_full_tuple() {
        // Same tx, different price → distinct (multiple fills in one tx).
        let k1 = dedup_key(&trade("m", "0xtx", "0.41"));
        let k2 = dedup_key(&trade("m", "0xtx", "0.42"));
        assert_ne!(k1, k2);
    }

    #[test]
    fn dedup_key_trade_identical_collides() {
        let k1 = dedup_key(&trade("m", "0xtx", "0.41"));
        let k2 = dedup_key(&trade("m", "0xtx", "0.41"));
        assert_eq!(k1, k2);
    }

    #[test]
    fn dedup_key_tick_distinct_per_timestamp() {
        let k1 = dedup_key(&tick("a", "1"));
        let k2 = dedup_key(&tick("a", "2"));
        assert_ne!(k1, k2);
    }

    // ---- DedupCache ----------------------------------------------------

    #[test]
    fn cache_first_sighting_is_new_second_is_dup() {
        let mut c = DedupCache::new(Duration::from_secs(10));
        assert!(c.check_and_insert(42));
        assert!(!c.check_and_insert(42));
        assert!(c.check_and_insert(43));
        assert!(!c.check_and_insert(43));
    }

    #[test]
    fn cache_rotation_drops_old_entries_after_full_ttl() {
        // ttl=20ms → rotate_every=10ms.
        let mut c = DedupCache::new(Duration::from_millis(20));
        assert!(c.check_and_insert(1));
        std::thread::sleep(Duration::from_millis(15));
        c.maybe_rotate(); // moves 1 from current to previous
        assert!(!c.check_and_insert(1), "still seen via previous");
        std::thread::sleep(Duration::from_millis(15));
        c.maybe_rotate(); // drops previous (which had 1), new previous = empty current
        assert!(c.check_and_insert(1), "fully aged out, treated as new");
    }

    #[test]
    fn cache_rotation_preserves_recent_entries() {
        let mut c = DedupCache::new(Duration::from_millis(100));
        c.check_and_insert(1);
        c.maybe_rotate(); // no time has passed → no-op
        assert!(!c.check_and_insert(1));
        assert_eq!(c.len(), 1);
    }

    // ---- DedupForwarder integration ------------------------------------

    #[tokio::test]
    async fn forwarder_passes_first_event_drops_second() {
        let (tx, mut rx) = mpsc::channel::<Event>(8);
        let fwd = DedupForwarder::new(Duration::from_secs(10), tx);
        let h = "56621a121a47ed9333273e21c83b660cff37ae50";
        fwd.try_send(book("a", h)).unwrap();
        fwd.try_send(book("a", h)).unwrap(); // duplicate, absorbed
        assert!(rx.recv().await.is_some(), "first event reaches sink");
        assert!(rx.try_recv().is_err(), "no second event");
        assert_eq!(fwd.forwarded.load(Ordering::Relaxed), 1);
        assert_eq!(fwd.dropped.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn forwarder_returns_full_when_global_channel_full() {
        let (tx, _rx) = mpsc::channel::<Event>(1);
        let fwd = DedupForwarder::new(Duration::from_secs(10), tx);
        let h1 = "56621a121a47ed9333273e21c83b660cff37ae50";
        let h2 = "11112222333344445566778899aabbccddeeff00";
        fwd.try_send(book("a", h1)).unwrap();
        match fwd.try_send(book("a", h2)) {
            Err(mpsc::error::TrySendError::Full(_)) => {}
            other => panic!("expected Full, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn forwarder_returns_closed_when_receiver_dropped() {
        let (tx, rx) = mpsc::channel::<Event>(8);
        drop(rx);
        let fwd = DedupForwarder::new(Duration::from_secs(10), tx);
        let h = "56621a121a47ed9333273e21c83b660cff37ae50";
        match fwd.try_send(book("a", h)) {
            Err(mpsc::error::TrySendError::Closed(_)) => {}
            other => panic!("expected Closed, got {other:?}"),
        }
    }
}
