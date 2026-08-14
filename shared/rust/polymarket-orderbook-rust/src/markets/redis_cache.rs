//! Typed Redis restart cache for active market subscriptions.
//!
//! The JSON representation remains compatible with the Python producer:
//! ```json
//! {
//!   "fetched_at": "2026-08-14T12:34:56Z",
//!   "markets": [
//!     {"market": "0x...", "yes_asset_id": "...", "no_asset_id": "..."}
//!   ]
//! }
//! ```

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::events::Market;

/// Guards the collector against accidentally loading an unbounded cache value.
/// This leaves substantial headroom above the current active-market universe.
pub const MAX_CACHE_MARKETS: usize = 500_000;

/// Validated, complete restart-cache snapshot.
#[derive(Debug, Clone)]
pub struct CacheDocument {
    fetched_at: DateTime<Utc>,
    markets: Vec<Market>,
}

impl CacheDocument {
    pub fn new(fetched_at: DateTime<Utc>, markets: Vec<Market>) -> Result<Self> {
        validate_markets(&markets)?;
        Ok(Self {
            fetched_at,
            markets,
        })
    }

    pub fn fetched_at(&self) -> DateTime<Utc> {
        self.fetched_at
    }

    pub fn markets(&self) -> &[Market] {
        &self.markets
    }

    pub fn into_markets(self) -> Vec<Market> {
        self.markets
    }

    /// Cache age, saturating at zero when the producer timestamp is in the future.
    pub fn age(&self, now: DateTime<Utc>) -> Duration {
        now.signed_duration_since(self.fetched_at)
            .to_std()
            .unwrap_or_default()
    }

    pub fn age_now(&self) -> Duration {
        self.age(Utc::now())
    }

    pub fn from_json(raw: &str) -> Result<Self> {
        let serialized: SerializedCacheDocument =
            serde_json::from_str(raw).context("parse cache JSON")?;
        let fetched_at = DateTime::parse_from_rfc3339(&serialized.fetched_at)
            .context("parse cache fetched_at")?
            .with_timezone(&Utc);
        let markets = serialized
            .markets
            .into_iter()
            .map(|entry| Market::new(entry.market, entry.yes_asset_id, entry.no_asset_id))
            .collect();
        Self::new(fetched_at, markets)
    }

    pub fn to_json(&self) -> Result<String> {
        validate_markets(&self.markets)?;
        let serialized = SerializedCacheDocument {
            fetched_at: self.fetched_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            markets: self
                .markets
                .iter()
                .map(|market| CacheEntry {
                    market: market.hash.clone(),
                    yes_asset_id: market.assets[0].clone(),
                    no_asset_id: market.assets[1].clone(),
                })
                .collect(),
        };
        serde_json::to_string(&serialized).context("serialize cache JSON")
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SerializedCacheDocument {
    fetched_at: String,
    markets: Vec<CacheEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CacheEntry {
    market: String,
    yes_asset_id: String,
    no_asset_id: String,
}

/// Load and validate a complete restart-cache document.
pub async fn load_document(redis_url: &str, key: &str) -> Result<Option<CacheDocument>> {
    let client = redis::Client::open(redis_url).context("open redis client")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect redis")?;

    let raw: Option<String> = conn.get(key).await.context("redis GET")?;
    let Some(raw) = raw else {
        return Ok(None);
    };

    let document = CacheDocument::from_json(&raw)?;
    info!(
        count = document.markets.len(),
        age_seconds = document.age_now().as_secs(),
        key,
        "loaded markets from Redis cache"
    );
    Ok(Some(document))
}

/// Backward-compatible loader used by the current Python-owned startup path.
pub async fn load(redis_url: &str, key: &str) -> Result<Option<Vec<Market>>> {
    Ok(load_document(redis_url, key)
        .await?
        .map(CacheDocument::into_markets))
}

/// Atomically replace the cache key with one complete JSON snapshot.
pub async fn save(redis_url: &str, key: &str, document: &CacheDocument) -> Result<()> {
    // Serialize and validate before opening Redis so a bad snapshot cannot
    // replace the last known-good restart cache.
    let raw = document.to_json()?;
    save_json(redis_url, key, raw).await
}

/// Atomically store JSON previously produced by [`CacheDocument::to_json`].
/// This split lets callers move serialization to `spawn_blocking`.
pub async fn save_json(redis_url: &str, key: &str, raw: String) -> Result<()> {
    let client = redis::Client::open(redis_url).context("open redis client")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect redis")?;
    conn.set::<_, _, ()>(key, raw).await.context("redis SET")
}

/// Return the Unix timestamp of the latest Python-owned cache update.
///
/// Kept during the transition so the current startup freshness gate is
/// unchanged. Rust-owned cache consumers use [`CacheDocument::age`] instead.
pub async fn last_updated(redis_url: &str, key: &str) -> Result<Option<u64>> {
    let client = redis::Client::open(redis_url).context("open redis client")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect redis")?;

    let raw: Option<String> = conn.get(key).await.context("redis GET")?;
    raw.map(|value| value.parse().context("parse cache update timestamp"))
        .transpose()
}

fn validate_markets(markets: &[Market]) -> Result<()> {
    ensure!(
        markets.len() <= MAX_CACHE_MARKETS,
        "cache contains {} markets, maximum is {MAX_CACHE_MARKETS}",
        markets.len()
    );

    let mut condition_assets = HashMap::<&str, [&str; 2]>::with_capacity(markets.len());
    let mut asset_owner = HashMap::<&str, &str>::with_capacity(markets.len().saturating_mul(2));
    for market in markets {
        ensure!(
            !market.hash.trim().is_empty(),
            "cache market condition ID must not be empty"
        );
        ensure!(
            market.assets.iter().all(|asset| !asset.trim().is_empty()),
            "cache market {} has an empty asset ID",
            market.hash
        );
        ensure!(
            market.assets[0] != market.assets[1],
            "cache market {} uses asset {} for both outcomes",
            market.hash,
            market.assets[0]
        );

        let canonical = canonical_assets(&market.assets);
        if let Some(existing) = condition_assets.get(market.hash.as_str()) {
            ensure!(
                existing == &canonical,
                "cache market {} has conflicting asset pairs {:?} and {:?}",
                market.hash,
                existing,
                canonical
            );
            bail!("cache contains duplicate market {}", market.hash);
        }

        for asset in &market.assets {
            if let Some(owner) = asset_owner.get(asset.as_str()) {
                bail!(
                    "cache asset {asset} is shared by markets {owner} and {}",
                    market.hash
                );
            }
        }
        condition_assets.insert(market.hash.as_str(), canonical);
        for asset in &market.assets {
            asset_owner.insert(asset.as_str(), market.hash.as_str());
        }
    }
    Ok(())
}

fn canonical_assets(assets: &[String; 2]) -> [&str; 2] {
    if assets[0] <= assets[1] {
        [&assets[0], &assets[1]]
    } else {
        [&assets[1], &assets[0]]
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    fn timestamp() -> DateTime<Utc> {
        "2026-08-14T12:34:56Z".parse().unwrap()
    }

    fn market(hash: &str, yes: &str, no: &str) -> Market {
        Market::new(hash.into(), yes.into(), no.into())
    }

    #[test]
    fn python_document_round_trips_without_changing_asset_order() {
        let raw = r#"{
            "fetched_at": "2026-08-14T12:34:56Z",
            "markets": [
                {"market": "0xabc", "yes_asset_id": "z", "no_asset_id": "a"},
                {"market": "0xdef", "yes_asset_id": "y2", "no_asset_id": "n2"}
            ]
        }"#;
        let document = CacheDocument::from_json(raw).unwrap();
        assert_eq!(document.fetched_at(), timestamp());
        assert_eq!(document.markets()[0].hash, "0xabc");
        assert_eq!(document.markets()[0].assets, ["z", "a"]);

        let round_trip = CacheDocument::from_json(&document.to_json().unwrap()).unwrap();
        assert_eq!(round_trip.fetched_at(), timestamp());
        assert_eq!(round_trip.markets()[0].assets, ["z", "a"]);
        assert_eq!(
            round_trip.age(timestamp() + TimeDelta::seconds(65)),
            Duration::from_secs(65)
        );
    }

    #[test]
    fn corrupt_document_is_rejected() {
        let error = CacheDocument::from_json(
            r#"{"fetched_at":"not-a-time","markets":[{"market":"m","yes_asset_id":"a","no_asset_id":"b"}]}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("parse cache fetched_at"), "{error}");
    }

    #[test]
    fn conflicting_condition_is_order_insensitive() {
        let error = CacheDocument::new(
            timestamp(),
            vec![market("m", "a", "b"), market("m", "c", "a")],
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("conflicting asset pairs"), "{error}");

        let duplicate = CacheDocument::new(
            timestamp(),
            vec![market("m", "a", "b"), market("m", "b", "a")],
        )
        .unwrap_err()
        .to_string();
        assert!(duplicate.contains("duplicate market"), "{duplicate}");
    }

    #[test]
    fn shared_asset_is_rejected() {
        let error = CacheDocument::new(
            timestamp(),
            vec![market("first", "a", "b"), market("second", "c", "a")],
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("shared by markets first and second"),
            "{error}"
        );
    }

    #[test]
    fn empty_cache_is_valid_and_future_age_saturates_to_zero() {
        let document =
            CacheDocument::from_json(r#"{"fetched_at":"2026-08-14T12:34:56Z","markets":[]}"#)
                .unwrap();
        assert!(document.markets().is_empty());
        assert_eq!(
            document.age(timestamp() - TimeDelta::seconds(1)),
            Duration::ZERO
        );
    }
}
