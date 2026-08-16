//! Typed Redis restart cache for active market subscriptions.
//!
//! The JSON representation is:
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
#[derive(Debug)]
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

    pub fn to_json(self) -> Result<String> {
        let serialized = SerializedCacheDocument {
            fetched_at: self.fetched_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            markets: self
                .markets
                .into_iter()
                .map(|market| {
                    let [yes_asset_id, no_asset_id] = market.assets;
                    CacheEntry {
                        market: market.hash,
                        yes_asset_id,
                        no_asset_id,
                    }
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

#[derive(Serialize)]
struct BorrowedCacheDocument<'a> {
    fetched_at: String,
    markets: Vec<BorrowedCacheEntry<'a>>,
}

#[derive(Serialize)]
struct BorrowedCacheEntry<'a> {
    market: &'a str,
    yes_asset_id: &'a str,
    no_asset_id: &'a str,
}

/// Serialize the authoritative lifecycle state without cloning its IDs.
pub(crate) fn serialize_lifecycle_document<'a>(
    fetched_at: DateTime<Utc>,
    markets: impl ExactSizeIterator<Item = (&'a str, &'a [String; 2])>,
) -> Result<String> {
    ensure_market_count(markets.len())?;
    let mut entries: Vec<_> = markets
        .map(|(market, assets)| BorrowedCacheEntry {
            market,
            yes_asset_id: &assets[0],
            no_asset_id: &assets[1],
        })
        .collect();
    entries.sort_unstable_by(|left, right| left.market.cmp(right.market));
    serde_json::to_string(&BorrowedCacheDocument {
        fetched_at: fetched_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
        markets: entries,
    })
    .context("serialize cache JSON")
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

/// Atomically store a serialized restart-cache JSON document.
pub async fn save_json(redis_url: &str, key: &str, raw: String) -> Result<()> {
    let client = redis::Client::open(redis_url).context("open redis client")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect redis")?;
    conn.set::<_, _, ()>(key, raw).await.context("redis SET")
}

fn validate_markets(markets: &[Market]) -> Result<()> {
    ensure_market_count(markets.len())?;

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

fn ensure_market_count(count: usize) -> Result<()> {
    ensure!(
        count <= MAX_CACHE_MARKETS,
        "cache contains {count} markets, maximum is {MAX_CACHE_MARKETS}"
    );
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

    #[test]
    fn cache_market_limit_is_inclusive() {
        assert!(ensure_market_count(MAX_CACHE_MARKETS).is_ok());
        assert!(ensure_market_count(MAX_CACHE_MARKETS + 1).is_err());
    }

    #[test]
    fn lifecycle_serialization_matches_owned_document_and_sorts_markets() {
        let mut markets = vec![
            market("z\nmarket", "z\\yes", "z\"no"),
            market("a-market", "a-yes", "a-no"),
        ];
        let raw = serialize_lifecycle_document(
            timestamp(),
            markets
                .iter()
                .map(|market| (market.hash.as_str(), &market.assets)),
        )
        .unwrap();

        markets.sort_unstable_by(|left, right| left.hash.cmp(&right.hash));
        let owned = CacheDocument::new(timestamp(), markets)
            .unwrap()
            .to_json()
            .unwrap();
        assert_eq!(raw, owned);
        assert_eq!(
            raw,
            r#"{"fetched_at":"2026-08-14T12:34:56Z","markets":[{"market":"a-market","yes_asset_id":"a-yes","no_asset_id":"a-no"},{"market":"z\nmarket","yes_asset_id":"z\\yes","no_asset_id":"z\"no"}]}"#
        );
    }

    #[test]
    fn lifecycle_serialization_rejects_more_than_the_cache_limit() {
        let assets = ["yes".to_string(), "no".to_string()];
        let markets = std::iter::repeat_n(("market", &assets), MAX_CACHE_MARKETS + 1);
        let error = serialize_lifecycle_document(timestamp(), markets)
            .unwrap_err()
            .to_string();
        assert!(error.contains("maximum is 500000"), "{error}");
    }
}
