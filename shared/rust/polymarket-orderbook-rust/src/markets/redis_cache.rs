//! Load active markets from the Redis cache key (default: `polymarket:active_markets`).
//!
//! The cache value is a JSON document of the form:
//! ```json
//! {"markets": [{"market": "0x...", "yes_asset_id": "...", "no_asset_id": "..."}, ...]}
//! ```

use anyhow::{Context, Result};
use redis::AsyncCommands;
use serde::Deserialize;
use tracing::{info, warn};

use crate::events::Market;

#[derive(Debug, Deserialize)]
struct CacheDocument {
    markets: Vec<CacheEntry>,
}

#[derive(Debug, Deserialize)]
struct CacheEntry {
    market: String,
    yes_asset_id: String,
    no_asset_id: String,
}

/// Load active markets from the Redis cache. Returns an empty vec if the
/// key doesn't exist (matching `market_events.py::load_markets_from_redis`).
pub async fn load(redis_url: &str, key: &str) -> Result<Vec<Market>> {
    let client = redis::Client::open(redis_url).context("open redis client")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("connect redis")?;

    let raw: Option<String> = conn.get(key).await.context("redis GET")?;
    let Some(raw) = raw else {
        warn!(key, "no active markets cache in Redis");
        return Ok(Vec::new());
    };

    let doc: CacheDocument = serde_json::from_str(&raw).context("parse cache JSON")?;
    let markets: Vec<Market> = doc
        .markets
        .into_iter()
        .map(|e| Market::new(e.market, e.yes_asset_id, e.no_asset_id))
        .collect();

    info!(count = markets.len(), key, "loaded markets from Redis cache");
    Ok(markets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cache_document() {
        let raw = r#"{
            "markets": [
                {"market": "0xabc", "yes_asset_id": "y1", "no_asset_id": "n1"},
                {"market": "0xdef", "yes_asset_id": "y2", "no_asset_id": "n2"}
            ]
        }"#;
        let doc: CacheDocument = serde_json::from_str(raw).unwrap();
        assert_eq!(doc.markets.len(), 2);
        assert_eq!(doc.markets[0].market, "0xabc");
        assert_eq!(doc.markets[1].yes_asset_id, "y2");
    }

    #[test]
    fn parse_empty_markets_array() {
        let raw = r#"{"markets": []}"#;
        let doc: CacheDocument = serde_json::from_str(raw).unwrap();
        assert_eq!(doc.markets.len(), 0);
    }
}
