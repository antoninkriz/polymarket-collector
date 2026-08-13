//! Fallback active-markets fetch from the Polymarket Gamma API.
//!
//! Used when `SKIP_ACTIVE_MARKETS_CACHE=true`. Pages over `/markets`,
//! filters to binary markets (exactly 2 outcomes / 2 CLOB tokens), and
//! produces [`Market`]s in the same shape as the Redis cache.
//!
//! The Python version (`gamma.py`) parallelizes pages with a semaphore.
//! This Rust port uses sequential pagination — it's a startup-only path
//! that only runs under `SKIP_ACTIVE_MARKETS_CACHE=true`, which the
//! production deployment doesn't set.

use anyhow::{Context, Result};
use serde::Deserialize;
use tracing::info;

use crate::events::Market;

pub const GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";
const FETCH_BATCH_SIZE: usize = 500;
const MAX_OFFSET: usize = 60_000;

/// Fetch all active binary markets from the Gamma API by paging.
pub async fn fetch_active_markets(http: &reqwest::Client) -> Result<Vec<Market>> {
    let mut all: Vec<Market> = Vec::new();
    let mut offset = 0_usize;

    loop {
        let url = format!("{GAMMA_BASE_URL}/markets");
        let resp = http
            .get(&url)
            .query(&[
                ("active", "true"),
                ("closed", "false"),
                ("limit", &FETCH_BATCH_SIZE.to_string()),
                ("offset", &offset.to_string()),
            ])
            .send()
            .await
            .context("gamma /markets request")?;
        if !resp.status().is_success() {
            anyhow::bail!("gamma /markets status: {}", resp.status());
        }
        let raw: Vec<RawMarket> = resp.json().await.context("gamma /markets JSON")?;
        let page_size = raw.len();

        for m in raw {
            if let Some(market) = parse_market(m) {
                all.push(market);
            }
        }

        info!(
            offset,
            fetched = page_size,
            total = all.len(),
            "gamma page fetched"
        );

        if page_size < FETCH_BATCH_SIZE {
            break;
        }
        offset += FETCH_BATCH_SIZE;
        if offset >= MAX_OFFSET {
            break;
        }
    }

    info!(total_binary_markets = all.len(), "gamma fetch complete");
    Ok(all)
}

/// Parse one Gamma market dict into a [`Market`], or `None` if it isn't binary.
/// Mirrors `gamma.py::parse_markets` filtering and YES/NO ordering.
fn parse_market(raw: RawMarket) -> Option<Market> {
    let clob_tokens = raw.clob_token_ids.into_strings().ok()?;
    let outcomes = raw.outcomes.into_strings().ok()?;

    if clob_tokens.len() != 2 || outcomes.len() != 2 {
        return None;
    }

    let (yes, no) = if outcomes[0] == "Yes" {
        (clob_tokens[0].clone(), clob_tokens[1].clone())
    } else {
        (clob_tokens[1].clone(), clob_tokens[0].clone())
    };

    Some(Market::new(raw.condition_id, yes, no))
}

#[derive(Debug, Deserialize)]
struct RawMarket {
    #[serde(rename = "conditionId", default)]
    condition_id: String,
    #[serde(rename = "clobTokenIds", default)]
    clob_token_ids: StringOrJsonArray,
    #[serde(default)]
    outcomes: StringOrJsonArray,
}

/// Gamma sometimes returns `clobTokenIds` and `outcomes` as a JSON array,
/// sometimes as a JSON-encoded string containing an array. Handle both.
#[derive(Debug, Deserialize, Default)]
#[serde(untagged)]
enum StringOrJsonArray {
    Array(Vec<String>),
    Encoded(String),
    #[default]
    Empty,
}

impl StringOrJsonArray {
    fn into_strings(self) -> Result<Vec<String>> {
        match self {
            StringOrJsonArray::Array(v) => Ok(v),
            StringOrJsonArray::Encoded(s) => {
                if s.is_empty() {
                    return Ok(Vec::new());
                }
                serde_json::from_str(&s).context("decode JSON-encoded array string")
            }
            StringOrJsonArray::Empty => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_market_with_array_form() {
        let raw = RawMarket {
            condition_id: "0xabc".into(),
            clob_token_ids: StringOrJsonArray::Array(vec!["t1".into(), "t2".into()]),
            outcomes: StringOrJsonArray::Array(vec!["Yes".into(), "No".into()]),
        };
        let m = parse_market(raw).unwrap();
        assert_eq!(m.hash, "0xabc");
        assert_eq!(m.yes(), "t1");
        assert_eq!(m.no(), "t2");
    }

    #[test]
    fn parse_market_with_encoded_string_form() {
        let raw = RawMarket {
            condition_id: "0xdef".into(),
            clob_token_ids: StringOrJsonArray::Encoded(r#"["t1","t2"]"#.into()),
            outcomes: StringOrJsonArray::Encoded(r#"["No","Yes"]"#.into()),
        };
        let m = parse_market(raw).unwrap();
        // outcomes[0] is "No", so YES is t2 and NO is t1.
        assert_eq!(m.yes(), "t2");
        assert_eq!(m.no(), "t1");
    }

    #[test]
    fn parse_market_filters_non_binary() {
        let raw = RawMarket {
            condition_id: "0xghi".into(),
            clob_token_ids: StringOrJsonArray::Array(vec!["t1".into(), "t2".into(), "t3".into()]),
            outcomes: StringOrJsonArray::Array(vec!["A".into(), "B".into(), "C".into()]),
        };
        assert!(parse_market(raw).is_none());
    }

    #[test]
    fn parse_real_gamma_market_json() {
        let raw_json = r#"{
            "conditionId": "0xabc",
            "clobTokenIds": "[\"123\", \"456\"]",
            "outcomes": "[\"Yes\", \"No\"]",
            "extra_field_we_dont_care_about": "ignored"
        }"#;
        let raw: RawMarket = serde_json::from_str(raw_json).unwrap();
        let m = parse_market(raw).unwrap();
        assert_eq!(m.yes(), "123");
        assert_eq!(m.no(), "456");
    }
}
