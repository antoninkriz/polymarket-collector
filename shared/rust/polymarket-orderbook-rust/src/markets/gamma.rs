//! Polymarket Gamma API client and active-market discovery.
//!
//! The active universe is fetched from `/markets/keyset`. Gamma limits keyset
//! pages to 100 records and returns an opaque `next_cursor`; offset pagination
//! cannot cover the complete market universe. The client spaces requests and
//! shares that pacing state across clones so future discovery and lifecycle
//! tasks can safely use one Gamma budget.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use chrono::{DateTime, Utc};
use reqwest::{header, StatusCode};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::warn;

use crate::events::Market;
use crate::record::now_ns;

pub const GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";
const FETCH_BATCH_SIZE: usize = 100;
const REQUEST_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
const MAX_ATTEMPTS: usize = 6;

/// One parsed Gamma market with fields needed by discovery and lifecycle
/// reconciliation. `received_at_ns` is sampled after the complete HTTP body is
/// available and before JSON decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GammaMarket {
    pub id: String,
    pub condition_id: String,
    pub question: String,
    pub slug: String,
    pub active: bool,
    pub closed: bool,
    pub uma_resolution_status: String,
    pub assets_ids: Vec<String>,
    pub outcomes: Vec<String>,
    pub outcome_prices: Vec<Decimal>,
    pub created_at_ms: Option<i64>,
    pub start_date_ms: Option<i64>,
    pub closed_time_ms: Option<i64>,
    pub received_at_ns: i64,
}

impl GammaMarket {
    /// Convert an active, open binary Gamma record to a pool subscription.
    ///
    /// Asset order deliberately follows Gamma's outcome/token array order. It
    /// has no YES/NO meaning for arbitrary binary sports and scalar markets;
    /// the pool only requires the pair to remain on one connection.
    pub fn active_subscription(&self) -> Option<Market> {
        if !self.active
            || self.closed
            || self.condition_id.is_empty()
            || self.assets_ids.len() != 2
            || self.outcomes.len() != 2
            || self.assets_ids.iter().any(String::is_empty)
            || self.assets_ids[0] == self.assets_ids[1]
        {
            return None;
        }
        Some(Market::new(
            self.condition_id.clone(),
            self.assets_ids[0].clone(),
            self.assets_ids[1].clone(),
        ))
    }

    /// Return an unambiguous resolved winner when exactly one aligned outcome
    /// has a settlement price of one.
    pub fn winner(&self) -> Option<GammaWinner<'_>> {
        if !self.is_resolved()
            || self.outcomes.len() != self.assets_ids.len()
            || self.outcome_prices.len() != self.assets_ids.len()
        {
            return None;
        }

        let mut winners = self
            .outcome_prices
            .iter()
            .enumerate()
            .filter(|(_, price)| **price == Decimal::ONE);
        let (index, _) = winners.next()?;
        if winners.next().is_some() {
            return None;
        }
        Some(GammaWinner {
            asset_id: &self.assets_ids[index],
            outcome: &self.outcomes[index],
        })
    }

    pub fn is_resolved(&self) -> bool {
        self.closed && self.uma_resolution_status.eq_ignore_ascii_case("resolved")
    }

    pub fn new_market_timestamp_ms(&self) -> Option<i64> {
        self.created_at_ms.or(self.start_date_ms)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GammaWinner<'a> {
    pub asset_id: &'a str,
    pub outcome: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeysetScanKind {
    FullActive,
    ActiveSince(i64),
    ClosedSince(i64),
}

pub struct KeysetScan {
    kind: KeysetScanKind,
    cursor: Option<String>,
    seen_cursors: HashSet<String>,
    finished: bool,
}

impl KeysetScan {
    fn new(kind: KeysetScanKind) -> Self {
        Self {
            kind,
            cursor: None,
            seen_cursors: HashSet::new(),
            finished: false,
        }
    }
}

/// Rate-limited Gamma client. Clones share one request gate.
#[derive(Clone)]
pub struct GammaClient {
    http: reqwest::Client,
    base_url: Arc<str>,
    next_request_at: Arc<Mutex<Option<Instant>>>,
}

impl GammaClient {
    pub fn new(http: reqwest::Client) -> Self {
        Self::with_base_url(http, GAMMA_BASE_URL)
    }

    fn with_base_url(http: reqwest::Client, base_url: impl Into<Arc<str>>) -> Self {
        Self {
            http,
            base_url: base_url.into(),
            next_request_at: Arc::new(Mutex::new(None)),
        }
    }

    pub fn full_active_scan(&self) -> KeysetScan {
        KeysetScan::new(KeysetScanKind::FullActive)
    }

    pub fn active_since_scan(&self, since_ms: i64) -> KeysetScan {
        KeysetScan::new(KeysetScanKind::ActiveSince(since_ms))
    }

    pub fn closed_since_scan(&self, since_ms: i64) -> KeysetScan {
        KeysetScan::new(KeysetScanKind::ClosedSince(since_ms))
    }

    /// Fetch one filtered keyset page. Returning `None` means the scan is
    /// complete. Incremental scans stop after the first page whose newest
    /// relevant timestamp predates their overlap boundary.
    pub async fn next_keyset_page(
        &self,
        scan: &mut KeysetScan,
    ) -> Result<Option<Vec<GammaMarket>>> {
        if scan.finished {
            return Ok(None);
        }
        let params = scan_params(scan.kind, scan.cursor.as_deref());
        let received: Received<KeysetPage> = self
            .get_json("/markets/keyset", &params)
            .await
            .context("fetch Gamma market keyset page")?;
        let mut newest_relevant_timestamp = None::<i64>;
        let mut markets = Vec::new();
        for raw in received.value.markets {
            let market = match GammaMarket::from_raw(raw, received.received_at_ns) {
                Ok(market) => market,
                Err(error) => {
                    warn!(%error, "ignoring malformed Gamma market");
                    continue;
                }
            };
            let include = match scan.kind {
                KeysetScanKind::FullActive => market.active_subscription().is_some(),
                KeysetScanKind::ActiveSince(since_ms) => {
                    let timestamp = market.start_date_ms;
                    newest_relevant_timestamp = newest_relevant_timestamp.max(timestamp);
                    timestamp.is_some_and(|timestamp| timestamp >= since_ms)
                        && market.active_subscription().is_some()
                }
                KeysetScanKind::ClosedSince(since_ms) => {
                    newest_relevant_timestamp =
                        newest_relevant_timestamp.max(market.closed_time_ms);
                    market.is_resolved()
                        && market
                            .closed_time_ms
                            .is_some_and(|timestamp| timestamp >= since_ms)
                }
            };
            if include {
                markets.push(market);
            }
        }

        scan.finished = incremental_boundary_reached(scan.kind, newest_relevant_timestamp);
        if !scan.finished {
            scan.cursor = next_cursor(received.value.next_cursor, &mut scan.seen_cursors)?;
            scan.finished = scan.cursor.is_none();
        }
        Ok(Some(markets))
    }

    async fn get_json<T>(&self, path: &str, params: &[(&str, String)]) -> Result<Received<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = format!("{}{path}", self.base_url);
        for attempt in 0..MAX_ATTEMPTS {
            self.wait_for_request_slot().await;
            let response = match self.http.get(&url).query(params).send().await {
                Ok(response) => response,
                Err(error) if attempt + 1 < MAX_ATTEMPTS => {
                    let delay = retry_delay(attempt, None);
                    warn!(
                        path,
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis() as u64,
                        %error,
                        "Gamma request failed; retrying"
                    );
                    self.defer_requests(delay).await;
                    continue;
                }
                Err(error) => return Err(error).context("Gamma request failed"),
            };

            let status = response.status();
            if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                if attempt + 1 == MAX_ATTEMPTS {
                    anyhow::bail!("Gamma {path} returned {status} after {MAX_ATTEMPTS} attempts");
                }
                let retry_after = retry_after(response.headers());
                let delay = retry_delay(attempt, retry_after);
                warn!(
                    path,
                    %status,
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis() as u64,
                    "Gamma request throttled or unavailable; retrying"
                );
                self.defer_requests(delay).await;
                continue;
            }
            ensure!(status.is_success(), "Gamma {path} returned {status}");

            let body = match response.bytes().await {
                Ok(body) => body,
                Err(error) if attempt + 1 < MAX_ATTEMPTS => {
                    let delay = retry_delay(attempt, None);
                    warn!(
                        path,
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis() as u64,
                        %error,
                        "reading Gamma response failed; retrying"
                    );
                    self.defer_requests(delay).await;
                    continue;
                }
                Err(error) => return Err(error).context("read Gamma response body"),
            };
            let received_at_ns = now_ns();
            let value = serde_json::from_slice(&body).context("decode Gamma response JSON")?;
            return Ok(Received {
                value,
                received_at_ns,
            });
        }
        unreachable!("the retry loop returns on its final attempt")
    }

    async fn wait_for_request_slot(&self) {
        let mut next_request_at = self.next_request_at.lock().await;
        let now = Instant::now();
        if let Some(next) = *next_request_at {
            if next > now {
                tokio::time::sleep_until(next).await;
            }
        }
        *next_request_at = Some(Instant::now() + REQUEST_INTERVAL);
    }

    async fn defer_requests(&self, delay: Duration) {
        let mut next_request_at = self.next_request_at.lock().await;
        let deferred_until = Instant::now() + delay;
        *next_request_at = Some(
            next_request_at
                .map(|current| current.max(deferred_until))
                .unwrap_or(deferred_until),
        );
    }
}

struct Received<T> {
    value: T,
    received_at_ns: i64,
}

fn scan_params(kind: KeysetScanKind, cursor: Option<&str>) -> Vec<(&'static str, String)> {
    let mut params = vec![("limit", FETCH_BATCH_SIZE.to_string())];
    match kind {
        KeysetScanKind::FullActive => {
            params.push(("active", "true".to_string()));
            params.push(("closed", "false".to_string()));
        }
        KeysetScanKind::ActiveSince(_) => {
            // start_date_min is the only documented keyset lower bound for
            // discovery. This poll is deliberately best-effort: late/backfilled
            // markets with an old startDate are recovered by the full scan.
            params.push(("active", "true".to_string()));
            params.push(("closed", "false".to_string()));
            if let KeysetScanKind::ActiveSince(since_ms) = kind {
                if let Some(since) = DateTime::<Utc>::from_timestamp_millis(since_ms) {
                    params.push(("start_date_min", since.to_rfc3339()));
                }
            }
            params.push(("order", "startDate".to_string()));
            params.push(("ascending", "false".to_string()));
        }
        KeysetScanKind::ClosedSince(_) => {
            params.push(("closed", "true".to_string()));
            params.push(("uma_resolution_status", "resolved".to_string()));
            params.push(("order", "closedTime".to_string()));
            params.push(("ascending", "false".to_string()));
        }
    }
    if let Some(cursor) = cursor {
        params.push(("after_cursor", cursor.to_string()));
    }
    params
}

fn incremental_boundary_reached(
    kind: KeysetScanKind,
    newest_relevant_timestamp: Option<i64>,
) -> bool {
    match kind {
        KeysetScanKind::FullActive => false,
        KeysetScanKind::ActiveSince(since_ms) | KeysetScanKind::ClosedSince(since_ms) => {
            newest_relevant_timestamp.is_some_and(|timestamp| timestamp < since_ms)
        }
    }
}

fn next_cursor(
    cursor: Option<String>,
    seen_cursors: &mut HashSet<String>,
) -> Result<Option<String>> {
    let Some(cursor) = cursor.filter(|cursor| !cursor.is_empty()) else {
        return Ok(None);
    };
    ensure!(
        seen_cursors.insert(cursor.clone()),
        "Gamma repeated keyset cursor {cursor:?}"
    );
    Ok(Some(cursor))
}

fn retry_delay(attempt: usize, retry_after: Option<Duration>) -> Duration {
    if let Some(retry_after) = retry_after {
        return retry_after.max(REQUEST_INTERVAL);
    }
    let multiplier = 1_u32 << attempt.min(5);
    let exponential = DEFAULT_RETRY_DELAY
        .saturating_mul(multiplier)
        .min(MAX_RETRY_DELAY);
    let jitter_ms = ((attempt as u64 + 1) * 137) % 251;
    exponential
        .saturating_add(Duration::from_millis(jitter_ms))
        .min(MAX_RETRY_DELAY)
}

fn retry_after(headers: &header::HeaderMap) -> Option<Duration> {
    let raw = headers.get(header::RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(seconds) = raw.parse::<f64>() {
        if seconds.is_finite() && seconds >= 0.0 {
            return Some(Duration::from_secs_f64(seconds));
        }
        return None;
    }

    let retry_at = DateTime::parse_from_rfc2822(raw).ok()?.with_timezone(&Utc);
    let delay_ms = retry_at.timestamp_millis() - Utc::now().timestamp_millis();
    Some(Duration::from_millis(delay_ms.max(0) as u64))
}

#[derive(Debug, Deserialize)]
struct KeysetPage {
    #[serde(default)]
    markets: Vec<RawMarket>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMarket {
    #[serde(default)]
    id: String,
    #[serde(default)]
    condition_id: String,
    #[serde(default)]
    question: String,
    #[serde(default)]
    slug: String,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
    #[serde(default)]
    uma_resolution_status: String,
    #[serde(default)]
    clob_token_ids: StringOrJsonArray,
    #[serde(default)]
    outcomes: StringOrJsonArray,
    #[serde(default)]
    outcome_prices: StringOrJsonArray,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    start_date: Option<String>,
    #[serde(default)]
    closed_time: Option<String>,
}

impl GammaMarket {
    fn from_raw(raw: RawMarket, received_at_ns: i64) -> Result<Self> {
        let assets_ids = raw
            .clob_token_ids
            .into_strings()
            .context("decode clobTokenIds")?;
        let outcomes = raw.outcomes.into_strings().context("decode outcomes")?;
        let outcome_prices = raw
            .outcome_prices
            .into_strings()
            .context("decode outcomePrices")?
            .into_iter()
            .map(|price| {
                price
                    .parse::<Decimal>()
                    .with_context(|| format!("parse outcome price {price:?}"))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            id: raw.id,
            condition_id: raw.condition_id,
            question: raw.question,
            slug: raw.slug,
            active: raw.active,
            closed: raw.closed,
            uma_resolution_status: raw.uma_resolution_status,
            assets_ids,
            outcomes,
            outcome_prices,
            created_at_ms: raw.created_at.as_deref().and_then(parse_gamma_timestamp_ms),
            start_date_ms: raw.start_date.as_deref().and_then(parse_gamma_timestamp_ms),
            closed_time_ms: raw
                .closed_time
                .as_deref()
                .and_then(parse_gamma_timestamp_ms),
            received_at_ns,
        })
    }
}

fn parse_gamma_timestamp_ms(value: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(value)
        .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f%#z"))
        .ok()
        .map(|timestamp| timestamp.timestamp_millis())
}

/// Gamma can return these fields as arrays or as strings containing JSON
/// arrays. Empty/missing values are normalized to an empty vector.
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
            Self::Array(values) => Ok(values),
            Self::Encoded(value) if value.is_empty() => Ok(Vec::new()),
            Self::Encoded(value) => {
                serde_json::from_str(&value).context("decode JSON-encoded string array")
            }
            Self::Empty => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    fn parse_market(json: &str, received_at_ns: i64) -> GammaMarket {
        let raw: RawMarket = serde_json::from_str(json).unwrap();
        GammaMarket::from_raw(raw, received_at_ns).unwrap()
    }

    #[test]
    fn parses_encoded_arrays_and_preserves_arbitrary_outcome_order() {
        let market = parse_market(
            r#"{
                "id": "7",
                "conditionId": "0xabc",
                "question": "Player A or Player B?",
                "slug": "a-or-b",
                "active": true,
                "closed": false,
                "clobTokenIds": "[\"token-a\",\"token-b\"]",
                "outcomes": "[\"Player A\",\"Player B\"]",
                "outcomePrices": "[\"0.4\",\"0.6\"]",
                "createdAt": "2026-08-14T12:01:02.345Z",
                "startDate": "2026-08-14T12:02:00Z"
            }"#,
            123,
        );

        let subscription = market.active_subscription().unwrap();
        assert_eq!(subscription.hash, "0xabc");
        assert_eq!(subscription.assets, ["token-a", "token-b"]);
        assert_eq!(market.outcomes, ["Player A", "Player B"]);
        assert_eq!(
            market.outcome_prices,
            [Decimal::new(4, 1), Decimal::new(6, 1)]
        );
        assert_eq!(market.received_at_ns, 123);
        assert_eq!(market.new_market_timestamp_ms(), Some(1_786_708_862_345));
    }

    #[test]
    fn parses_native_arrays() {
        let market = parse_market(
            r#"{
                "conditionId": "0xdef",
                "active": true,
                "closed": false,
                "clobTokenIds": ["one", "two"],
                "outcomes": ["Yes", "No"],
                "outcomePrices": ["1", "0"]
            }"#,
            1,
        );
        assert_eq!(market.assets_ids, ["one", "two"]);
        assert_eq!(market.outcomes, ["Yes", "No"]);
    }

    #[test]
    fn active_subscription_validates_open_binary_market() {
        let mut market = parse_market(
            r#"{
                "conditionId": "m",
                "active": true,
                "closed": false,
                "clobTokenIds": ["a", "b"],
                "outcomes": ["A", "B"]
            }"#,
            1,
        );
        assert!(market.active_subscription().is_some());

        market.closed = true;
        assert!(market.active_subscription().is_none());
        market.closed = false;
        market.active = false;
        assert!(market.active_subscription().is_none());
        market.active = true;
        market.assets_ids = vec!["same".into(), "same".into()];
        assert!(market.active_subscription().is_none());
        market.assets_ids = vec!["only-one".into()];
        assert!(market.active_subscription().is_none());
    }

    #[test]
    fn winner_requires_one_aligned_settlement_price() {
        let mut market = parse_market(
            r#"{
                "conditionId": "m",
                "active": true,
                "closed": true,
                "umaResolutionStatus": "resolved",
                "clobTokenIds": ["a", "b"],
                "outcomes": ["A", "B"],
                "outcomePrices": ["0", "1"],
                "closedTime": "2026-08-14 13:05:12+00"
            }"#,
            1,
        );
        assert_eq!(
            market.winner(),
            Some(GammaWinner {
                asset_id: "b",
                outcome: "B"
            })
        );
        assert_eq!(market.closed_time_ms, Some(1_786_712_712_000));

        market.outcome_prices = vec![Decimal::ONE, Decimal::ONE];
        assert!(market.winner().is_none());
        market.outcome_prices = vec![Decimal::new(99, 2), Decimal::new(1, 2)];
        assert!(market.winner().is_none());
        market.outcome_prices = vec![Decimal::ONE];
        assert!(market.winner().is_none());
    }

    #[test]
    fn timestamp_parser_accepts_gamma_formats() {
        assert_eq!(
            parse_gamma_timestamp_ms("2026-08-14T13:05:12.123456Z"),
            Some(1_786_712_712_123)
        );
        assert_eq!(
            parse_gamma_timestamp_ms("2026-08-14 13:05:12+00"),
            Some(1_786_712_712_000)
        );
        assert_eq!(parse_gamma_timestamp_ms("not-a-time"), None);
    }

    #[test]
    fn keyset_parameters_use_documented_page_size_and_cursor() {
        assert_eq!(
            scan_params(KeysetScanKind::FullActive, None),
            vec![
                ("limit", "100".into()),
                ("active", "true".into()),
                ("closed", "false".into()),
            ]
        );
        assert_eq!(
            scan_params(KeysetScanKind::FullActive, Some("opaque")),
            vec![
                ("limit", "100".into()),
                ("active", "true".into()),
                ("closed", "false".into()),
                ("after_cursor", "opaque".into()),
            ]
        );
    }

    #[test]
    fn incremental_queries_and_page_boundaries_overlap() {
        let since = 1_786_708_862_345;
        assert_eq!(
            scan_params(KeysetScanKind::ActiveSince(since), None),
            vec![
                ("limit", "100".into()),
                ("active", "true".into()),
                ("closed", "false".into()),
                ("start_date_min", "2026-08-14T12:01:02.345+00:00".into()),
                ("order", "startDate".into()),
                ("ascending", "false".into()),
            ]
        );
        assert_eq!(
            scan_params(KeysetScanKind::ClosedSince(since), None),
            vec![
                ("limit", "100".into()),
                ("closed", "true".into()),
                ("uma_resolution_status", "resolved".into()),
                ("order", "closedTime".into()),
                ("ascending", "false".into()),
            ]
        );
        assert!(!incremental_boundary_reached(
            KeysetScanKind::ClosedSince(since),
            Some(since)
        ));
        assert!(incremental_boundary_reached(
            KeysetScanKind::ClosedSince(since),
            Some(since - 1)
        ));
        assert!(!incremental_boundary_reached(
            KeysetScanKind::ActiveSince(since),
            None
        ));
    }

    #[test]
    fn closed_pending_market_is_not_resolved() {
        let market = parse_market(
            r#"{
                "conditionId":"m", "active":false, "closed":true,
                "umaResolutionStatus":"pending",
                "clobTokenIds":["a","b"], "outcomes":["Yes","No"],
                "outcomePrices":["1","0"], "closedTime":"2026-08-14T13:00:00Z"
            }"#,
            1,
        );
        assert!(!market.is_resolved());
        assert!(market.winner().is_none());
    }

    #[test]
    fn keyset_page_parses_wrapper_and_ignores_schema_field() {
        let page: KeysetPage = serde_json::from_str(
            r#"{
                "$schema": "ignored",
                "markets": [{"conditionId":"m","active":true,"closed":false}],
                "next_cursor": "next"
            }"#,
        )
        .unwrap();
        assert_eq!(page.markets.len(), 1);
        assert_eq!(page.next_cursor.as_deref(), Some("next"));
    }

    #[test]
    fn repeated_keyset_cursor_is_rejected() {
        let mut seen = HashSet::new();
        assert_eq!(
            next_cursor(Some("cursor".into()), &mut seen).unwrap(),
            Some("cursor".into())
        );
        let error = next_cursor(Some("cursor".into()), &mut seen)
            .unwrap_err()
            .to_string();
        assert!(error.contains("repeated keyset cursor"), "{error}");
        assert_eq!(next_cursor(Some(String::new()), &mut seen).unwrap(), None);
        assert_eq!(next_cursor(None, &mut seen).unwrap(), None);
    }

    #[test]
    fn retry_after_accepts_seconds_and_http_date() {
        let mut headers = header::HeaderMap::new();
        headers.insert(header::RETRY_AFTER, HeaderValue::from_static("2.5"));
        assert_eq!(retry_after(&headers), Some(Duration::from_millis(2_500)));

        let future = Utc::now() + chrono::TimeDelta::seconds(10);
        headers.insert(
            header::RETRY_AFTER,
            HeaderValue::from_str(&future.to_rfc2822()).unwrap(),
        );
        let parsed = retry_after(&headers).unwrap();
        assert!(parsed >= Duration::from_secs(8));
        assert!(parsed <= Duration::from_secs(10));

        headers.insert(header::RETRY_AFTER, HeaderValue::from_static("invalid"));
        assert_eq!(retry_after(&headers), None);
    }

    #[test]
    fn exponential_retry_delay_is_bounded() {
        assert!(retry_delay(1, None) > retry_delay(0, None));
        assert_eq!(retry_delay(100, None), MAX_RETRY_DELAY);
        assert_eq!(
            retry_delay(0, Some(Duration::from_millis(1))),
            REQUEST_INTERVAL
        );
    }
}
