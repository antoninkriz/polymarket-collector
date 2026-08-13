"""Shared Gamma API client utilities for fetching Polymarket markets."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import httpx2

from obdata.constants import GAMMA_API
from obdata.polymarket import MarketSubscription

log = logging.getLogger(__name__)

FETCH_BATCH_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 30
CONNECT_RETRIES = 3
# Gamma /markets allows 300 requests per 10 seconds. Keep ample reserve for
# other processes sharing the same public IP.
GAMMA_REQUESTS_PER_SECOND = 10
GAMMA_REQUEST_INTERVAL_SECONDS = 1 / GAMMA_REQUESTS_PER_SECOND
GAMMA_RATE_LIMIT_RETRY_SECONDS = 10.0


class GammaClient:
    """Rate-limited HTTP client for the Polymarket Gamma API."""

    def __init__(self) -> None:
        transport = httpx2.AsyncHTTPTransport(
            retries=CONNECT_RETRIES,
            http2=True,
        )
        self._client = httpx2.AsyncClient(
            timeout=REQUEST_TIMEOUT_SECONDS,
            transport=transport,
        )
        self._request_lock = asyncio.Lock()
        self._next_request_at = 0.0

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    async def get(
        self,
        path: str,
        params: Mapping[str, str | int | float | bool],
    ) -> httpx2.Response:
        """Return a successful Gamma response while respecting its rate limit."""
        while True:
            await self._wait_for_request_slot()
            response = await self._client.get(
                f"{GAMMA_API}{path}",
                params=params,
            )
            if response.status_code != 429:
                response.raise_for_status()
                return response

            retry_seconds = _retry_after_seconds(response)
            log.warning(
                "Gamma rate limit reached for %s; retrying in %.1f seconds",
                path,
                retry_seconds,
            )
            await self._defer_requests(retry_seconds)

    async def _wait_for_request_slot(self) -> None:
        async with self._request_lock:
            loop = asyncio.get_running_loop()
            delay = self._next_request_at - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_request_at = loop.time() + GAMMA_REQUEST_INTERVAL_SECONDS

    async def _defer_requests(self, seconds: float) -> None:
        async with self._request_lock:
            loop = asyncio.get_running_loop()
            self._next_request_at = max(
                self._next_request_at,
                loop.time() + seconds,
            )


def _retry_after_seconds(response: httpx2.Response) -> float:
    raw_retry_after = response.headers.get("Retry-After")
    if raw_retry_after is None:
        return GAMMA_RATE_LIMIT_RETRY_SECONDS
    try:
        return max(float(raw_retry_after), 0.0)
    except ValueError:
        return GAMMA_RATE_LIMIT_RETRY_SECONDS


@dataclass
class ActiveMarkets:
    """Active market subscriptions with startDate bounds."""

    markets: list[MarketSubscription]
    min_start_date: Optional[datetime]
    max_start_date: Optional[datetime]


def _parse_start_date(raw: str) -> Optional[datetime]:
    """Parse a startDate string from the Gamma API."""
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def parse_markets(raw_markets: list[dict]) -> ActiveMarkets:
    """Parse raw Gamma API market dicts into ActiveMarkets.

    Filters to binary markets (exactly 2 outcomes / 2 CLOB tokens) and
    tracks the min/max startDate across the input set.
    """
    subscriptions: list[MarketSubscription] = []
    min_start_date: Optional[datetime] = None
    max_start_date: Optional[datetime] = None

    for m in raw_markets:
        asset_ids = m.get("clobTokenIds", [])
        if isinstance(asset_ids, str):
            asset_ids = json.loads(asset_ids)

        outcomes = m.get("outcomes", [])
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)

        if len(asset_ids) != 2 or len(outcomes) != 2:
            continue

        if outcomes[0] == "Yes":
            yes_asset, no_asset = asset_ids[0], asset_ids[1]
        else:
            yes_asset, no_asset = asset_ids[1], asset_ids[0]

        subscriptions.append(
            MarketSubscription(
                market=m.get("conditionId", ""),
                yes_asset_id=yes_asset,
                no_asset_id=no_asset,
            ),
        )

        start_date = _parse_start_date(m.get("startDate", ""))
        if start_date is not None:
            if min_start_date is None or start_date < min_start_date:
                min_start_date = start_date
            if max_start_date is None or start_date > max_start_date:
                max_start_date = start_date

    return ActiveMarkets(
        markets=subscriptions,
        min_start_date=min_start_date,
        max_start_date=max_start_date,
    )


async def fetch_active_markets_from_gamma(client: GammaClient) -> ActiveMarkets:
    """Fetch all active binary markets from the Gamma API via keyset pagination.

    Uses the /markets/keyset endpoint, which is cursor-based and has no
    offset cap (the legacy /markets endpoint rejects offsets above ~10,000).

    Returns:
        ActiveMarkets with binary subscriptions and the min/max startDate
        observed across the response.
    """
    base_params: dict = {
        "active": "true",
        "closed": "false",
        "limit": FETCH_BATCH_SIZE,
    }
    raw_markets: list[dict] = []
    cursor: Optional[str] = None

    while True:
        params = dict(base_params)
        if cursor:
            params["after_cursor"] = cursor
        response = await client.get("/markets/keyset", params=params)
        body = response.json()

        batch = body.get("markets", []) if isinstance(body, dict) else []
        if not batch:
            break
        raw_markets.extend(batch)

        cursor = body.get("next_cursor") if isinstance(body, dict) else None
        if not cursor:
            break

    log.info("Fetched %d markets via keyset", len(raw_markets))
    return parse_markets(raw_markets)
