"""Shared Gamma API client utilities for fetching Polymarket markets."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import httpx2

from obdata.constants import GAMMA_API
from obdata.polymarket import MarketSubscription

log = logging.getLogger(__name__)

FETCH_BATCH_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3


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


async def fetch_active_markets_from_gamma() -> ActiveMarkets:
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

    transport = httpx2.AsyncHTTPTransport(retries=MAX_RETRIES, http2=True)
    async with httpx2.AsyncClient(
        timeout=REQUEST_TIMEOUT_SECONDS, transport=transport
    ) as client:
        while True:
            params = dict(base_params)
            if cursor:
                params["after_cursor"] = cursor
            resp = await client.get(f"{GAMMA_API}/markets/keyset", params=params)
            resp.raise_for_status()
            body = resp.json()

            batch = body.get("markets", []) if isinstance(body, dict) else []
            if not batch:
                break
            raw_markets.extend(batch)

            cursor = body.get("next_cursor") if isinstance(body, dict) else None
            if not cursor:
                break

    log.info("Fetched %d markets via keyset", len(raw_markets))
    return parse_markets(raw_markets)
