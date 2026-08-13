"""Polymarket active-markets tracker with lifecycle polling.

Fetches all active binary markets from Gamma, maintains their Redis cache,
and publishes new/resolved market events to a Redis Stream.

Usage:
    python run.py
    python run.py --poll-interval 120
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import click
from dotenv import load_dotenv

from obdata.cache import CacheData, RedisMarketCache
from obdata.constants import STREAM_MARKET_EVENTS, require_redis_url
from obdata.messaging import RedisStreamPublisher
from obdata.polymarket import MarketSubscription

from obdata.gamma import GammaClient, fetch_active_markets_from_gamma, parse_markets

log = logging.getLogger(__name__)

FETCH_BATCH_SIZE = 100
DEFAULT_POLL_INTERVAL_SECONDS = 10
CLOSED_TIME_OVERLAP = timedelta(minutes=30)


# -- Gamma API ----------------------------------------------------------------


def _extract_winning_outcome(market_data: dict) -> tuple[Optional[str], Optional[str]]:
    """Determine winning outcome and asset from a resolved market.

    Returns:
        (winning_outcome, winning_asset_id) or (None, None) if undetermined.
    """
    outcome_prices = market_data.get("outcomePrices")
    outcomes = market_data.get("outcomes", [])
    token_ids = market_data.get("clobTokenIds", [])

    if isinstance(outcome_prices, str):
        try:
            outcome_prices = json.loads(outcome_prices)
        except (json.JSONDecodeError, TypeError):
            outcome_prices = None

    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except (json.JSONDecodeError, TypeError):
            outcomes = []

    if isinstance(token_ids, str):
        try:
            token_ids = json.loads(token_ids)
        except (json.JSONDecodeError, TypeError):
            token_ids = []

    if outcome_prices and outcomes:
        for i, price in enumerate(outcome_prices):
            try:
                if float(price) >= 0.99 and i < len(outcomes):
                    winning_outcome = outcomes[i]
                    winning_asset_id = token_ids[i] if i < len(token_ids) else None
                    return winning_outcome, winning_asset_id
            except (ValueError, TypeError):
                continue

    return None, None


async def _fetch_recently_closed_markets(
    gamma: GammaClient,
    active_markets: dict[str, MarketSubscription],
) -> tuple[list[dict], int, int]:
    """Fetch closed markets paged newest-first; return those in active_markets.

    Queries Gamma with closed=true ordered by closedTime descending and pages
    until a market with closedTime older than (now - CLOSED_TIME_OVERLAP) is
    seen, then returns the subset whose conditionId is in active_markets.
    """
    base_params: dict = {
        "closed": "true",
        "order": "closedTime",
        "ascending": "false",
    }

    cutoff = datetime.now(timezone.utc) - CLOSED_TIME_OVERLAP
    matched: list[dict] = []
    offset = 0
    requests_made = 0
    total_fetched = 0
    stop = False

    while not stop:
        params = {
            **base_params,
            "limit": FETCH_BATCH_SIZE,
            "offset": offset,
        }
        response = await gamma.get("/markets", params=params)
        requests_made += 1
        batch = response.json()

        if not batch:
            break

        total_fetched += len(batch)
        for market_data in batch:
            closed_time = _parse_closed_time(market_data.get("closedTime"))
            if closed_time is not None and closed_time < cutoff:
                stop = True
                break
            market = market_data.get("conditionId", "")
            if market and market in active_markets:
                matched.append(market_data)

        offset += len(batch)

    return matched, requests_made, total_fetched


async def _fetch_new_markets(
    gamma: GammaClient,
    max_start_date: datetime,
) -> tuple[list[dict], int]:
    """Fetch active markets with startDate >= max_start_date.

    Queries with:
        active=true, closed=false, start_date_min=<max_start_date>,
        order=startDate, ascending=false
    """
    base_params: dict = {
        "active": "true",
        "closed": "false",
        "start_date_min": max_start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "order": "startDate",
        "ascending": "false",
    }

    results: list[dict] = []
    offset = 0
    requests_made = 0

    while True:
        params = {
            **base_params,
            "limit": FETCH_BATCH_SIZE,
            "offset": offset,
        }
        response = await gamma.get("/markets", params=params)
        requests_made += 1
        batch = response.json()

        if not batch:
            break

        results.extend(batch)
        offset += len(batch)

    return results, requests_made


def _serialize_resolved_event(market_data: dict) -> dict[str, str]:
    """Serialize a resolved market into a Redis stream message."""
    market = market_data.get("conditionId", "")
    outcomes = market_data.get("outcomes", [])
    token_ids = market_data.get("clobTokenIds", [])

    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except (json.JSONDecodeError, TypeError):
            outcomes = []
    if isinstance(token_ids, str):
        try:
            token_ids = json.loads(token_ids)
        except (json.JSONDecodeError, TypeError):
            token_ids = []

    winning_outcome, winning_asset_id = _extract_winning_outcome(market_data)

    return {
        "event_type": "market_resolved",
        "payload": json.dumps(
            {
                "id": str(market_data.get("id", "")),
                "question": market_data.get("question", ""),
                "market": market,
                "slug": market_data.get("slug", ""),
                "assets_ids": token_ids,
                "outcomes": outcomes,
                "timestamp": str(int(time.time() * 1000)),
                "winning_asset_id": winning_asset_id,
                "winning_outcome": winning_outcome,
            }
        ),
    }


# -- Cache persistence --------------------------------------------------------


async def _save_cache(
    cache: RedisMarketCache,
    active_markets: dict[str, MarketSubscription],
) -> None:
    """Persist the current active market set to Redis."""
    await cache.save(
        CacheData(
            fetched_at=datetime.now(timezone.utc),
            markets=list(active_markets.values()),
        )
    )


# -- Background tasks ---------------------------------------------------------


def _parse_closed_time(value: object) -> Optional[datetime]:
    """Parse a Gamma `closedTime` field into a tz-aware datetime."""
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


async def _resolution_poller(
    gamma: GammaClient,
    active_markets: dict[str, MarketSubscription],
    publisher: RedisStreamPublisher,
    cache: RedisMarketCache,
    poll_interval: int,
) -> None:
    """Poll Gamma API for resolved markets, publish events, remove from active set.

    Each cycle pages closed markets newest-first until a closedTime older
    than `now - CLOSED_TIME_OVERLAP` is reached, then publishes resolution
    events for any of those markets present in the active set.
    """
    while True:
        try:
            (
                closed_markets,
                requests_made,
                total_fetched,
            ) = await _fetch_recently_closed_markets(
                gamma,
                active_markets,
            )

            resolved_count = 0
            for market_data in closed_markets:
                market = market_data.get("conditionId", "")
                if market not in active_markets:
                    continue

                event = _serialize_resolved_event(market_data)
                await publisher.publish(event)
                del active_markets[market]
                resolved_count += 1

                payload = json.loads(event["payload"])
                log.debug(
                    "market_resolved market=%s question=%s winner=%s",
                    market[:16],
                    payload.get("question", "")[:80],
                    payload.get("winning_outcome", ""),
                )

            # Save cache after each poll cycle to persist removals
            await _save_cache(cache, active_markets)

            log.info(
                "poller=resolution requests=%d total=%d resolved=%d active_markets=%d",
                requests_made,
                total_fetched,
                resolved_count,
                len(active_markets),
            )

        except Exception:
            log.error("Resolution poll failed", exc_info=True)

        await asyncio.sleep(poll_interval)


def _serialize_new_market_event(market_data: dict) -> dict[str, str]:
    """Serialize a newly created market into a Redis stream message."""
    outcomes = market_data.get("outcomes", [])
    token_ids = market_data.get("clobTokenIds", [])

    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except (json.JSONDecodeError, TypeError):
            outcomes = []
    if isinstance(token_ids, str):
        try:
            token_ids = json.loads(token_ids)
        except (json.JSONDecodeError, TypeError):
            token_ids = []

    return {
        "event_type": "new_market",
        "payload": json.dumps(
            {
                "id": str(market_data.get("id", "")),
                "question": market_data.get("question", ""),
                "market": market_data.get("conditionId", ""),
                "slug": market_data.get("slug", ""),
                "assets_ids": token_ids,
                "outcomes": outcomes,
                "timestamp": str(int(time.time() * 1000)),
            }
        ),
    }


async def _new_markets_poller(
    gamma: GammaClient,
    active_markets: dict[str, MarketSubscription],
    publisher: RedisStreamPublisher,
    cache: RedisMarketCache,
    max_start_date: datetime,
    poll_interval: int,
) -> None:
    """Poll Gamma API for newly created markets and add them to the active set.

    On startup we compute the maximum ``startDate`` across all currently active
    markets (``max_start_date``). The date range *before* that watermark is
    already covered by the initial active-markets load, so on each poll cycle
    we only need to ask Gamma for active markets whose ``startDate`` is greater
    than or equal to the current watermark — anything older has already been
    seen.

    Each cycle:
      1. Sleep for ``poll_interval`` seconds.
      2. Fetch active markets from Gamma with ``start_date_min=current_max``.
      3. Parse the response and insert any markets not already tracked into
         the shared ``active_markets`` dict (deduped by ``market``).
      4. Advance ``current_max`` to the highest ``startDate`` observed in the
         batch, so the next poll keeps moving the window forward and avoids
         re-fetching the same tail repeatedly.
      5. If anything new was added, persist the updated set to the Redis cache.

    Errors are caught and logged so a transient Gamma failure does not kill
    the poller; the loop simply retries on the next interval.

    Market lifecycle (typical chronological order of timestamps):
      createdAt                → market record created in the database
      deployingTimestamp       → contracts deployed on-chain (seconds to days after creation)
      acceptingOrdersTimestamp → CLOB orderbook begins accepting orders
      startDate                → market flagged active=true, open to the public
      endDate                  → scheduled market close / resolution deadline
      closedTime               → market actually resolved and closed

    Most markets have automaticallyActive=true, meaning the system flips them
    active once the deploy → acceptingOrders → start pipeline completes.
    """
    current_max = max_start_date

    while True:
        await asyncio.sleep(poll_interval)

        try:
            raw_markets, requests_made = await _fetch_new_markets(
                gamma,
                current_max,
            )
            parsed = parse_markets(raw_markets)
            raw_by_market = {r.get("conditionId", ""): r for r in raw_markets}

            added = 0
            for m in parsed.markets:
                if m.market in active_markets:
                    continue
                active_markets[m.market] = m
                added += 1
                raw = raw_by_market.get(m.market)
                if raw is not None:
                    await publisher.publish(_serialize_new_market_event(raw))

            if (
                parsed.max_start_date is not None
                and parsed.max_start_date > current_max
            ):
                current_max = parsed.max_start_date

            if added > 0:
                await _save_cache(cache, active_markets)

            log.info(
                "poller=new_markets requests=%d added=%d active_markets=%d",
                requests_made,
                added,
                len(active_markets),
            )

        except Exception:
            log.error("Markets poll failed", exc_info=True)


async def _log_stats(active_markets: dict[str, MarketSubscription]) -> None:
    """Periodically log the size of the active market set."""
    while True:
        await asyncio.sleep(300)
        log.info("Stats: %d active markets tracked", len(active_markets))


# -- Main runner --------------------------------------------------------------


async def _run(poll_interval: int) -> None:
    """Run the active-markets tracker."""
    redis_url = require_redis_url()

    cache = RedisMarketCache(redis_url)
    await cache.connect()

    gamma = GammaClient()

    # Fetch all active markets from Gamma API
    result = await fetch_active_markets_from_gamma(gamma)
    active_markets: dict[str, MarketSubscription] = {
        m.market: m for m in result.markets
    }
    log.info("Loaded %d active markets", len(active_markets))

    if result.min_start_date is None or result.max_start_date is None:
        log.error("No startDate found across active markets, cannot run pollers")
        await gamma.close()
        return

    log.info(
        "startDate range: min=%s, max=%s",
        result.min_start_date.isoformat(),
        result.max_start_date.isoformat(),
    )

    # Save initial snapshot to cache
    await _save_cache(cache, active_markets)

    publisher = RedisStreamPublisher(
        stream=STREAM_MARKET_EVENTS,
        redis_url=redis_url,
    )
    await publisher.connect()

    tasks: list[asyncio.Task[None]] = []
    try:
        resolution_task = asyncio.create_task(
            _resolution_poller(
                gamma,
                active_markets,
                publisher,
                cache,
                poll_interval,
            ),
            name="resolution-poller",
        )
        markets_task = asyncio.create_task(
            _new_markets_poller(
                gamma,
                active_markets,
                publisher,
                cache,
                result.max_start_date,
                poll_interval,
            ),
            name="markets-poller",
        )
        stats_task = asyncio.create_task(
            _log_stats(active_markets),
            name="stats-logger",
        )

        tasks = [resolution_task, markets_task, stats_task]
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await _save_cache(cache, active_markets)
        await cache.close()
        await publisher.close()
        await gamma.close()


# -- CLI ----------------------------------------------------------------------


@click.command()
@click.option(
    "--poll-interval",
    type=int,
    default=DEFAULT_POLL_INTERVAL_SECONDS,
    show_default=True,
    help="Seconds between Gamma API resolution polls.",
)
def main(poll_interval: int) -> None:
    """Track active Polymarket markets and publish lifecycle events."""
    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    logging.getLogger("httpx2").setLevel(logging.WARNING)
    asyncio.run(_run(poll_interval))


if __name__ == "__main__":
    main()
