"""Shared market event handling for orderbook services.

Provides:
- load_markets_from_redis()  : load MarketSubscription list from Redis
- handle_new_market()        : parse + subscribe a new binary market
- handle_market_resolved()   : unsubscribe a resolved market
- stream_listener()          : Redis stream consumer that dispatches market events
"""

from __future__ import annotations

import json
import logging
from typing import Any

import redis.asyncio as aioredis

from obdata.constants import KEY_ACTIVE_MARKETS, STREAM_MARKET_EVENTS
from obdata.messaging import RedisStreamSubscriber
from obdata.orderbook.client_protocol import SubscribableClient
from obdata.orderbook.models import MarketSubscription

log = logging.getLogger(__name__)


async def load_markets_from_redis(redis_url: str) -> list[MarketSubscription]:
    """Load market subscriptions from the Redis active markets cache.

    Args:
        redis_url: Redis connection URL.

    Returns:
        List of MarketSubscription parsed from the cache, or empty list if
        the cache key does not exist.
    """
    client = aioredis.from_url(redis_url, decode_responses=True)
    try:
        raw = await client.get(KEY_ACTIVE_MARKETS)
        if not raw:
            log.warning("No active markets cache in Redis")
            return []

        data: dict[str, Any] = json.loads(raw)
        markets = [
            MarketSubscription(
                market=m["market"],
                yes_asset_id=m["yes_asset_id"],
                no_asset_id=m["no_asset_id"],
            )
            for m in data["markets"]
        ]

        log.info("Loaded %d markets from Redis", len(markets))
        return markets
    finally:
        await client.aclose()


async def handle_new_market(
    client: SubscribableClient,
    payload: dict,
) -> None:
    """Subscribe to a newly created market's orderbook."""
    assets_ids = payload.get("assets_ids", [])
    outcomes = payload.get("outcomes", [])
    market = payload.get("market", "")

    if len(assets_ids) != 2 or len(outcomes) != 2:
        log.debug("Skipping non-binary market %s", market[:16])
        return

    if outcomes[0] == "Yes":
        yes_asset, no_asset = assets_ids[0], assets_ids[1]
    else:
        yes_asset, no_asset = assets_ids[1], assets_ids[0]

    sub = MarketSubscription(
        market=market,
        yes_asset_id=yes_asset,
        no_asset_id=no_asset,
    )
    await client.subscribe_markets([sub])
    log.info(
        "[MARKET_EVENT] new_market market=%s question=%s slug=%s",
        market[:16],
        payload.get("question", "")[:80],
        payload.get("slug", ""),
    )


async def handle_market_resolved(
    client: SubscribableClient,
    payload: dict,
) -> None:
    """Unsubscribe from a resolved market's orderbook."""
    market = payload.get("market", "")
    if not market:
        return

    await client.unsubscribe_markets([market])
    log.info(
        "[MARKET_EVENT] market_resolved market=%s question=%s winner=%s",
        market[:16],
        payload.get("question", "")[:80],
        payload.get("winning_outcome", ""),
    )


async def stream_listener(
    client: SubscribableClient,
    redis_url: str,
    *,
    group: str,
    consumer: str,
    skip_backlog: bool = False,
) -> None:
    """Listen for market events from Redis stream and update subscriptions.

    Args:
        client: Orderbook client to subscribe/unsubscribe markets on.
        redis_url: Redis connection URL.
        group: Redis consumer group name.
        consumer: Redis consumer name.
        skip_backlog: If True, skip unprocessed messages and start from current.
    """
    subscriber = RedisStreamSubscriber(
        stream=STREAM_MARKET_EVENTS,
        group=group,
        consumer=consumer,
        redis_url=redis_url,
        start_id="$" if skip_backlog else "0",
        reset_cursor=skip_backlog,
    )

    async with subscriber:
        async for msg_id, data in subscriber.messages():
            try:
                event_type = data.get("event_type", "")
                payload = json.loads(data.get("payload", "{}"))

                if event_type == "new_market":
                    await handle_new_market(client, payload)
                elif event_type == "market_resolved":
                    await handle_market_resolved(client, payload)

                await subscriber.ack(msg_id)
            except Exception:
                log.error(
                    "Error processing stream message %s",
                    msg_id,
                    exc_info=True,
                )
