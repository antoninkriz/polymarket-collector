"""Redis-backed cache for active market subscriptions."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import redis.asyncio as aioredis

from obdata.constants import (
    KEY_ACTIVE_MARKETS,
    KEY_ACTIVE_MARKETS_COUNT,
    KEY_ACTIVE_MARKETS_UPDATED_AT,
)
from obdata.polymarket import MarketSubscription

log = logging.getLogger(__name__)


@dataclass
class CacheData:
    """Timestamped snapshot of active market subscriptions."""

    fetched_at: datetime
    markets: list[MarketSubscription]


class RedisMarketCache:
    """Load and save active market subscriptions in Redis."""

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
        await self._redis.ping()

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()

    async def save(self, data: CacheData) -> None:
        """Serialize CacheData to JSON and write to Redis."""
        assert self._redis is not None

        payload: dict[str, Any] = {
            "fetched_at": data.fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "markets": [
                {
                    "market": m.market,
                    "yes_asset_id": m.yes_asset_id,
                    "no_asset_id": m.no_asset_id,
                }
                for m in data.markets
            ],
        }

        pipe = self._redis.pipeline()
        pipe.set(KEY_ACTIVE_MARKETS, json.dumps(payload))
        pipe.set(KEY_ACTIVE_MARKETS_COUNT, str(len(data.markets)))
        pipe.set(KEY_ACTIVE_MARKETS_UPDATED_AT, str(int(data.fetched_at.timestamp())))
        await pipe.execute()

        log.debug(
            "Cache saved to Redis, fetched_at=%s market_count=%d",
            data.fetched_at.isoformat(),
            len(data.markets),
        )
