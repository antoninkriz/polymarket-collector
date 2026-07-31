"""Redis-backed cache for Kalshi active market tickers.

This module is intentionally separate from ``obdata.cache``. Polymarket
subscriptions are asset-id based, while Kalshi subscriptions are ticker based.
Keeping the Redis schema and defaults here prevents accidental cross-writes
between the two deployments.

Kalshi has many more active markets than Polymarket, so this cache does not
store one serialized snapshot. Active tickers are indexed in Redis sets and
market metadata is stored one key per market. Incremental lifecycle updates
therefore mutate one ticker instead of rewriting the entire cache.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
from typing import Optional, TypeVar

import redis.asyncio as aioredis

from obdata.kalshi_models import (
    KIND_MULTIVARIATE,
    KIND_REGULAR,
    KalshiCacheData,
    KalshiMarket,
    MarketKind,
)

log = logging.getLogger(__name__)

DEFAULT_KALSHI_KEY_ACTIVE_MARKETS = "kalshi:active_markets"
DEFAULT_KALSHI_KEY_ACTIVE_MARKETS_COUNT = "kalshi:active_markets:count"
DEFAULT_KALSHI_KEY_MARKET_PREFIX = "kalshi:market"

KALSHI_KEY_ACTIVE_MARKETS = os.environ.get(
    "KALSHI_REDIS_KEY_ACTIVE_MARKETS",
    DEFAULT_KALSHI_KEY_ACTIVE_MARKETS,
)
KALSHI_KEY_MARKET_PREFIX = os.environ.get(
    "KALSHI_REDIS_KEY_MARKET_PREFIX",
    DEFAULT_KALSHI_KEY_MARKET_PREFIX,
)

_BATCH_SIZE = 1000
T = TypeVar("T")


class RedisKalshiMarketCache:
    """Load and save active Kalshi market tickers in Redis."""

    def __init__(
        self,
        redis_url: str,
        *,
        key_active_markets: str = KALSHI_KEY_ACTIVE_MARKETS,
        key_active_markets_count: str | None = None,
        key_market_prefix: str = KALSHI_KEY_MARKET_PREFIX,
    ) -> None:
        self._redis_url = redis_url
        self._key_active_markets = key_active_markets
        self._key_market_prefix = key_market_prefix
        self._redis: Optional[aioredis.Redis] = None

    async def connect(self) -> None:
        self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
        await self._redis.ping()

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    async def load(self) -> Optional[KalshiCacheData]:
        assert self._redis is not None
        regular_markets = await self._load_kind(KIND_REGULAR)
        multivariate_markets = await self._load_kind(KIND_MULTIVARIATE)
        if not regular_markets and not multivariate_markets:
            log.info("No Kalshi active markets cache in Redis")
            return None

        data = KalshiCacheData.from_markets(
            regular_markets=regular_markets,
            multivariate_markets=multivariate_markets,
        )
        log.info(
            "Kalshi cache loaded from Redis, fetched_at=%s regular=%d multivariate=%d",
            data.fetched_at.isoformat(),
            len(data.regular_markets),
            len(data.multivariate_markets),
        )
        return data

    async def replace_all(self, data: KalshiCacheData) -> None:
        assert self._redis is not None
        existing_regular = await self._load_tickers(KIND_REGULAR)
        existing_multivariate = await self._load_tickers(KIND_MULTIVARIATE)
        new_regular = {m.ticker for m in data.regular_markets}
        new_multivariate = {m.ticker for m in data.multivariate_markets}

        await self._delete_market_keys(KIND_REGULAR, existing_regular - new_regular)
        await self._delete_market_keys(
            KIND_MULTIVARIATE,
            existing_multivariate - new_multivariate,
        )
        await self._replace_kind(KIND_REGULAR, data.regular_markets)
        await self._replace_kind(KIND_MULTIVARIATE, data.multivariate_markets)
        log.info(
            "Kalshi cache snapshot saved to Redis, base_key=%s fetched_at=%s regular=%d multivariate=%d total=%d",
            self._key_active_markets,
            data.fetched_at.isoformat(),
            len(data.regular_markets),
            len(data.multivariate_markets),
            data.total_count,
        )

    async def replace_markets(
        self,
        market_kind: MarketKind,
        markets: list[KalshiMarket],
    ) -> None:
        assert self._redis is not None
        existing = await self._load_tickers(market_kind)
        new = {market.ticker for market in markets}
        await self._delete_market_keys(market_kind, existing - new)
        await self._replace_kind(market_kind, markets)
        log.info(
            "Kalshi cache kind snapshot saved to Redis, base_key=%s kind=%s count=%d",
            self._key_active_markets,
            market_kind,
            len(markets),
        )

    async def upsert_markets(
        self,
        market_kind: MarketKind,
        markets: Iterable[KalshiMarket],
    ) -> None:
        assert self._redis is not None
        batch: list[KalshiMarket] = []
        for market in markets:
            batch.append(market)
            if len(batch) >= _BATCH_SIZE:
                await self._upsert_market_batch(market_kind, batch)
                batch = []
        if batch:
            await self._upsert_market_batch(market_kind, batch)

    async def delete_markets(
        self,
        market_kind: MarketKind,
        tickers: Iterable[str],
    ) -> None:
        assert self._redis is not None
        batch: list[str] = []
        for ticker in tickers:
            batch.append(ticker)
            if len(batch) >= _BATCH_SIZE:
                await self._delete_market_batch(market_kind, batch)
                batch = []
        if batch:
            await self._delete_market_batch(market_kind, batch)

    async def refresh_count(self) -> int:
        assert self._redis is not None
        regular = await self._redis.scard(self._kind_set_key(KIND_REGULAR))
        multivariate = await self._redis.scard(self._kind_set_key(KIND_MULTIVARIATE))
        return regular + multivariate

    async def load_watermarks(self) -> dict[str, int]:
        assert self._redis is not None
        raw = await self._redis.hgetall(self._watermarks_key())
        watermarks = {
            self._watermark_field(KIND_REGULAR, "created_ts"): 0,
            self._watermark_field(KIND_REGULAR, "settled_ts"): 0,
            self._watermark_field(KIND_MULTIVARIATE, "created_ts"): 0,
            self._watermark_field(KIND_MULTIVARIATE, "settled_ts"): 0,
        }
        for key, value in raw.items():
            if key not in watermarks:
                continue
            try:
                watermarks[key] = int(value)
            except (TypeError, ValueError):
                log.warning("Skipping malformed Kalshi watermark key=%s value=%s", key, value)
        return watermarks

    async def set_watermark(
        self,
        market_kind: MarketKind,
        watermark: str,
        value: int,
    ) -> None:
        assert self._redis is not None
        await self._redis.hset(
            self._watermarks_key(),
            self._watermark_field(market_kind, watermark),
            str(value),
        )

    async def _load_kind(self, market_kind: MarketKind) -> list[KalshiMarket]:
        tickers = await self._load_tickers(market_kind)
        markets: list[KalshiMarket] = []
        for batch in _chunks(sorted(tickers), _BATCH_SIZE):
            raw_markets = await self._redis.mget([
                self._market_key(market_kind, ticker)
                for ticker in batch
            ])
            for raw in raw_markets:
                if not raw:
                    continue
                try:
                    markets.append(KalshiMarket.model_validate_json(raw))
                except (json.JSONDecodeError, ValueError) as e:
                    log.warning("Skipping malformed Kalshi market cache entry: %s", e)
        return markets

    async def _load_tickers(self, market_kind: MarketKind) -> set[str]:
        assert self._redis is not None
        key = self._kind_set_key(market_kind)
        tickers: set[str] = set()
        async for ticker in self._redis.sscan_iter(key):
            tickers.add(ticker)
        return tickers

    async def _replace_kind(
        self,
        market_kind: MarketKind,
        markets: list[KalshiMarket],
    ) -> None:
        assert self._redis is not None
        key = self._kind_set_key(market_kind)
        await self._redis.delete(key)
        for batch in _chunks(markets, _BATCH_SIZE):
            await self._upsert_market_batch(market_kind, batch)

    async def _upsert_market_batch(
        self,
        market_kind: MarketKind,
        markets: list[KalshiMarket],
    ) -> None:
        assert self._redis is not None
        if not markets:
            return
        pipe = self._redis.pipeline()
        pipe.sadd(self._kind_set_key(market_kind), *(market.ticker for market in markets))
        for market in markets:
            pipe.set(self._market_key(market_kind, market.ticker), market.model_dump_json())
        await pipe.execute()

    async def _delete_market_keys(
        self,
        market_kind: MarketKind,
        tickers: Iterable[str],
    ) -> None:
        await self.delete_markets(market_kind, tickers)

    async def _delete_market_batch(
        self,
        market_kind: MarketKind,
        tickers: list[str],
    ) -> None:
        assert self._redis is not None
        if not tickers:
            return
        pipe = self._redis.pipeline()
        pipe.srem(self._kind_set_key(market_kind), *tickers)
        pipe.delete(*(self._market_key(market_kind, ticker) for ticker in tickers))
        await pipe.execute()

    def _kind_set_key(self, market_kind: MarketKind) -> str:
        return f"{self._key_active_markets}:{market_kind}"

    def _market_key(self, market_kind: MarketKind, ticker: str) -> str:
        return f"{self._key_market_prefix}:{market_kind}:{ticker}"

    def _watermarks_key(self) -> str:
        return f"{self._key_active_markets}:watermarks"

    @staticmethod
    def _watermark_field(market_kind: MarketKind, watermark: str) -> str:
        return f"{market_kind}:{watermark}"


def _chunks(items: Iterable[T], size: int) -> Iterable[list[T]]:
    batch: list[T] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch
