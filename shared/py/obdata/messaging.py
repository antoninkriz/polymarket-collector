"""Async Redis Streams publisher."""

from __future__ import annotations

import logging

import redis.asyncio as aioredis

from obdata.constants import REDIS_URL

log = logging.getLogger(__name__)


class RedisStreamPublisher:
    """Publishes messages to a Redis stream."""

    def __init__(
        self,
        stream: str,
        redis_url: str = REDIS_URL,
        maxlen: int = 10_000,
    ) -> None:
        self._stream = stream
        self._redis_url = redis_url
        self._maxlen = maxlen
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        """Connect to Redis."""
        self._redis = aioredis.from_url(
            self._redis_url,
            decode_responses=True,
        )
        await self._redis.ping()
        log.info("RedisStreamPublisher connected, stream=%s", self._stream)

    async def close(self) -> None:
        """Close the Redis connection."""
        if self._redis:
            await self._redis.aclose()
            self._redis = None
        log.info("RedisStreamPublisher closed")

    async def publish(self, data: dict[str, str]) -> str:
        """Publish a message to the stream.

        Args:
            data: Flat dict of string key-value pairs.

        Returns:
            The Redis-assigned message ID.
        """
        if not self._redis:
            raise RuntimeError("Not connected")

        msg_id: str = await self._redis.xadd(
            self._stream,
            data,
            maxlen=self._maxlen,
            approximate=True,
        )
        log.debug("Published to %s: id=%s", self._stream, msg_id)
        return msg_id
