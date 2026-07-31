"""Redis Streams publisher and subscriber.

Provides async-friendly wrappers around Redis Streams for inter-service
messaging. Publisher uses XADD; subscriber uses consumer groups
(XREADGROUP) for durable, resumable consumption.
"""

from __future__ import annotations

import logging
from typing import AsyncIterator

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

    async def __aenter__(self) -> RedisStreamPublisher:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

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


class RedisStreamSubscriber:
    """Consumes messages from a Redis stream via consumer groups.

    Uses XREADGROUP for durable consumption — the subscriber resumes
    from the last acknowledged message after restart.
    """

    def __init__(
        self,
        stream: str,
        group: str,
        consumer: str,
        redis_url: str = REDIS_URL,
        block_ms: int = 1000,
        start_id: str = "0",
        reset_cursor: bool = False,
    ) -> None:
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._redis_url = redis_url
        self._block_ms = block_ms
        self._start_id = start_id
        self._reset_cursor = reset_cursor
        self._redis: aioredis.Redis | None = None
        self._running = False

    async def connect(self) -> None:
        """Connect to Redis and create the consumer group if needed."""
        self._redis = aioredis.from_url(
            self._redis_url,
            decode_responses=True,
        )
        await self._redis.ping()

        try:
            await self._redis.xgroup_create(
                self._stream,
                self._group,
                id=self._start_id,
                mkstream=True,
            )
            log.info(
                "Created consumer group %s on %s",
                self._group,
                self._stream,
            )
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                log.debug("Consumer group %s already exists", self._group)
                if self._reset_cursor:
                    await self._redis.xgroup_setid(
                        self._stream,
                        self._group,
                        id=self._start_id,
                    )
                    log.debug(
                        "Reset group %s cursor to %s",
                        self._group,
                        self._start_id,
                    )
            else:
                raise

        self._running = True
        log.info(
            "RedisStreamSubscriber connected, stream=%s group=%s consumer=%s",
            self._stream,
            self._group,
            self._consumer,
        )

    async def close(self) -> None:
        """Close the Redis connection."""
        self._running = False
        if self._redis:
            await self._redis.aclose()
            self._redis = None
        log.info("RedisStreamSubscriber closed")

    async def __aenter__(self) -> RedisStreamSubscriber:
        await self.connect()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def messages(self) -> AsyncIterator[tuple[str, dict[str, str]]]:
        """Yield ``(message_id, data)`` from the stream.

        Blocks up to ``block_ms`` when caught up, then retries.
        Processes any pending (unacknowledged) messages first, then
        switches to reading new messages.
        """
        if not self._redis:
            raise RuntimeError("Not connected")

        # First pass: claim any pending (unacked) messages from previous runs
        pending_done = False
        last_pending_id = "0"

        while self._running:
            read_id = last_pending_id if not pending_done else ">"

            responses = await self._redis.xreadgroup(
                groupname=self._group,
                consumername=self._consumer,
                streams={self._stream: read_id},
                count=10,
                block=self._block_ms,
            )

            if not responses:
                if not pending_done:
                    pending_done = True
                continue

            for _stream_name, entries in responses:
                if not entries:
                    if not pending_done:
                        pending_done = True
                    continue

                for msg_id, data in entries:
                    last_pending_id = msg_id
                    yield msg_id, data

    async def ack(self, message_id: str) -> None:
        """Acknowledge a message after successful processing."""
        if not self._redis:
            raise RuntimeError("Not connected")
        await self._redis.xack(self._stream, self._group, message_id)
