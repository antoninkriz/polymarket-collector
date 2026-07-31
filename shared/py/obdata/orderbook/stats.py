"""Periodic stats reporter for the orderbook streaming service."""

from __future__ import annotations

import asyncio
import bisect
import logging
import time
from collections import deque
from typing import Any

from obdata.orderbook.models import BookEvent, EventType

log = logging.getLogger(__name__)

MAX_WINDOW_SECONDS = 900  # 15 minutes


class ServiceStats:
    """Track and periodically log service throughput metrics.

    Records BOOK, PRICE_CHANGE, LAST_TRADE_PRICE, and TICK_SIZE_CHANGE
    event timestamps in a rolling 15-minute window and reports counts for
    1m / 5m / 15m intervals.
    """

    def __init__(self) -> None:
        self._price_timestamps: deque[float] = deque()
        self._book_timestamps: deque[float] = deque()
        self._trade_timestamps: deque[float] = deque()
        self._tick_timestamps: deque[float] = deque()
        self._client: Any = None
        self._task: asyncio.Task | None = None

    def record_event(self, event: BookEvent) -> None:
        """Record event timestamp by type."""
        now = time.time()
        if event.event_type == EventType.PRICE_CHANGE:
            self._price_timestamps.append(now)
        elif event.event_type == EventType.BOOK:
            self._book_timestamps.append(now)
        elif event.event_type == EventType.LAST_TRADE_PRICE:
            self._trade_timestamps.append(now)
        elif event.event_type == EventType.TICK_SIZE_CHANGE:
            self._tick_timestamps.append(now)

    @property
    def price_changes_1m(self) -> int:
        return self._count_since(self._price_timestamps, 60)

    @property
    def price_changes_5m(self) -> int:
        return self._count_since(self._price_timestamps, 300)

    @property
    def price_changes_15m(self) -> int:
        return self._count_since(self._price_timestamps, 900)

    @property
    def book_changes_1m(self) -> int:
        return self._count_since(self._book_timestamps, 60)

    @property
    def book_changes_5m(self) -> int:
        return self._count_since(self._book_timestamps, 300)

    @property
    def book_changes_15m(self) -> int:
        return self._count_since(self._book_timestamps, 900)

    @property
    def trades_1m(self) -> int:
        return self._count_since(self._trade_timestamps, 60)

    @property
    def trades_5m(self) -> int:
        return self._count_since(self._trade_timestamps, 300)

    @property
    def trades_15m(self) -> int:
        return self._count_since(self._trade_timestamps, 900)

    @property
    def tick_changes_1m(self) -> int:
        return self._count_since(self._tick_timestamps, 60)

    @property
    def tick_changes_5m(self) -> int:
        return self._count_since(self._tick_timestamps, 300)

    @property
    def tick_changes_15m(self) -> int:
        return self._count_since(self._tick_timestamps, 900)

    def start(self, client: Any, interval: int = 30) -> None:
        """Start the background reporting loop."""
        self._client = client
        self._task = asyncio.create_task(
            self._report_loop(interval),
            name="service-stats",
        )

    def stop(self) -> None:
        """Cancel the background reporting loop."""
        if self._task and not self._task.done():
            self._task.cancel()

    # -- internals -------------------------------------------------------------

    def _count_since(self, timestamps: deque[float], seconds: int) -> int:
        """Count timestamps within the last *seconds* using bisect."""
        cutoff = time.time() - seconds
        idx = bisect.bisect_left(timestamps, cutoff)
        return len(timestamps) - idx

    def _prune(self) -> None:
        """Remove entries older than 15 minutes."""
        cutoff = time.time() - MAX_WINDOW_SECONDS
        for ts in (
            self._price_timestamps,
            self._book_timestamps,
            self._trade_timestamps,
            self._tick_timestamps,
        ):
            while ts and ts[0] < cutoff:
                ts.popleft()

    async def _report_loop(self, interval: int) -> None:
        """Log stats every *interval* seconds."""
        while True:
            await asyncio.sleep(interval)
            self._prune()
            markets = (
                self._client.subscribed_market_count if self._client else 0
            )
            log.info(
                "[stats] markets=%d price_changes_1m=%d"
                " price_changes_5m=%d price_changes_15m=%d"
                " book_changes_1m=%d book_changes_5m=%d"
                " book_changes_15m=%d trades_1m=%d trades_5m=%d"
                " trades_15m=%d tick_changes_1m=%d tick_changes_5m=%d"
                " tick_changes_15m=%d",
                markets,
                self.price_changes_1m,
                self.price_changes_5m,
                self.price_changes_15m,
                self.book_changes_1m,
                self.book_changes_5m,
                self.book_changes_15m,
                self.trades_1m,
                self.trades_5m,
                self.trades_15m,
                self.tick_changes_1m,
                self.tick_changes_5m,
                self.tick_changes_15m,
            )
