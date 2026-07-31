"""Base orderbook client with connection pool and subscription management.

Provides the shared infrastructure for orderbook clients: connection pool,
fill-first market distribution, subscribe/unsubscribe, event queue, and
lifecycle management. Subclasses implement ``_on_book_update`` and
``_on_price_change`` to define how events are processed.
"""

from __future__ import annotations

import asyncio
import logging
import os
from abc import ABC, abstractmethod
from typing import AsyncIterator, Optional

from obdata.orderbook.connection import (
    MAX_ASSETS_PER_CONNECTION,
    WebSocketConnection,
)
from obdata.orderbook.models import (
    BookEvent,
    LastTradePrice,
    MarketSubscription,
    OrderBookSnapshot,
    PriceChange,
    TickSizeChange,
)

log = logging.getLogger(__name__)

_SENTINEL = object()


class BaseOrderBookClient(ABC):
    """Base class for Polymarket orderbook clients.

    Manages a pool of WebSocket connections, distributes market
    subscriptions fill-first, and exposes an async event queue.

    Subclasses must implement:
    - ``_on_book_update(snapshot)``        — handle a full book snapshot
    - ``_on_price_change(change)``         — handle a price change
    - ``_on_last_trade_price(trade)``      — handle a trade execution
    - ``_on_tick_size_change(change)``     — handle a tick size change
    """

    # During startup, subscribe_markets holds the event loop while creating
    # connections sequentially.  Already-connected sockets receive book
    # snapshots immediately, so the queue must be large enough to absorb the
    # full initial burst (up to 2 events per token across all connections).
    _STATS_INTERVAL_SECONDS = 60

    def __init__(self, queue_maxsize: int = 5_000_000) -> None:
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)
        self._running = False

        # Connection pool keyed by monotonic ID (created on demand)
        self._connections: dict[int, WebSocketConnection] = {}
        self._next_conn_id: int = 0

        # Market registry
        self._markets: dict[str, MarketSubscription] = {}

        self._stats_task: Optional[asyncio.Task] = None

    # -- Lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        """Mark the client as running.

        Connections are created lazily by subscribe_markets().
        """
        self._running = True
        self._stats_task = asyncio.create_task(self._stats_loop())
        log.info("%s started", type(self).__name__)

    async def close(self) -> None:
        """Shut down all connections."""
        log.info("%s shutting down", type(self).__name__)
        self._running = False

        if self._stats_task and not self._stats_task.done():
            self._stats_task.cancel()
            try:
                await self._stats_task
            except asyncio.CancelledError:
                pass
            self._stats_task = None

        try:
            self._queue.put_nowait(_SENTINEL)
        except asyncio.QueueFull:
            pass

        for conn in self._connections.values():
            await conn.close()

        log.info("%s shut down", type(self).__name__)

    async def __aenter__(self) -> BaseOrderBookClient:
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # -- Subscription management ---------------------------------------------

    async def subscribe_markets(self, markets: list[MarketSubscription]) -> None:
        """Subscribe to order book updates for the given markets.

        Phase 1 (pure computation): allocate markets to existing connections
        (fill-first) or new connection slots (up to 500 tokens each).

        Phase 2 (parallel I/O): create connections in batches of 50,
        subscribing each immediately after connect.
        """
        existing_pending: dict[int, list[str]] = {}
        new_slots: list[list[str]] = []
        current_slot: list[str] = []
        total_new = 0

        for sub in markets:
            if sub.market in self._markets:
                continue

            assets = [sub.yes_asset_id, sub.no_asset_id]

            # Fill-first: try existing connections with capacity
            conn_id = self._find_connection_with_capacity(existing_pending)
            if conn_id is not None:
                existing_pending.setdefault(conn_id, []).extend(assets)
            else:
                # Allocate to a new connection slot
                if len(current_slot) + 2 > MAX_ASSETS_PER_CONNECTION:
                    new_slots.append(current_slot)
                    current_slot = []
                current_slot.extend(assets)

            total_new += 1

            self._markets[sub.market] = sub

        if current_slot:
            new_slots.append(current_slot)

        if total_new:
            log.info(
                "Subscribing to %d new markets (%d new connections)",
                total_new,
                len(new_slots),
            )

        # Subscribe assets to existing connections
        for conn_id, assets in existing_pending.items():
            await self._connections[conn_id].subscribe(assets)

        # Create connections in parallel batches of 50
        _STARTUP_BATCH_SIZE = 50
        for batch_start in range(0, len(new_slots), _STARTUP_BATCH_SIZE):
            batch = new_slots[batch_start : batch_start + _STARTUP_BATCH_SIZE]

            async def _connect_and_subscribe(slot_assets: list[str]) -> None:
                cid = await self._add_connection()
                await self._connections[cid].subscribe(slot_assets)

            await asyncio.gather(*[
                _connect_and_subscribe(slot_assets) for slot_assets in batch
            ])
            done = min(batch_start + len(batch), len(new_slots))
            log.info("Connection progress: %d/%d", done, len(new_slots))

        if total_new:
            log.info(
                "Subscribed to %d new markets (total=%d, connections=%d)",
                total_new,
                len(self._markets),
                len(self._connections),
            )

    async def unsubscribe_markets(self, markets: list[str]) -> None:
        """Unsubscribe from the given markets (by market hash).

        Delegates to ``conn.unsubscribe()`` which handles force-close and
        reconnect internally. Empty connections are garbage-collected.
        """
        affected_conns: dict[int, list[str]] = {}
        removed = 0

        for market in markets:
            sub = self._markets.pop(market, None)
            if sub is None:
                continue

            removed += 1
            assets = [sub.yes_asset_id, sub.no_asset_id]

            # Find which connection holds these assets
            for conn_id, conn in self._connections.items():
                if conn.has_asset(sub.yes_asset_id):
                    affected_conns.setdefault(conn_id, []).extend(assets)
                    break

            self._on_market_unsubscribed(sub)

        for conn_id, assets in affected_conns.items():
            await self._connections[conn_id].unsubscribe(assets)

        # Garbage-collect empty connections
        for conn_id in list(affected_conns):
            conn = self._connections.get(conn_id)
            if conn and conn.desired_count == 0:
                await conn.close()
                del self._connections[conn_id]
                log.info(
                    "Removed empty connection %d (remaining=%d)",
                    conn_id,
                    len(self._connections),
                )

        log.info(
            "Unsubscribed from %d markets (remaining=%d)",
            removed,
            len(self._markets),
        )

    # -- Consumer interface --------------------------------------------------

    async def book_updates(self) -> AsyncIterator[BookEvent]:
        """Async iterator yielding BookEvent objects."""
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if event is _SENTINEL:
                return

            if isinstance(event, BookEvent):
                yield event

    # -- Diagnostics ---------------------------------------------------------

    @property
    def subscribed_market_count(self) -> int:
        return len(self._markets)

    @property
    def connection_count(self) -> int:
        return len(self._connections)

    @property
    def is_healthy(self) -> bool:
        if not self._connections:
            return True  # No connections yet is OK (pre-subscribe)
        return all(c.is_connected for c in self._connections.values())

    # -- Internal ------------------------------------------------------------

    def _find_connection_with_capacity(
        self,
        pending: Optional[dict[int, list]] = None,
    ) -> Optional[int]:
        """Find the first connection with room for 2 more assets.

        Args:
            pending: Assets allocated in this batch but not yet sent.
        """
        for conn_id, conn in self._connections.items():
            pending_count = len(pending.get(conn_id, ())) if pending else 0
            if conn.desired_count + pending_count + 2 <= MAX_ASSETS_PER_CONNECTION:
                return conn_id
        return None

    async def _add_connection(self) -> int:
        """Create a new WebSocket connection and start its listener task.

        Returns:
            The ID of the newly created connection.
        """
        conn_id = self._next_conn_id
        self._next_conn_id += 1

        conn = WebSocketConnection(
            on_book=self._on_book_update,
            on_price_change=self._on_price_change,
            on_last_trade_price=self._on_last_trade_price,
            on_tick_size_change=self._on_tick_size_change,
            index=conn_id,
        )
        await conn.start()
        self._connections[conn_id] = conn

        log.info(
            "Connection %d established (total=%d)",
            conn_id,
            len(self._connections),
        )
        return conn_id

    def _enqueue(self, event: BookEvent) -> None:
        """Non-blocking enqueue; fatal exit on queue full."""
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            log.critical(
                "Event queue full (size=%d). Consumer cannot keep up, exiting.",
                self._queue.maxsize,
            )
            os._exit(1)

    def _on_market_unsubscribed(self, market: MarketSubscription) -> None:
        """Hook called after a market is removed from the registry.

        Override to perform cleanup (e.g., evict cache entries).
        """

    async def _stats_loop(self) -> None:
        """Log per-connection stats every minute."""
        iteration = 0
        try:
            while self._running:
                await asyncio.sleep(self._STATS_INTERVAL_SECONDS)
                iteration += 1
                if not self._connections:
                    continue
                qsize = self._queue.qsize()
                qmax = self._queue.maxsize
                qpct = (qsize / qmax * 100) if qmax > 0 else 0.0
                log.info(
                    "[QUEUE-STATS] iter=%d queue_size=%d queue_max=%d queue_pct=%.1f%%",
                    iteration,
                    qsize,
                    qmax,
                    qpct,
                )
                if qpct > 50:
                    log.warning(
                        "[QUEUE-PRESSURE] Queue above 50%% — size=%d max=%d pct=%.1f%%"
                        " (consumer may not be keeping up)",
                        qsize,
                        qmax,
                        qpct,
                    )
                for conn_id, conn in self._connections.items():
                    period = conn.read_and_reset_period_stats()
                    log.info(
                        "[CONN-STATS] iter=%d window=%ds conn=%d assets=%d"
                        " books=%d price_changes=%d trades=%d tick_changes=%d"
                        " reconnects=%d total_added=%d total_removed=%d"
                        " subscribe_calls=%d unsubscribe_calls=%d",
                        iteration,
                        self._STATS_INTERVAL_SECONDS,
                        conn_id,
                        conn.desired_count,
                        period["book_events"],
                        period["price_changes"],
                        period["last_trade_prices"],
                        period["tick_size_changes"],
                        conn.reconnect_count,
                        conn.total_assets_added,
                        conn.total_assets_removed,
                        conn.total_subscribe_calls,
                        conn.total_unsubscribe_calls,
                    )
        except asyncio.CancelledError:
            pass

    @abstractmethod
    def _on_book_update(self, snapshot: OrderBookSnapshot) -> None:
        """Handle a full book snapshot from the WebSocket."""

    @abstractmethod
    def _on_price_change(self, change: PriceChange) -> None:
        """Handle a price change from the WebSocket."""

    @abstractmethod
    def _on_last_trade_price(self, trade: LastTradePrice) -> None:
        """Handle a last_trade_price event from the WebSocket."""

    @abstractmethod
    def _on_tick_size_change(self, change: TickSizeChange) -> None:
        """Handle a tick_size_change event from the WebSocket."""
