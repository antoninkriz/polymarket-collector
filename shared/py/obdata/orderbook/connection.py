"""Single WebSocket connection to the Polymarket CLOB.

Handles connecting, subscribing, listening, parsing events, caching
orderbook state, and delegating parsed events to callbacks.

Operates on raw token (asset) IDs — has no concept of markets or YES/NO sides.

Key fix over connection.py: validates PONG responses to detect dead connections.
Per Polymarket docs, client sends "PING" every 5s and server responds "PONG".
If no PONG is received within the timeout, the connection is closed and
run() reconnects immediately (no delay).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from decimal import Decimal
from typing import Any, Optional

import websockets
from websockets.exceptions import ConnectionClosed
from websockets.protocol import State

from obdata.orderbook.models import (
    BookCallback,
    BookLevel,
    LastTradePrice,
    LastTradePriceCallback,
    OrderBookSnapshot,
    PriceChange,
    PriceChangeCallback,
    TickSizeChange,
    TickSizeChangeCallback,
)

log = logging.getLogger(__name__)

# Polymarket WebSocket endpoint
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
MAX_ASSETS_PER_CONNECTION = int(os.environ.get("MAX_ASSETS_PER_CONN", "200"))

_PING_INTERVAL_SECONDS = 5
_PONG_TIMEOUT_SECONDS = 5


class WebSocketConnection:
    """Single WebSocket connection to the Polymarket CLOB.

    Handles connecting, subscribing, listening for messages, parsing
    events, and caching orderbook state. Delegates parsed events to
    callbacks provided by the caller.

    Subscription tracking uses two sets:

    - ``_desired_assets``: durable intent — what this connection *should* be
      subscribed to. Persists across reconnects, used to filter incoming
      events and to re-subscribe after a connection drop.
    - ``_subscribed_assets``: transient state — what has actually been sent
      to the server in the current WebSocket session. Cleared on every
      ``connect()`` call so that ``subscribe()`` can diff against it and
      avoid sending duplicates during normal operation, while still
      re-sending everything after a reconnect.

    Heartbeat protocol (per Polymarket docs):
    - Client sends ``"PING"`` every 5 seconds
    - Server responds with ``"PONG"``
    - If no PONG received within 5s, connection is closed and ``run()``
      reconnects immediately with no delay.

    Usage::

        conn = WebSocketConnection(on_book=..., on_price_change=...)
        await conn.start()   # connect with retries + start listener task
        await conn.close()   # cancel listener + close ws
    """

    CONNECT_MAX_RETRIES = 5
    CONNECT_STAGGER_SECONDS = 0.1

    def __init__(
        self,
        on_book: Optional[BookCallback] = None,
        on_price_change: Optional[PriceChangeCallback] = None,
        on_last_trade_price: Optional[LastTradePriceCallback] = None,
        on_tick_size_change: Optional[TickSizeChangeCallback] = None,
        index: int = -1,
    ) -> None:
        self._ws: Any = None
        self._running = False
        self._subscribed_assets: set[str] = set()
        self._desired_assets: set[str] = set()
        self._has_initial_subscription: bool = False

        self._on_book = on_book
        self._on_price_change = on_price_change
        self._on_last_trade_price = on_last_trade_price
        self._on_tick_size_change = on_tick_size_change
        self.index = index

        # Cached state
        self._orderbooks: dict[str, OrderBookSnapshot] = {}

        # Metrics
        self._last_message_time: float = 0.0
        self._last_pong_time: float = 0.0
        self._message_count: int = 0
        self._pong_count: int = 0
        self._missed_pong_count: int = 0
        self._reconnect_count: int = 0
        self._period_book_count: int = 0
        self._period_price_change_count: int = 0
        self._period_last_trade_price_count: int = 0
        self._period_tick_size_change_count: int = 0

        # Lifetime counters
        self._total_assets_added: int = 0
        self._total_assets_removed: int = 0
        self._total_subscribe_calls: int = 0
        self._total_unsubscribe_calls: int = 0

        # Set by _ping_loop when PONG timeout triggers the close, so
        # run() can skip the backoff delay and reconnect immediately.
        self._pong_timed_out: bool = False

        # Background tasks
        self._ping_task: Optional[asyncio.Task] = None
        self._listener_task: Optional[asyncio.Task] = None

    # -- Lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        """Connect with retries and start the listener task.

        Retries with exponential backoff if the initial connect fails,
        then staggers to avoid overwhelming the server.
        """
        delay = 1.0
        for attempt in range(self.CONNECT_MAX_RETRIES):
            try:
                await self.connect()
                break
            except Exception:
                if attempt == self.CONNECT_MAX_RETRIES - 1:
                    await self.close()
                    raise
                log.warning(
                    "Connection attempt %d/%d failed, retrying in %.1fs",
                    attempt + 1,
                    self.CONNECT_MAX_RETRIES,
                    delay,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

        self._listener_task = asyncio.create_task(self.run())

        # Stagger to avoid overwhelming the server with simultaneous connections
        await asyncio.sleep(self.CONNECT_STAGGER_SECONDS)

    async def connect(self) -> None:
        """Connect to the Polymarket WebSocket server."""
        log.info("Connecting to %s (conn=%d)", WS_MARKET_URL, self.index)
        try:
            # Polymarket uses application-level "PING"/"PONG" text messages,
            # not WebSocket protocol-level ping/pong frames. We disable the
            # library's built-in ping and handle it ourselves in _ping_loop().
            self._ws = await websockets.connect(
                WS_MARKET_URL,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                open_timeout=10,
            )
            self._running = True
            now = time.time()
            self._last_message_time = now
            self._last_pong_time = now
            self._subscribed_assets.clear()
            self._has_initial_subscription = False
            self._start_ping()
            log.info("WebSocket connected (conn=%d)", self.index)
        except Exception:
            log.error("WebSocket connection failed (conn=%d)", self.index, exc_info=True)
            raise

    async def close(self) -> None:
        """Cancel the listener task and close the WebSocket connection."""
        self._running = False
        self._stop_ping()

        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None

        if self._ws:
            await self._ws.close()
            self._ws = None
        log.info("WebSocket closed")

    # -- Heartbeat (PING/PONG) -----------------------------------------------

    def _start_ping(self) -> None:
        """Start the PING/PONG heartbeat loop."""
        self._stop_ping()
        self._ping_task = asyncio.create_task(self._ping_loop())

    def _stop_ping(self) -> None:
        """Cancel the PING/PONG heartbeat loop."""
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        self._ping_task = None

    async def _ping_loop(self) -> None:
        """Send PING every interval and verify PONG response.

        Per Polymarket docs, client sends "PING" and server responds "PONG".
        If no PONG is received within the timeout, the connection is
        considered dead — close it so listen() exits and run() reconnects.
        """
        try:
            while self._running and self._ws and self.is_connected:
                await asyncio.sleep(_PING_INTERVAL_SECONDS)
                if not (self._ws and self.is_connected):
                    break

                pong_time_before_ping = self._last_pong_time
                await self._ws.send("PING")

                # Wait for PONG response
                await asyncio.sleep(_PONG_TIMEOUT_SECONDS)

                if self._last_pong_time <= pong_time_before_ping:
                    # No PONG received — connection is dead
                    self._missed_pong_count += 1
                    self._pong_timed_out = True
                    log.warning(
                        "No PONG received within %ds, closing dead connection"
                        " (conn=%d, missed_pongs=%d, last_pong=%.1fs ago)",
                        _PONG_TIMEOUT_SECONDS,
                        self.index,
                        self._missed_pong_count,
                        time.time() - self._last_pong_time,
                    )
                    if self._ws:
                        await self._ws.close()
                    break

        except ConnectionClosed:
            log.debug("Ping loop stopped: connection closed (conn=%d)", self.index)
        except asyncio.CancelledError:
            pass
        except Exception:
            log.debug("Ping loop error (conn=%d)", self.index, exc_info=True)

    # -- Subscription --------------------------------------------------------

    async def subscribe(self, asset_ids: list[str]) -> None:
        """Subscribe to orderbook updates for the given asset (token) IDs.

        Stores asset IDs in ``_desired_assets`` so they survive reconnects,
        then attempts to send the subscription. Never raises — if the send
        fails, ``run()`` will re-subscribe on the next reconnect.
        """
        if len(asset_ids) > MAX_ASSETS_PER_CONNECTION:
            log.warning(
                "Too many assets for single connection: %d (max %d)",
                len(asset_ids),
                MAX_ASSETS_PER_CONNECTION,
            )
            asset_ids = asset_ids[:MAX_ASSETS_PER_CONNECTION]

        new_desired = [a for a in asset_ids if a not in self._desired_assets]
        self._total_assets_added += len(new_desired)
        self._total_subscribe_calls += 1
        self._desired_assets.update(asset_ids)

        new_assets = [a for a in asset_ids if a not in self._subscribed_assets]
        if not new_assets:
            return

        self._subscribed_assets.update(new_assets)

        if self._ws and self.is_connected:
            try:
                await self._send_subscription(new_assets)
            except Exception:
                log.warning("Subscribe send failed, will retry on reconnect")

    async def unsubscribe(self, asset_ids: list[str]) -> None:
        """Remove tokens from desired set and send an unsubscribe message."""
        removed: list[str] = []
        self._total_unsubscribe_calls += 1
        for aid in asset_ids:
            if aid in self._desired_assets:
                self._desired_assets.discard(aid)
                self._subscribed_assets.discard(aid)
                self._orderbooks.pop(aid, None)
                self._total_assets_removed += 1
                removed.append(aid)

        if removed:
            await self._send_unsubscription(removed)

    def reset_subscriptions(self) -> None:
        """Clear all subscription tracking (does not send unsubscribe)."""
        self._subscribed_assets.clear()
        self._desired_assets.clear()

    async def _send_subscription(self, asset_ids: list[str]) -> None:
        if not self._ws or not self.is_connected:
            log.warning("conn=%d _send_subscription skipped: ws=%s connected=%s",
                        self.index, self._ws is not None, self.is_connected)
            return

        if not self._has_initial_subscription:
            message = {"assets_ids": asset_ids, "type": "market"}
            self._has_initial_subscription = True
        else:
            message = {"assets_ids": asset_ids, "operation": "subscribe"}

        await self._ws.send(json.dumps(message))
        log.info("conn=%d subscribed to %d assets", self.index, len(asset_ids))

    async def _send_unsubscription(self, asset_ids: list[str]) -> None:
        if not self._ws or not self.is_connected:
            log.warning(
                "conn=%d _send_unsubscription skipped: ws=%s connected=%s",
                self.index, self._ws is not None, self.is_connected,
            )
            return

        message = {"assets_ids": asset_ids, "operation": "unsubscribe"}
        await self._ws.send(json.dumps(message))
        log.info("conn=%d unsubscribed from %d assets", self.index, len(asset_ids))

    # -- Listening -----------------------------------------------------------

    async def listen(self) -> None:
        """Listen for messages until the connection drops.

        Returns normally on disconnect — does not set ``_running = False``
        so that ``run()`` can reconnect. Call ``close()`` to stop ``run()``.
        """
        if not self._ws:
            raise RuntimeError("Not connected")
        try:
            async for message in self._ws:
                try:
                    self._handle_message(message)
                except Exception:
                    log.error("Error handling message", exc_info=True)
        except ConnectionClosed as e:
            log.warning(
                "WebSocket closed: conn=%d code=%s reason=%s",
                self.index, e.code, e.reason,
            )
        except Exception:
            log.error("WebSocket error: conn=%d", self.index, exc_info=True)

    async def run(self) -> None:
        """Listen with automatic reconnection and subscription recovery.

        Loops until ``close()`` is called. On each reconnect, re-subscribes
        all ``_desired_assets`` so subscriptions are guaranteed to survive
        connection drops.

        If the ping loop detected a PONG timeout, reconnects immediately
        with no delay. Otherwise uses exponential backoff.
        """
        reconnect_delay = 1.0

        while self._running:
            try:
                await self.listen()
            except Exception:
                log.error("WebSocket error", exc_info=True)

            if not self._running:
                break

            self._reconnect_count += 1
            disconnect_time = self._last_message_time

            # Skip delay if ping loop detected a dead connection
            if self._pong_timed_out:
                self._pong_timed_out = False
                log.info(
                    "PONG timeout — reconnecting immediately (conn=%d, reconnects=%d)",
                    self.index, self._reconnect_count,
                )
            else:
                log.info(
                    "Reconnecting conn=%d (delay=%.1fs, reconnects=%d)",
                    self.index, reconnect_delay, self._reconnect_count,
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60.0)

            try:
                await self.connect()
                if self._desired_assets:
                    await self.subscribe(list(self._desired_assets))
                reconnect_delay = 1.0
                if disconnect_time > 0:
                    gap_seconds = time.time() - disconnect_time
                    log.warning(
                        "[DATA-GAP] Reconnect gap: conn=%d gap_seconds=%.1f"
                        " assets=%d reconnects=%d affected_assets=%s",
                        self.index,
                        gap_seconds,
                        len(self._desired_assets),
                        self._reconnect_count,
                        list(self._desired_assets),
                    )
            except Exception:
                log.error(
                    "Reconnect failed: conn=%d", self.index, exc_info=True,
                )

    # -- Message parsing (all synchronous) -----------------------------------

    def _handle_message(self, raw_message: str) -> None:
        self._last_message_time = time.time()
        self._message_count += 1

        if not raw_message or not raw_message.strip():
            return

        # Handle PONG response from server
        stripped = raw_message.strip()
        if stripped == "PONG":
            self._last_pong_time = time.time()
            self._pong_count += 1
            log.debug("PONG received (conn=%d, total=%d)", self.index, self._pong_count)
            return

        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError:
            log.debug("Non-JSON message (conn=%d, len=%d): %s",
                      self.index, len(raw_message), raw_message[:100])
            return

        if isinstance(data, list):
            for item in data:
                self._process_event(item)
        else:
            self._process_event(data)

    def _process_event(self, data: dict) -> None:
        if not isinstance(data, dict):
            return

        event_type = data.get("event_type")
        if event_type == "book":
            self._handle_book(data)
        elif event_type == "price_change":
            self._handle_price_change(data)
        elif event_type == "last_trade_price":
            self._handle_last_trade_price(data)
        elif event_type == "tick_size_change":
            self._handle_tick_size_change(data)

    def _handle_book(self, data: dict) -> None:
        try:
            asset_id = data.get("asset_id", "")
            if asset_id not in self._desired_assets:
                return

            bids = [
                BookLevel(
                    price=Decimal(str(b.get("price", "0"))),
                    size=Decimal(str(b.get("size", "0"))),
                )
                for b in data.get("bids", [])
            ]
            asks = [
                BookLevel(
                    price=Decimal(str(a.get("price", "0"))),
                    size=Decimal(str(a.get("size", "0"))),
                )
                for a in data.get("asks", [])
            ]
            snapshot = OrderBookSnapshot(
                asset_id=asset_id,
                market=data.get("market", ""),
                bids=bids,
                asks=asks,
                timestamp=data.get("timestamp", ""),
                hash=data.get("hash", ""),
            )
            self._orderbooks[snapshot.asset_id] = snapshot

            self._period_book_count += 1
            if self._on_book:
                self._on_book(snapshot)
        except Exception:
            log.error("Error parsing book message", exc_info=True)

    def _handle_price_change(self, data: dict) -> None:
        try:
            market = data.get("market", "")
            timestamp = data.get("timestamp", "")
            for change in data.get("price_changes", []):
                asset_id = change.get("asset_id", "")
                if asset_id not in self._desired_assets:
                    continue

                pc = PriceChange(
                    asset_id=asset_id,
                    market=market,
                    price=Decimal(str(change.get("price", "0"))),
                    size=Decimal(str(change.get("size", "0"))),
                    side=change.get("side", ""),
                    best_bid=(
                        Decimal(str(change["best_bid"]))
                        if change.get("best_bid")
                        else None
                    ),
                    best_ask=(
                        Decimal(str(change["best_ask"]))
                        if change.get("best_ask")
                        else None
                    ),
                    timestamp=timestamp,
                    hash=change.get("hash") or None,
                )
                self._period_price_change_count += 1
                if self._on_price_change:
                    self._on_price_change(pc)
        except Exception:
            log.error("Error parsing price change", exc_info=True)

    def _handle_last_trade_price(self, data: dict) -> None:
        try:
            asset_id = data.get("asset_id", "")
            if asset_id not in self._desired_assets:
                return

            trade = LastTradePrice(
                asset_id=asset_id,
                market=data.get("market", ""),
                price=Decimal(str(data.get("price", "0"))),
                size=Decimal(str(data.get("size", "0"))),
                side=data.get("side", ""),
                fee_rate_bps=str(data.get("fee_rate_bps", "")),
                transaction_hash=data.get("transaction_hash", ""),
                timestamp=data.get("timestamp", ""),
            )
            self._period_last_trade_price_count += 1
            if self._on_last_trade_price:
                self._on_last_trade_price(trade)
        except Exception:
            log.error("Error parsing last_trade_price", exc_info=True)

    def _handle_tick_size_change(self, data: dict) -> None:
        try:
            asset_id = data.get("asset_id", "")
            if asset_id not in self._desired_assets:
                return

            change = TickSizeChange(
                asset_id=asset_id,
                market=data.get("market", ""),
                old_tick_size=Decimal(str(data.get("old_tick_size", "0"))),
                new_tick_size=Decimal(str(data.get("new_tick_size", "0"))),
                timestamp=data.get("timestamp", ""),
            )
            self._period_tick_size_change_count += 1
            if self._on_tick_size_change:
                self._on_tick_size_change(change)
        except Exception:
            log.error("Error parsing tick_size_change", exc_info=True)

    # -- State queries -------------------------------------------------------

    def get_orderbook(self, asset_id: str) -> Optional[OrderBookSnapshot]:
        return self._orderbooks.get(asset_id)

    @property
    def is_connected(self) -> bool:
        if self._ws is None:
            return False
        try:
            return self._ws.state == State.OPEN
        except AttributeError:
            return False

    @property
    def subscribed_count(self) -> int:
        return len(self._subscribed_assets)

    @property
    def desired_count(self) -> int:
        return len(self._desired_assets)

    def has_asset(self, asset_id: str) -> bool:
        return asset_id in self._desired_assets

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    @property
    def total_assets_added(self) -> int:
        return self._total_assets_added

    @property
    def total_assets_removed(self) -> int:
        return self._total_assets_removed

    @property
    def total_subscribe_calls(self) -> int:
        return self._total_subscribe_calls

    @property
    def total_unsubscribe_calls(self) -> int:
        return self._total_unsubscribe_calls

    def read_and_reset_period_stats(self) -> dict[str, int]:
        """Return per-period stats since last call and reset counters."""
        stats = {
            "book_events": self._period_book_count,
            "price_changes": self._period_price_change_count,
            "last_trade_prices": self._period_last_trade_price_count,
            "tick_size_changes": self._period_tick_size_change_count,
        }
        self._period_book_count = 0
        self._period_price_change_count = 0
        self._period_last_trade_price_count = 0
        self._period_tick_size_change_count = 0
        return stats

    @property
    def seconds_since_last_message(self) -> float:
        if self._last_message_time == 0:
            return 0.0
        return time.time() - self._last_message_time
