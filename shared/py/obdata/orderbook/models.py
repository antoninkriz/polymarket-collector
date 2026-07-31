"""Data models for the Polymarket order book streaming service.

All ``timestamp`` fields are the raw string ms-epoch value as sent by
Polymarket. We never substitute local receive time.

Field naming mirrors the Polymarket WebSocket market channel payloads
(see https://docs.polymarket.com/api-reference/wss/market) so that what
we store matches what we receive on the wire. The market hash is named
``market`` everywhere, matching the WSS field. Polymarket's REST/Gamma
APIs call the same value ``conditionId`` — that name is only used at
the Gamma boundary (``gamma.parse_markets``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Callable, Optional


@dataclass
class BookLevel:
    """A single price level in the orderbook."""

    price: Decimal
    size: Decimal


@dataclass
class OrderBookSnapshot:
    """Full orderbook snapshot for a single asset (``event_type=book``).

    Field names mirror the Polymarket WSS ``book`` payload exactly.
    """

    asset_id: str
    market: str
    bids: list[BookLevel]
    asks: list[BookLevel]
    timestamp: str
    hash: str


@dataclass
class PriceChange:
    """Single entry from a ``price_change`` event's ``price_changes`` array.

    ``timestamp`` is copied from the parent payload (Polymarket sends one
    timestamp per event, shared by all entries in ``price_changes``).
    """

    asset_id: str
    market: str
    price: Decimal
    size: Decimal
    side: str  # "BUY" or "SELL"
    best_bid: Optional[Decimal]
    best_ask: Optional[Decimal]
    timestamp: str
    hash: Optional[str] = None


@dataclass
class LastTradePrice:
    """``last_trade_price`` event payload."""

    asset_id: str
    market: str
    price: Decimal
    size: Decimal
    side: str  # "BUY" or "SELL"
    fee_rate_bps: str
    transaction_hash: str
    timestamp: str


@dataclass
class TickSizeChange:
    """``tick_size_change`` event payload."""

    asset_id: str
    market: str
    old_tick_size: Decimal
    new_tick_size: Decimal
    timestamp: str


@dataclass
class MarketSubscription:
    """Minimal market info needed to subscribe to order book updates.

    Every Polymarket market is binary (YES + NO). Neg-risk events are
    decomposed into multiple binary markets upstream.
    """

    market: str  # market hash (Polymarket WSS `market` / Gamma `conditionId`)
    yes_asset_id: str
    no_asset_id: str


class EventType(Enum):
    """Polymarket WSS market channel event types."""

    BOOK = "book"
    PRICE_CHANGE = "price_change"
    LAST_TRADE_PRICE = "last_trade_price"
    TICK_SIZE_CHANGE = "tick_size_change"


@dataclass
class BookEvent:
    """Single event yielded by the async iterator.

    Field names mirror the corresponding Polymarket WSS payload fields.
    """

    event_type: EventType
    market: str
    asset_id: str

    # Full book levels — only populated for BOOK
    bids: list[BookLevel] = field(default_factory=list)
    asks: list[BookLevel] = field(default_factory=list)
    hash: Optional[str] = None  # Polymarket book hash

    # Populated for PRICE_CHANGE and LAST_TRADE_PRICE
    price: Optional[Decimal] = None
    size: Optional[Decimal] = None
    side: Optional[str] = None  # "BUY" or "SELL"

    # Populated for PRICE_CHANGE (sent by Polymarket inside price_changes entries)
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None

    # Populated for LAST_TRADE_PRICE
    fee_rate_bps: Optional[str] = None
    transaction_hash: Optional[str] = None

    # Populated for TICK_SIZE_CHANGE
    old_tick_size: Optional[Decimal] = None
    new_tick_size: Optional[Decimal] = None

    # Polymarket server-side timestamp, string ms epoch (e.g. "1757908892351")
    timestamp: str = ""


# Callback types used by WebSocketConnection
BookCallback = Callable[[OrderBookSnapshot], None]
PriceChangeCallback = Callable[[PriceChange], None]
LastTradePriceCallback = Callable[[LastTradePrice], None]
TickSizeChangeCallback = Callable[[TickSizeChange], None]
