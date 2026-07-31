"""Shared orderbook infrastructure: models, WebSocket connection, stats."""

from obdata.orderbook.base_client import BaseOrderBookClient
from obdata.orderbook.client_protocol import SubscribableClient
from obdata.orderbook.connection import MAX_ASSETS_PER_CONNECTION, WebSocketConnection
from obdata.orderbook.market_events import (
    handle_market_resolved,
    handle_new_market,
    load_markets_from_redis,
    stream_listener,
)
from obdata.orderbook.models import (
    BookCallback,
    BookEvent,
    BookLevel,
    EventType,
    LastTradePrice,
    LastTradePriceCallback,
    MarketSubscription,
    OrderBookSnapshot,
    PriceChange,
    PriceChangeCallback,
    TickSizeChange,
    TickSizeChangeCallback,
)
from obdata.orderbook.stats import ServiceStats

__all__ = [
    "BaseOrderBookClient",
    "BookCallback",
    "BookEvent",
    "BookLevel",
    "EventType",
    "LastTradePrice",
    "LastTradePriceCallback",
    "MAX_ASSETS_PER_CONNECTION",
    "MarketSubscription",
    "OrderBookSnapshot",
    "PriceChange",
    "PriceChangeCallback",
    "ServiceStats",
    "SubscribableClient",
    "TickSizeChange",
    "TickSizeChangeCallback",
    "WebSocketConnection",
    "handle_market_resolved",
    "handle_new_market",
    "load_markets_from_redis",
    "stream_listener",
]
