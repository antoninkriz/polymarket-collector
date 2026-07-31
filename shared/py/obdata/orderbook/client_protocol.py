"""Protocol for orderbook clients used by shared run-loop helpers."""

from __future__ import annotations

from typing import AsyncIterator, Protocol, runtime_checkable

from obdata.orderbook.models import BookEvent, MarketSubscription


@runtime_checkable
class SubscribableClient(Protocol):
    """Structural interface shared by OrderBookClient and OrderBookSnapshotClient."""

    async def subscribe_markets(self, markets: list[MarketSubscription]) -> None:
        ...

    async def unsubscribe_markets(self, markets: list[str]) -> None:
        ...

    def book_updates(self) -> AsyncIterator[BookEvent]:
        ...

    @property
    def subscribed_market_count(self) -> int:
        ...

    @property
    def is_healthy(self) -> bool:
        ...
