"""Pydantic models for Kalshi market lifecycle tracking."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
)

MarketKind = Literal["regular", "multivariate"]

KIND_REGULAR: MarketKind = "regular"
KIND_MULTIVARIATE: MarketKind = "multivariate"

REGULAR_LIFECYCLE_CHANNEL = "market_lifecycle_v2"
MULTIVARIATE_LIFECYCLE_CHANNEL = "multivariate_market_lifecycle"
KALSHI_LIFECYCLE_CHANNELS = (
    REGULAR_LIFECYCLE_CHANNEL,
    MULTIVARIATE_LIFECYCLE_CHANNEL,
)

ACTIVE_MARKET_STATUSES = ("unopened", "open", "paused", "closed")
REGULAR_IN_EVENTS = frozenset({
    "created",
    "activated",
    "deactivated",
    "close_date_updated",
    "determined",
    "fractional_trading_updated",
    "price_level_structure_updated",
})
MULTIVARIATE_IN_EVENTS = frozenset({
    "created",
    "activated",
    "deactivated",
    "close_date_updated",
    "determined",
})


class KalshiMarket(BaseModel):
    """Minimal Kalshi market data needed for active ticker subscriptions."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    ticker: str
    event_ticker: str = ""
    title: str = ""
    status: str = ""
    open_ts: Optional[datetime] = None
    close_ts: Optional[datetime] = None
    created_ts: Optional[int] = None
    settled_ts: Optional[int] = None


class KalshiCacheData(BaseModel):
    """Timestamped snapshot of active Kalshi regular and multivariate markets."""

    model_config = ConfigDict(extra="ignore")

    fetched_at: datetime
    regular_markets: list[KalshiMarket] = Field(default_factory=list)
    multivariate_markets: list[KalshiMarket] = Field(default_factory=list)

    @property
    def total_count(self) -> int:
        return len(self.regular_markets) + len(self.multivariate_markets)

    @classmethod
    def from_markets(
        cls,
        *,
        regular_markets: list[KalshiMarket],
        multivariate_markets: list[KalshiMarket],
    ) -> KalshiCacheData:
        return cls(
            fetched_at=datetime.now(timezone.utc),
            regular_markets=regular_markets,
            multivariate_markets=multivariate_markets,
        )

    @field_serializer("fetched_at")
    def _serialize_fetched_at(self, value: datetime) -> str:
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @classmethod
    def from_json(cls, raw: str) -> KalshiCacheData:
        return cls.model_validate_json(raw)

    def to_json(self) -> str:
        return self.model_dump_json()


class KalshiLifecycleMetadata(BaseModel):
    """Optional metadata object included on Kalshi lifecycle messages."""

    model_config = ConfigDict(extra="ignore")

    event_ticker: Optional[str] = None
    title: Optional[str] = None
    name: Optional[str] = None


class KalshiLifecycleMessage(BaseModel):
    """Typed ``msg`` payload for Kalshi lifecycle websocket frames."""

    model_config = ConfigDict(extra="ignore")

    market_ticker: str
    event_type: str
    open_ts: Optional[datetime] = None
    close_ts: Optional[datetime] = None
    additional_metadata: KalshiLifecycleMetadata = Field(
        default_factory=KalshiLifecycleMetadata,
    )
    title: Optional[str] = None

    def to_market(self) -> KalshiMarket:
        return KalshiMarket(
            ticker=self.market_ticker,
            event_ticker=self.additional_metadata.event_ticker or "",
            title=(
                self.additional_metadata.title
                or self.additional_metadata.name
                or self.title
                or ""
            ),
            open_ts=self.open_ts,
            close_ts=self.close_ts,
        )


class KalshiLifecycleFrame(BaseModel):
    """A regular or multivariate Kalshi lifecycle websocket frame."""

    model_config = ConfigDict(extra="ignore")

    type: Literal[
        "market_lifecycle_v2",
        "multivariate_market_lifecycle",
    ]
    msg: KalshiLifecycleMessage

    @property
    def kind(self) -> MarketKind:
        if self.type == REGULAR_LIFECYCLE_CHANNEL:
            return KIND_REGULAR
        return KIND_MULTIVARIATE

    @classmethod
    def from_json(cls, raw: str) -> KalshiLifecycleFrame:
        return cls.model_validate_json(raw)


class KalshiRestMarket(BaseModel):
    """REST market shape normalized into ``KalshiMarket``."""

    model_config = ConfigDict(extra="ignore", from_attributes=True)

    ticker: str
    event_ticker: Optional[str] = None
    title: Optional[str] = None
    status: Optional[str] = None
    open_time: Optional[datetime] = None
    close_time: Optional[datetime] = None
    created_time: Optional[datetime] = None
    created_ts: Optional[int] = None
    settled_time: Optional[datetime] = None
    settlement_ts: Optional[datetime] = None
    settled_ts: Optional[int] = None

    def to_market(self) -> KalshiMarket:
        return KalshiMarket(
            ticker=self.ticker,
            event_ticker=self.event_ticker or "",
            title=self.title or "",
            status=self.status or "",
            open_ts=self.open_time,
            close_ts=self.close_time,
            created_ts=self.created_timestamp,
            settled_ts=self.settled_timestamp,
        )

    @property
    def created_timestamp(self) -> Optional[int]:
        return self.created_ts or _datetime_timestamp(self.created_time)

    @property
    def settled_timestamp(self) -> Optional[int]:
        return (
            self.settled_ts
            or _datetime_timestamp(self.settled_time)
            or _datetime_timestamp(self.settlement_ts)
        )


class KalshiMarketsResponse(BaseModel):
    """Subset of the Kalshi get-markets response used by this service."""

    model_config = ConfigDict(extra="ignore", from_attributes=True)

    markets: list[KalshiRestMarket] = Field(default_factory=list)
    cursor: Optional[str] = None


class KalshiStreamPayload(BaseModel):
    """Payload published to the Kalshi market-events Redis stream."""

    model_config = ConfigDict(extra="ignore")

    ticker: str
    market_kind: MarketKind
    event_ticker: str = ""
    title: str = ""
    open_ts: Optional[datetime] = None
    close_ts: Optional[datetime] = None
    timestamp: str


def in_events_for_kind(kind: MarketKind) -> frozenset[str]:
    if kind == KIND_REGULAR:
        return REGULAR_IN_EVENTS
    return MULTIVARIATE_IN_EVENTS


def _datetime_timestamp(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())
