"""Kalshi lifecycle websocket client."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass

import websockets
from pydantic import ValidationError

from obdata.kalshi_models import (
    KALSHI_LIFECYCLE_CHANNELS,
    KalshiLifecycleFrame,
)

log = logging.getLogger(__name__)

DEFAULT_KALSHI_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"


@dataclass(frozen=True)
class KalshiWebSocketConfig:
    auth_headers: Mapping[str, str] | Callable[[], Mapping[str, str]]
    ws_url: str = DEFAULT_KALSHI_WS_URL
    channels: Sequence[str] = KALSHI_LIFECYCLE_CHANNELS


class KalshiLifecycleWebSocketClient:
    """Connects to Kalshi and yields typed lifecycle frames."""

    def __init__(self, config: KalshiWebSocketConfig) -> None:
        self._config = config

    async def messages(self) -> AsyncIterator[KalshiLifecycleFrame]:
        async with websockets.connect(
            self._config.ws_url,
            additional_headers=self._auth_headers(),
        ) as websocket:
            await websocket.send(json.dumps({
                "id": 1,
                "cmd": "subscribe",
                "params": {"channels": list(self._config.channels)},
            }))
            log.info(
                "Subscribed to Kalshi lifecycle websocket channels: %s",
                ", ".join(self._config.channels),
            )

            async for raw_message in websocket:
                try:
                    yield KalshiLifecycleFrame.from_json(raw_message)
                except ValidationError:
                    log.debug(
                        "Skipping non-lifecycle Kalshi websocket frame: %s",
                        raw_message,
                    )
                except ValueError:
                    log.warning("Skipping malformed Kalshi websocket frame")

    def _auth_headers(self) -> dict[str, str]:
        headers = self._config.auth_headers
        if callable(headers):
            headers = headers()
        log.info(
            "Opening Kalshi websocket ws_url=%s auth_header_names=%s",
            self._config.ws_url,
            sorted(headers),
        )
        return dict(headers)
