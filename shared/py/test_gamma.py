from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import httpx2

from obdata.gamma import (
    GAMMA_RATE_LIMIT_RETRY_SECONDS,
    GAMMA_REQUEST_INTERVAL_SECONDS,
    GammaClient,
)


def _response(status_code: int, retry_after: str | None = None) -> httpx2.Response:
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    return httpx2.Response(
        status_code,
        headers=headers,
        request=httpx2.Request("GET", "https://gamma-api.polymarket.com/markets"),
    )


def test_gamma_client_spaces_requests() -> None:
    async def run() -> list[float]:
        sleeps: list[float] = []
        sleep = AsyncMock(side_effect=lambda delay: sleeps.append(delay))
        get = AsyncMock(side_effect=[_response(200), _response(200)])

        with (
            patch.object(httpx2.AsyncClient, "get", get),
            patch("obdata.gamma.asyncio.sleep", sleep),
        ):
            client = GammaClient()
            try:
                await client.get("/markets", params={})
                await client.get("/markets", params={})
            finally:
                await client.close()

        assert get.await_count == 2
        return sleeps

    sleeps = asyncio.run(run())
    assert len(sleeps) == 1
    assert 0 < sleeps[0] <= GAMMA_REQUEST_INTERVAL_SECONDS


def test_gamma_client_retries_after_429() -> None:
    async def run() -> list[float]:
        sleeps: list[float] = []
        sleep = AsyncMock(side_effect=lambda delay: sleeps.append(delay))
        get = AsyncMock(side_effect=[_response(429, "3"), _response(200)])

        with (
            patch.object(httpx2.AsyncClient, "get", get),
            patch("obdata.gamma.asyncio.sleep", sleep),
        ):
            client = GammaClient()
            try:
                response = await client.get("/markets", params={})
            finally:
                await client.close()

        assert response.status_code == 200
        assert get.await_count == 2
        return sleeps

    sleeps = asyncio.run(run())
    assert len(sleeps) == 1
    assert 0 < sleeps[0] <= 3


def test_gamma_client_uses_default_429_delay() -> None:
    async def run() -> list[float]:
        sleeps: list[float] = []
        sleep = AsyncMock(side_effect=lambda delay: sleeps.append(delay))
        get = AsyncMock(side_effect=[_response(429), _response(200)])

        with (
            patch.object(httpx2.AsyncClient, "get", get),
            patch("obdata.gamma.asyncio.sleep", sleep),
        ):
            client = GammaClient()
            try:
                await client.get("/markets", params={})
            finally:
                await client.close()

        return sleeps

    sleeps = asyncio.run(run())
    assert len(sleeps) == 1
    assert 0 < sleeps[0] <= GAMMA_RATE_LIMIT_RETRY_SECONDS
