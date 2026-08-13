"""Shared constants used across services."""

from __future__ import annotations

import os
import sys

from dotenv import load_dotenv

load_dotenv()

# -- Polymarket API ----------------------------------------------------------

GAMMA_API = "https://gamma-api.polymarket.com"

# -- Redis -------------------------------------------------------------------

REDIS_URL = os.environ.get("REDIS_URL", "")

# Redis stream names
STREAM_MARKET_EVENTS = os.environ.get(
    "REDIS_STREAM_MARKET_EVENTS", "polymarket:market_events"
)

# Redis keys for active market cache
KEY_ACTIVE_MARKETS = os.environ.get(
    "REDIS_KEY_ACTIVE_MARKETS", "polymarket:active_markets"
)
KEY_ACTIVE_MARKETS_COUNT = os.environ.get(
    "REDIS_KEY_ACTIVE_MARKETS_COUNT", "polymarket:active_markets:count"
)


def require_redis_url() -> str:
    """Return REDIS_URL or exit with an error message if not configured."""
    if not REDIS_URL:
        print(
            "ERROR: REDIS_URL environment variable is not set.\n"
            "Please configure it, e.g.: export REDIS_URL=redis://localhost:6379",
            file=sys.stderr,
        )
        sys.exit(1)
    return REDIS_URL
