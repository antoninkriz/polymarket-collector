"""Polymarket data types shared by the Python services."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MarketSubscription:
    """Binary market identifiers needed by the collector."""

    market: str
    yes_asset_id: str
    no_asset_id: str
