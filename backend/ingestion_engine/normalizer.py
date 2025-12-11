"""Normalization helpers for raw exchange payloads.

Put exchange-specific parsing/normalization logic here and return `MarketTick`-compatible structures.
"""
from typing import Dict, Any

from backend.common.models import MarketTick, normalize_price
from datetime import datetime


def normalize_kalshi(raw: Dict[str, Any]) -> MarketTick:
    price = normalize_price("kalshi", raw.get("price"))
    ts = raw.get("time") or raw.get("ts")
    time = datetime.fromisoformat(ts) if isinstance(ts, str) else datetime.utcnow()
    return MarketTick(
        time=time,
        ticker_symbol=raw.get("symbol") or raw.get("market") or "",
        platform="kalshi",
        price=price,
        volume=raw.get("volume"),
    )
