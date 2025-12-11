from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class OrderBookLevel(BaseModel):
    price: float
    size: float


class MarketTick(BaseModel):
    time: datetime
    ticker_symbol: str
    platform: str
    price: float
    volume: Optional[float] = None
    bid_depth: Optional[List[OrderBookLevel]] = Field(default_factory=list)
    ask_depth: Optional[List[OrderBookLevel]] = Field(default_factory=list)


def normalize_price(platform: str, price: Any) -> float:
    """Normalize prices from different exchanges to 0.0 - 1.0 internal range.

    - Polymarket: 0.00 - 1.00 already (USDC probability)
    - Kalshi: typically cents (1 - 99) -> convert to 0.01 - 0.99

    Accepts numeric types and strings (e.g. "0.58" or "58") depending on source.
    """
    try:
        p = float(price)
    except Exception:
        raise ValueError(f"Invalid price value: {price}")

    platform = platform.lower()
    if platform == "polymarket":
        # already 0.0 - 1.0
        return max(0.0, min(1.0, p))
    if platform == "kalshi":
        # Kalshi uses cents (e.g., 58 -> 0.58). If input is already 0-1, handle gracefully.
        if p > 1.0:
            return max(0.0, min(1.0, p / 100.0))
        return max(0.0, min(1.0, p))

    # Default: assume already normalized
    return max(0.0, min(1.0, p))
