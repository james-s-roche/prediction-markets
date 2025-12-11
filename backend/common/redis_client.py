"""Redis helper using `redis.asyncio`.

Provides a small helper to get a shared Redis connection and publish normalized market ticks
on channels of the form `market_updates:{ticker}`.
"""
import os
import json
from typing import Any

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover - graceful import handling
    redis = None


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

_redis_client: "redis.Redis" | None = None


async def get_redis() -> "redis.Redis":
    global _redis_client
    if _redis_client is None:
        if redis is None:
            raise RuntimeError("`redis` package with asyncio support is required. `pip install redis`")
        _redis_client = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    return _redis_client


async def publish_market_tick(ticker: str, payload: Any) -> None:
    r = await get_redis()
    channel = f"market_updates:{ticker}"
    await r.publish(channel, json.dumps(payload, default=str))


async def close_redis() -> None:
    global _redis_client
    if _redis_client is not None:
        await _redis_client.close()
        _redis_client = None
