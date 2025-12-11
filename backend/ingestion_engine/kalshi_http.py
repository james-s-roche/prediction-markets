"""Lightweight Kalshi HTTP client built from OpenAPI spec.

Provides paginated endpoints:
- GET /markets (with cursor-based pagination and mve_filter)
- GET /events (with cursor-based pagination)
- GET /markets/{ticker}/orderbook (for on-demand orderbook snapshots)

Features:
- Rate limiting (token bucket) to respect API limits
- Exponential backoff retry for transient failures
- Async using httpx

This client uses `httpx` and is async.
"""
from typing import Any, Dict, Optional
import os
import asyncio
import time
import logging
import httpx

logger = logging.getLogger("kalshi_http")

BASE_URL = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
API_KEY = os.getenv("KALSHI_API_KEY")
RATE_LIMIT_PER_MINUTE = int(os.getenv("INGEST_RATE_LIMIT_PER_MINUTE", "120"))


class TokenBucket:
    """Simple token bucket for rate limiting."""
    
    def __init__(self, capacity: int, refill_rate_per_second: float):
        self.capacity = capacity
        self.refill_rate_per_second = refill_rate_per_second
        self.tokens = capacity
        self.last_refill = time.time()
    
    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(
            self.capacity,
            self.tokens + elapsed * self.refill_rate_per_second
        )
        self.last_refill = now
    
    async def acquire(self, tokens: int = 1) -> None:
        """Acquire tokens, blocking if necessary."""
        while True:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return
            # Wait a bit before checking again
            wait_time = (tokens - self.tokens) / self.refill_rate_per_second
            await asyncio.sleep(min(wait_time, 0.1))


class KalshiHttpClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        rate_limit_per_minute: int = RATE_LIMIT_PER_MINUTE,
    ):
        self.base_url = base_url or BASE_URL
        self.api_key = api_key or API_KEY
        self._client = httpx.AsyncClient(timeout=20.0)
        
        # Rate limiting: convert per-minute to per-second
        refill_rate_per_second = rate_limit_per_minute / 60.0
        self.rate_limiter = TokenBucket(
            capacity=rate_limit_per_minute,
            refill_rate_per_second=refill_rate_per_second
        )
        self.rate_limit_per_minute = rate_limit_per_minute

    def _headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        base_backoff: float = 1.0,
    ) -> Dict[str, Any]:
        """Make HTTP request with rate limiting and exponential backoff retry.
        
        Args:
            method: "GET" or "POST"
            url: full URL to request
            params: query parameters
            max_retries: number of retry attempts on transient failures
            base_backoff: initial backoff duration in seconds
        
        Returns:
            Parsed JSON response.
        
        Raises:
            httpx.HTTPError if all retries fail.
        """
        # Acquire a token from the rate limiter
        await self.rate_limiter.acquire(1)
        
        for attempt in range(max_retries):
            try:
                r = await self._client.request(
                    method,
                    url,
                    params=params,
                    headers=self._headers()
                )
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                # 429 (rate limit), 502, 503, 504 are transient; retry
                if e.response.status_code in (429, 502, 503, 504):
                    if attempt < max_retries - 1:
                        backoff = base_backoff * (2 ** attempt)
                        logger.warning(
                            f"Transient HTTP {e.response.status_code}; "
                            f"retrying in {backoff:.1f}s (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(backoff)
                        continue
                # Non-transient error or last retry failed
                logger.error(f"HTTP {e.response.status_code}: {e}")
                raise
            except (httpx.ConnectError, httpx.TimeoutException, asyncio.TimeoutError) as e:
                # Network errors are transient
                if attempt < max_retries - 1:
                    backoff = base_backoff * (2 ** attempt)
                    logger.warning(
                        f"Network error: {type(e).__name__}; "
                        f"retrying in {backoff:.1f}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(backoff)
                    continue
                logger.error(f"Network error after retries: {e}")
                raise

    async def get_markets(
        self,
        limit: int = 1000,
        cursor: Optional[str] = None,
        mve_filter: str = "exclude",
        min_created_ts: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch markets with cursor-based pagination.
        
        Args:
            limit: number of markets per page (default 1000)
            cursor: pagination cursor from previous response
            mve_filter: "exclude" (default) to skip multi-leg parlays, "include" to show all
            min_created_ts: (optional) ISO 8601 timestamp; only return markets created after this time
        
        Returns:
            Response dict with "markets" array and "cursor" for next page.
        """
        url = f"{self.base_url}/markets"
        params = {"limit": limit, "mve_filter": mve_filter}
        if cursor:
            params["cursor"] = cursor
        if min_created_ts:
            params["min_created_ts"] = min_created_ts
        return await self._request_with_retry("GET", url, params=params)

    async def get_events(
        self,
        limit: int = 200,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fetch events with cursor-based pagination.
        
        Args:
            limit: number of events per page (default 200)
            cursor: pagination cursor from previous response
        
        Returns:
            Response dict with "events" array and "cursor" for next page.
        """
        url = f"{self.base_url}/events"
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        return await self._request_with_retry("GET", url, params=params)

    async def get_orderbook(self, ticker: str) -> Dict[str, Any]:
        """Fetch orderbook snapshot for a market (on-demand only)."""
        url = f"{self.base_url}/markets/{ticker}/orderbook"
        return await self._request_with_retry("GET", url)

    async def close(self) -> None:
        await self._client.aclose()


__all__ = ["KalshiHttpClient"]

