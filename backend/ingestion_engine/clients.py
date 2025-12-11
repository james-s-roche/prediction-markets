"""Stubs for exchange websocket/HTTP clients.

These clients are intentionally minimal — they document the expected interfaces and
provide a starting point for implementing real exchange integrations.
"""
from typing import Callable, Optional


class WebsocketClient:
    """Minimal websocket client interface for an exchange.

    In practice, implementations should be fully async and resilient: automatic
    reconnects, backpressure handling, and exponential backoff.
    """

    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self._running = False
        self.on_message: Optional[Callable[[dict], None]] = None

    async def connect(self) -> None:
        """Connect to the exchange websocket and start delivering messages to
        `self.on_message` callback.

        This is a stub: replace with `aiohttp`/`websockets` implementation.
        """
        self._running = True
        # TODO: implement actual websocket handling
        return

    async def disconnect(self) -> None:
        self._running = False


class RestClient:
    """Minimal HTTP client interface for snapshot retrieval."""

    def __init__(self, name: str, base_url: str):
        self.name = name
        self.base_url = base_url

    async def fetch_snapshot(self, market: str) -> dict:
        """Fetch a full order book snapshot for the given market.

        Replace with an `httpx` or `aiohttp` implementation.
        """
        raise NotImplementedError
