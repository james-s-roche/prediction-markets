"""Extra API routes for Kalshi endpoints.

These routes provide direct HTTP access to Kalshi market/event data via
the internal HTTP client (`backend/ingestion_engine/kalshi_http.py`).

Automated ingestion that populates the database happens in the background and
is managed by `backend/ingestion_engine/auto_ingest.py`.
"""
from typing import List

from fastapi import HTTPException

from backend.api.main import app
from backend.ingestion_engine.kalshi_http import KalshiHttpClient


@app.get("/ingest/list")
async def list_ingest() -> List[str]:
    """List active background ingestion tasks (deprecated; for backward compatibility)."""
    return []


@app.get("/kalshi/markets")
async def kalshi_markets(cursor: str = None, limit: int = 1000):
    """List markets via Kalshi HTTP API with cursor-based pagination.
    
    Query params:
    - cursor: pagination cursor from previous response
    - limit: number of markets to return (default 1000)
    - mve_filter: "exclude" (default) to skip parlays, "include" for all
    """
    client = KalshiHttpClient()
    try:
        result = await client.get_markets(cursor=cursor, limit=limit, mve_filter="exclude")
        await client.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Kalshi API error: {str(e)}")


@app.get("/kalshi/events")
async def kalshi_events(cursor: str = None, limit: int = 200):
    """List events via Kalshi HTTP API with cursor-based pagination.
    
    Query params:
    - cursor: pagination cursor from previous response
    - limit: number of events to return (default 200)
    """
    client = KalshiHttpClient()
    try:
        result = await client.get_events(cursor=cursor, limit=limit)
        await client.close()
        return result
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Kalshi API error: {str(e)}")

