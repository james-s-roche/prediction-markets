from datetime import datetime
from typing import AsyncIterator
import os

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from backend.ingestion_engine.auto_ingest import start_ingestion, stop_ingestion

# Load .env file
load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start ingestion on startup
    try:
        # Read poll interval and min_created_ts from env
        poll = int(os.getenv("INGEST_POLL_INTERVAL", "60"))
        min_created = os.getenv("INGEST_MIN_CREATED_TS")
        await start_ingestion(poll_interval=poll, selected_markets=None, min_created_ts=min_created)
    except Exception:
        pass
    yield
    # On shutdown
    try:
        await stop_ingestion()
    except Exception:
        pass


app = FastAPI(title="Prediction Markets API", lifespan=lifespan)


class HealthResponse(BaseModel):
    status: str
    now: datetime


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Health endpoint for quick checks."""
    return HealthResponse(status="ok", now=datetime.utcnow())


@app.websocket("/ws/{market}")
async def websocket_market(websocket: WebSocket, market: str):
    """Simple WebSocket bridge stub.

    In a running system this would subscribe to Redis pubsub for `market` and forward
    normalized market ticks to browser clients.
    """
    await websocket.accept()
    try:
        # Simple stub: send a heartbeat every second.
        while True:
            await websocket.send_json({"market": market, "ts": datetime.utcnow().isoformat()})
            await websocket.receive_text()
    except WebSocketDisconnect:
        return
