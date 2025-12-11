"""Automated ingestion routines for Kalshi.

- Periodically polls `/markets` and `/events` endpoints and stores payloads to the database.
- Uses cursor-based pagination as per Kalshi API docs.
- Accumulates rows into batches and performs a single COPY+upsert per batch for efficiency.
- Supports incremental ingestion using `min_created_ts` filter.

This module exposes `start_ingestion` and `stop_ingestion` to control background
polling tasks. It's intended to run inside the FastAPI process as a background task
triggered on startup.
"""
import asyncio
import logging
import os
from typing import Optional

from backend.ingestion_engine.kalshi_http import KalshiHttpClient
from backend.common.db import create_tables, batch_upsert_markets, batch_upsert_events

logger = logging.getLogger("auto_ingest")

_INGEST_TASK: Optional[asyncio.Task] = None

# Default batch size (when to flush accumulated rows)
DEFAULT_BATCH_SIZE = 500


async def _poll_markets_and_events(
    poll_interval: int,
    min_created_ts: Optional[str] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    """Poll Kalshi /markets and /events endpoints and persist to database.
    
    Args:
        poll_interval: how long to sleep between polls (seconds)
        min_created_ts: (optional) ISO 8601 timestamp filter; only ingest markets created after this time
        batch_size: number of rows to accumulate before flushing to DB (default 500)
    """
    client = KalshiHttpClient()
    try:
        await create_tables()
    except Exception as e:
        logger.exception("Error creating DB tables: %s", e)
        return

    while True:
        try:
            # Accumulate markets and events in batches
            markets_batch = []
            events_batch = []

            # Poll markets with cursor-based pagination
            cursor = None
            total_markets = 0
            while True:
                res = await client.get_markets(
                    cursor=cursor,
                    mve_filter="exclude",
                    min_created_ts=min_created_ts,
                )
                markets = res.get("markets") or []
                markets_batch.extend(markets)
                total_markets += len(markets)

                # Flush batch if size exceeded
                if len(markets_batch) >= batch_size:
                    flushed = await batch_upsert_markets(markets_batch)
                    logger.info(f"Flushed {flushed} markets to DB")
                    markets_batch = []

                # Check for next cursor
                cursor = res.get("cursor")
                if not cursor:
                    break

            # Flush remaining markets
            if markets_batch:
                flushed = await batch_upsert_markets(markets_batch)
                logger.info(f"Flushed {flushed} markets to DB")

            logger.info(f"Ingested {total_markets} markets in total")

            # Poll events with cursor-based pagination
            cursor = None
            total_events = 0
            while True:
                res = await client.get_events(cursor=cursor)
                events = res.get("events") or []
                events_batch.extend(events)
                total_events += len(events)

                # Flush batch if size exceeded
                if len(events_batch) >= batch_size:
                    flushed = await batch_upsert_events(events_batch)
                    logger.info(f"Flushed {flushed} events to DB")
                    events_batch = []

                # Check for next cursor
                cursor = res.get("cursor")
                if not cursor:
                    break

            # Flush remaining events
            if events_batch:
                flushed = await batch_upsert_events(events_batch)
                logger.info(f"Flushed {flushed} events to DB")

            logger.info(f"Ingested {total_events} events in total")

            await asyncio.sleep(poll_interval)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Unhandled error in ingestion loop")
            await asyncio.sleep(poll_interval)


async def start_ingestion(
    poll_interval: int = 60,
    selected_markets: Optional[list] = None,
    min_created_ts: Optional[str] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    """Start the ingestion background task.
    
    Args:
        poll_interval: sleep duration between polls (seconds); default 60
        selected_markets: (deprecated; ignored)
        min_created_ts: (optional) ISO 8601 timestamp; only ingest markets created after this time.
                       If not provided, defaults to INGEST_MIN_CREATED_TS environment variable.
                       If still not set, all markets are ingested.
        batch_size: number of rows to accumulate before flushing to DB (default 500)
    """
    global _INGEST_TASK
    if _INGEST_TASK and not _INGEST_TASK.done():
        return

    # Load min_created_ts from env if not provided
    if min_created_ts is None:
        min_created_ts = os.getenv("INGEST_MIN_CREATED_TS")

    if min_created_ts:
        logger.info(f"Incremental ingestion enabled; only markets created after {min_created_ts}")
    else:
        logger.info("Ingesting all markets (no min_created_ts filter)")

    _INGEST_TASK = asyncio.create_task(
        _poll_markets_and_events(poll_interval, min_created_ts=min_created_ts, batch_size=batch_size)
    )


async def stop_ingestion():
    """Stop the ingestion background task."""
    global _INGEST_TASK
    if _INGEST_TASK:
        _INGEST_TASK.cancel()
        try:
            await _INGEST_TASK
        except asyncio.CancelledError:
            pass
        _INGEST_TASK = None


__all__ = ["start_ingestion", "stop_ingestion"]


