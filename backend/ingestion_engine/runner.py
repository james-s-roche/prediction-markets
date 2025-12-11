"""Ingestion runner — deprecated in favor of auto_ingest.

This module is kept for backward compatibility but the SDK-based ingestion
approach is removed. Use `backend/ingestion_engine/auto_ingest.py` for the
current HTTP-based polling approach that runs automatically on app startup.
"""
import asyncio
from typing import Dict, Optional
import logging

logger = logging.getLogger("ingestion.runner")


class IngestionRunner:
    """Deprecated. Kept for backward compatibility."""

    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._clients = {}

    def list_running(self):
        return list(self._tasks.keys())


_runner: Optional[IngestionRunner] = None


def get_runner() -> IngestionRunner:
    global _runner
    if _runner is None:
        _runner = IngestionRunner()
    return _runner


__all__ = ["get_runner", "IngestionRunner"]
