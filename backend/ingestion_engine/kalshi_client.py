"""Kalshi ingestion SDK wrapper removed.

Direct SDK-based streaming was removed in favor of an HTTP-based ingester
(see `backend/ingestion_engine/kalshi_http.py` and `auto_ingest.py`).
This module remains as a placeholder and will raise if imported.
"""

raise NotImplementedError("Kalshi SDK-based ingestion support is removed. Use kalshi_http and auto_ingest instead.")
