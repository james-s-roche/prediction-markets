"""API package for prediction-markets project.

This package initializer ensures ingestion routes are registered when the
package is imported by importing `backend.api._ingest_routes` lazily. This
avoids modifying `backend/api/main.py` directly and keeps route registration
in a separate module.
"""

from importlib import import_module

__all__ = ["main"]

# Attempt to import the ingestion routes so they register on the FastAPI app.
# Import errors are swallowed so the package remains import-safe when the
# ingestion module depends on optional SDKs that may not be installed.
try:
	import_module("backend.api._ingest_routes")
except Exception:
	# If ingestion routes fail to import (missing optional deps), don't break package import.
	pass
