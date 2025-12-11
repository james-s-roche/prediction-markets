# Prediction Markets - Scaffold

This repository contains a scaffold for a prediction markets trading platform (Kalshi, Polymarket).

Quickstart (development):

1. Create a Python virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. (Optional) Start local infrastructure via Docker Compose:

```bash
docker-compose up -d redis timescaledb
```

3. Run the FastAPI app locally:

```bash
uvicorn backend.api.main:app --reload --port 8000
```

4. Run tests:

```bash
pytest -q
```

Project layout (initial):
- `backend/api/` — FastAPI app and HTTP/WebSocket endpoints
- `backend/ingestion_engine/` — Exchange clients and normalizers
- `backend/execution_engine/` — Order manager and risk manager skeletons
- `backend/common/` — Shared pydantic models and helpers

Next steps:
- Implement real exchange websocket/HTTP clients using `aiohttp`/`httpx`.
- Integrate Redis pub/sub for live message passing and TimescaleDB for archival.
- Add CI, linting, and more complete unit tests.
