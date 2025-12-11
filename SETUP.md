# Setup & Initialization

This guide walks you through setting up the prediction markets platform locally.

## 1. Install Dependencies

### System Dependencies

**macOS:**
```bash
brew install postgresql
brew services start postgresql
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get install postgresql postgresql-contrib
sudo service postgresql start
```

### Python Dependencies

Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# or
.venv\Scripts\activate  # Windows
```

Install Python packages:
```bash
pip install -r requirements.txt
```

## 2. Set Up the Database

### Create the Database

Connect to Postgres and create the `prediction_markets` database:
```bash
psql -U postgres
```

Then in the psql prompt:
```sql
CREATE DATABASE prediction_markets;
\q
```

Or one-liner (if postgres has no password):
```bash
createdb prediction_markets
```

### Initialize the Schema

The schema is automatically created on first app startup via `backend/common/db.py:create_tables()`. However, you can also initialize it manually:

```bash
python -c "
import asyncio
from backend.common.db import create_tables

asyncio.run(create_tables())
print('✓ Database tables created successfully')
"
```

Or use a simple Python script:
```bash
cat > init_db.py << 'EOF'
#!/usr/bin/env python3
import asyncio
from backend.common.db import create_tables

async def main():
    await create_tables()
    print("✓ Database tables created successfully")

if __name__ == "__main__":
    asyncio.run(main())
EOF

python init_db.py
```

### Verify the Schema

Connect to your database and verify tables exist:
```bash
psql -d prediction_markets
```

Then in psql:
```sql
\d+ markets
\d+ events
SELECT COUNT(*) FROM markets;
SELECT COUNT(*) FROM events;
\q
```

## 3. Environment Configuration

### Copy .env.example to .env

```bash
cp .env.example .env
```

Edit `.env` with your actual values:
```bash
# .env (do NOT commit this file; it's in .gitignore)
DATABASE_URL=postgresql://james:@localhost:5432/prediction_markets
KALSHI_API_KEY=your-actual-api-key-here
INGEST_POLL_INTERVAL=60
INGEST_MIN_CREATED_TS=
INGEST_RATE_LIMIT_PER_MINUTE=120
```

### Important Environment Variables

- **DATABASE_URL** — PostgreSQL connection string.
  - Format: `postgresql://[user]:[password]@[host]:[port]/[database]`
  - For local Postgres without password: `postgresql://postgres@localhost:5432/prediction_markets`
  
- **KALSHI_API_KEY** — (Optional) Your Kalshi API key for authenticated endpoints.
  - Leave blank if using unauthenticated (limited rate limits).
  
- **KALSHI_BASE_URL** — Kalshi API base URL (default: `https://api.elections.kalshi.com/trade-api/v2`).

- **INGEST_POLL_INTERVAL** — How often (in seconds) to poll Kalshi API (default: `60`).

- **INGEST_MIN_CREATED_TS** — (Optional) ISO 8601 timestamp to filter for markets created after this time.
  - Example: `2025-12-01T00:00:00Z`
  - Leave blank to ingest all markets.

- **INGEST_RATE_LIMIT_PER_MINUTE** — Max HTTP requests per minute (default: `120`).

## 4. Run the Application

Load environment variables and start the FastAPI server:

```bash
source .venv/bin/activate
export $(cat .env | xargs)  # Load .env into environment
uvicorn backend.api.main:app --reload --port 8000
```

Or simpler, using `python-dotenv` (which is already used in the app):

```bash
source .venv/bin/activate
uvicorn backend.api.main:app --reload --port 8000
```

The server should start at http://localhost:8000.

- Health check: `curl http://localhost:8000/health`
- Swagger docs: http://localhost:8000/docs

## 5. Monitor Ingestion

Once the app is running, the background ingestion task starts automatically. Watch the logs for:

```
INFO:auto_ingest:Ingested 50 markets
INFO:auto_ingest:Ingested 10 events
```

Query the database to see data:
```bash
psql -d prediction_markets -c "
SELECT COUNT(*) as market_count FROM markets;
SELECT COUNT(*) as event_count FROM events;
"
```

## 6. Docker Setup (Optional)

If using Docker for Postgres/TimescaleDB:

```bash
docker-compose up -d redis timescaledb
```

Verify services are running:
```bash
docker-compose ps
```

## Troubleshooting

### Database Connection Error

```
ERROR: could not connect to database server: No such file or directory
```

**Solution:**
- Ensure Postgres is running: `brew services list` (macOS) or `sudo service postgresql status` (Linux).
- Check DATABASE_URL format matches your Postgres setup.
- Try connecting directly: `psql -d prediction_markets` (if auth-free).

### Schema Doesn't Exist

```
relation "markets" does not exist
```

**Solution:**
- Run the init script manually (see Section 2, "Initialize the Schema").

### API Key Issues

If you see rate-limit errors from Kalshi, check:
- KALSHI_API_KEY is set correctly in `.env`.
- INGEST_RATE_LIMIT_PER_MINUTE is not too aggressive.

### Port Already in Use

If port 8000 is taken:
```bash
uvicorn backend.api.main:app --reload --port 8001
```

## Next Steps

- Check `ingestion.md` for details on the ingestion process.
- Review `backend/ingestion_engine/auto_ingest.py` to customize polling behavior.
- Add tests in `tests/` and run with `pytest`.
