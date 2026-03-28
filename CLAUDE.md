# Meridian — Agent Instructions

## Project
Real-time market data pipeline: WebSocket ingestion → TimescaleDB → FastAPI.

## Stack
- FastAPI (async) + uvicorn
- TimescaleDB (Postgres extension) via asyncpg + SQLAlchemy 2.0
- Redis for pub/sub (Phase 2+)
- NumPy/Pandas for indicator math

## Running locally
```bash
# Start DB + Redis
docker compose up -d

# Install deps
pip install -r requirements.txt

# Copy env
cp .env.example .env
# Edit .env with your Alpaca credentials

# Run
uvicorn backend.main:app --reload
```

## Key design decisions
- `PriceEvent` is the canonical normalized event — all adapters produce it
- Ingestion engine uses asyncio.Queue + 100ms batch inserts, never blocking the event loop
- TimescaleDB schema initialized on startup via `init_schema()` (autocommit for DDL)
- `create_hypertable` and continuous aggregates run with AUTOCOMMIT isolation
- Each adapter implements `BaseAdapter` — adding a new exchange = one new class

## Phase status
- [x] Phase 1: Ingestion + TimescaleDB + REST API
- [ ] Phase 2: Indicator engine + WebSocket streaming
- [ ] Phase 3: Alerts + Coinbase adapter
- [ ] Phase 4: Backtest engine

## Adding a new exchange adapter
1. Create `backend/adapters/<exchange>.py`
2. Subclass `BaseAdapter`, implement `connect/subscribe/disconnect/stream`
3. Register in `backend/main.py` lifespan

## Tests
```bash
pytest backend/tests/ -v
```
