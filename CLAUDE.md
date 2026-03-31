# Meridian — Agent Instructions

## Project
Real-time market data pipeline: WebSocket ingestion → TimescaleDB → FastAPI.

## Stack
- FastAPI (async) + uvicorn
- TimescaleDB (Postgres extension) via asyncpg + SQLAlchemy 2.0
- httpx for async HTTP (news RSS, Telegram)
- NumPy/Pandas for indicator math and chart intelligence

## Running locally
```bash
# Start DB
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
- [x] Phase 2: Indicator engine + WebSocket streaming
- [x] Phase 3: Alerts + Coinbase adapter + chart intelligence + news service + portfolios
- [ ] Phase 4: Backtest engine
- [ ] Phase 5: Redis pub/sub fan-out + horizontal scaling
- [ ] Phase 6: Schwab / additional exchange adapters

## Adding a new exchange adapter
1. Create `backend/adapters/<exchange>.py`
2. Subclass `BaseAdapter`, implement `connect/subscribe/disconnect/stream`
3. Register in `backend/main.py` lifespan

## Tests
```bash
pytest backend/tests/ -v
```
