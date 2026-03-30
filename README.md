# Meridian

Real-time market data pipeline for finance and fintech demos.

Meridian ingests live market data, stores time-series ticks in Postgres, computes technical indicators in memory, streams updates over WebSockets, and now includes a lightweight browser dashboard with alerting.

## What It Does

- Ingests live market data from:
  - Coinbase Advanced Trade public WebSocket feed
  - Alpaca WebSocket feed when API keys are configured
- Falls back to Alpaca snapshots and historical bars for stocks, so equity price cards and charts can still populate without waiting for local tick history to build up
- Stores raw ticks plus computed indicators
- Computes:
  - `sma_20`, `sma_50`
  - `ema_12`, `ema_26`
  - `rsi_14`
  - `macd`
  - `bollinger_20`
  - `vwap`
- Streams live updates over `/ws/stream`
- Supports alert rules for:
  - `price_above`
  - `price_below`
  - `rsi_above`
  - `rsi_below`
  - `macd_cross_up`
  - `macd_cross_down`
- Can send triggered alerts to Telegram when configured
- Ships with a demo dashboard at `/dashboard`

## Current Stack

- FastAPI
- SQLAlchemy + asyncpg
- Postgres / TimescaleDB
- WebSockets
- NumPy
- Plain HTML/CSS/JS dashboard

## Repo Layout

```text
meridian/
├── backend/
│   ├── adapters/          # Exchange adapters
│   ├── db/                # DB engine + schema init
│   ├── engine/            # Ingestion + indicator pipeline
│   ├── indicators/        # Stateful indicator implementations
│   ├── routes/            # REST + dashboard + websocket routes
│   ├── services/          # Warmup + alert engine
│   ├── static/            # Dashboard UI
│   ├── tests/             # Pytest coverage
│   └── websocket/         # Connection manager
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Quick Start

### Option A: Docker / TimescaleDB

If you have Docker installed:

```bash
cp .env.example .env
docker compose up -d
uvicorn backend.main:app --reload
```

Open:

- API docs: `http://127.0.0.1:8000/docs`
- Dashboard: `http://127.0.0.1:8000/dashboard`

### Option B: Local Postgres Without Docker

Meridian now falls back to standard Postgres for local development if the TimescaleDB extension is not installed.

1. Make sure Postgres is running on `localhost:5432`
2. Create a local database, for example:

```bash
createdb meridian
```

3. Create `.env` and set a local database URL. Example:

```env
DATABASE_URL=postgresql+asyncpg://YOUR_LOCAL_USERNAME@localhost:5432/meridian
REDIS_URL=redis://localhost:6379/0
ALPACA_API_KEY=
ALPACA_API_SECRET=
ALPACA_FEED=iex
SCHWAB_CLIENT_ID=
SCHWAB_CLIENT_SECRET=
SCHWAB_REDIRECT_URI=http://127.0.0.1:8765/schwab/callback
SCHWAB_SCOPE=
SCHWAB_TOKEN_PATH=.schwab_tokens.json
DEFAULT_SYMBOLS=["SPY","QQQ","IWM","DIA","AAPL","MSFT","NVDA","AMZN","META","TSLA"]
COINBASE_ENABLED=true
COINBASE_SYMBOLS=["BTC-USD","ETH-USD","SOL-USD"]
TELEGRAM_ENABLED=false
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
BATCH_INTERVAL_MS=100
HEARTBEAT_TIMEOUT_S=30
```

4. Start the app from the repo root:

```bash
uvicorn backend.main:app --reload
```

5. To enable live stocks with Alpaca, add your market data credentials:

```env
ALPACA_API_KEY=your_key
ALPACA_API_SECRET=your_secret
ALPACA_FEED=iex
```

`ALPACA_FEED=iex` is the right default for the free plan.

6. If you are waiting on Schwab approval, Meridian now includes a Schwab OAuth scaffold. Once the app is approved, add:

```env
SCHWAB_CLIENT_ID=your_app_key
SCHWAB_CLIENT_SECRET=your_app_secret
SCHWAB_REDIRECT_URI=http://127.0.0.1:8765/schwab/callback
```

Then use:

- `GET /api/schwab/status` to check config/token status
- `GET /api/schwab/auth/url` to generate the login URL
- `GET /api/schwab/auth/start` to begin the OAuth flow
- `GET /schwab/callback` as the local redirect target

## Running Tests

```bash
pytest backend/tests -q
```

## Main Endpoints

- `GET /` — service metadata
- `GET /api/health` — DB + feed status
- `GET /api/feeds/status` — per-feed connection info
- `GET /api/schwab/status` — Schwab auth scaffold status
- `GET /api/schwab/auth/url` — generate Schwab authorization URL
- `GET /api/schwab/auth/start` — redirect into Schwab login flow
- `POST /api/schwab/auth/refresh` — refresh stored Schwab tokens
- `POST /api/schwab/auth/disconnect` — clear stored Schwab tokens
- `GET /api/symbols` — latest symbols with last price and tick count
- `GET /api/prices/{symbol}` — latest price snapshot
- `GET /api/candles/{symbol}` — OHLCV candles
- `GET /api/charts/{symbol}` — candle + indicator chart payload for selected timeframe
- `GET /api/indicators/{symbol}` — latest indicators
- `GET /api/indicators/{symbol}/history` — indicator history
- `GET /api/alerts` — list alerts
- `POST /api/alerts` — create alert
- `POST /api/alerts/{id}/activate` — reactivate alert
- `POST /api/alerts/{id}/disable` — disable alert
- `GET /dashboard` — live dashboard
- `WS /ws/stream` — live prices, indicators, and alerts

## Dashboard

The dashboard:

- Subscribes to `*` on `/ws/stream`
- Renders live market cards
- Renders multi-timeframe charts for price, RSI, MACD, volume, VWAP, moving averages, and Bollinger bands
- Loads extra hidden history so indicator values warm up before the visible window, which makes the charts more trustworthy
- Displays recent triggered alerts
- Lets you create, disable, and reactivate alerts from the browser

## Telegram Alerts

To enable Telegram delivery for triggered alerts, set:

```env
TELEGRAM_ENABLED=true
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

Your bot must already exist and be allowed to message the target chat.

## Notes

- Coinbase is enabled by default so the app can stream live data even without Alpaca credentials.
- Alpaca is optional and only activates when `ALPACA_API_KEY` and `ALPACA_API_SECRET` are set.
- When Alpaca is configured, Meridian uses Alpaca snapshots and historical bars to make stock cards and charts useful immediately, even before your own local stock tick database has much history.
- Schwab support is now scaffolded for OAuth and local token storage, so once Schwab approves your developer app the market-data client can be finished without reshaping the rest of the app.
- In plain Postgres mode, Meridian skips Timescale-only features like hypertables and continuous aggregates, but the API and dashboard still work for local development.
