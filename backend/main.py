import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.config import settings
from backend.db.database import engine
from backend.db.timescale import init_schema
from backend.engine.indicator_engine import IndicatorEngine
from backend.engine.ingestion import IngestionEngine
from backend.services.alert_engine import AlertEngine
from backend.services.warmup import warm_up
from backend.websocket.manager import ConnectionManager, set_manager
from backend.routes import (
    alerts as alerts_routes,
    candles,
    dashboard as dashboard_routes,
    indicators,
    prices,
    system as system_routes,
)
from backend.routes import ws as ws_routes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. TimescaleDB schema (hypertables, continuous aggregates)
    logger.info("Initializing TimescaleDB schema...")
    await init_schema()

    # 2. Indicator engine (pure in-memory, stateful per symbol)
    indicator_engine = IndicatorEngine()

    # 3. WebSocket connection manager
    ws_manager = ConnectionManager()
    set_manager(ws_manager)

    # 4. Alert engine (DB-backed definitions, in-memory evaluation)
    alert_engine = AlertEngine(engine)
    await alert_engine.load_active_alerts()
    alerts_routes.set_alert_engine(alert_engine)

    # 5. Ingestion engine — wired to indicator engine + alert engine + broadcaster
    ingestion = IngestionEngine(
        settings=settings,
        db_engine=engine,
        indicator_engine=indicator_engine,
        alert_engine=alert_engine,
        broadcaster=ws_manager,
    )

    if settings.alpaca_api_key and settings.alpaca_api_secret:
        from backend.adapters.alpaca import AlpacaAdapter
        ingestion.register_adapter(
            AlpacaAdapter(
                api_key=settings.alpaca_api_key,
                api_secret=settings.alpaca_api_secret,
                feed=settings.alpaca_feed,
            ),
            symbols=settings.default_symbols,
        )
        logger.info(
            "Alpaca adapter registered (feed=%s, symbols=%s)",
            settings.alpaca_feed,
            settings.default_symbols,
        )
    else:
        logger.warning(
            "Alpaca credentials not set — live ingestion disabled. "
            "Set ALPACA_API_KEY + ALPACA_API_SECRET in .env to enable."
        )

    if settings.coinbase_enabled:
        from backend.adapters.coinbase import CoinbaseAdapter
        ingestion.register_adapter(CoinbaseAdapter(), symbols=settings.coinbase_symbols)
        logger.info("Coinbase adapter registered (symbols=%s)", settings.coinbase_symbols)
    else:
        logger.info("Coinbase adapter disabled")

    system_routes.set_ingestion_engine(ingestion)

    # 6. Warm up indicator windows from historical data before going live
    logger.info("Warming up indicator windows from DB history...")
    await warm_up(engine, indicator_engine, settings.all_symbols)

    # 7. Start ingestion (adapters + batch writer)
    await ingestion.start()
    logger.info("Meridian started — Phase 3 foundation (alerts + demo dashboard)")

    yield

    logger.info("Shutting down...")
    await ingestion.stop()


app = FastAPI(
    title="Meridian",
    description="Real-time market data pipeline — Bloomberg Terminal's data layer, built from scratch.",
    version="0.3.0",
    lifespan=lifespan,
)

app.include_router(prices.router)
app.include_router(candles.router)
app.include_router(indicators.router)
app.include_router(alerts_routes.router)
app.include_router(system_routes.router)
app.include_router(dashboard_routes.router)
app.include_router(ws_routes.router)


@app.get("/", tags=["root"])
async def root():
    return {
        "name": "Meridian",
        "version": "0.3.0",
        "phase": "3 — Alerts + WebSocket dashboard",
        "docs": "/docs",
        "websocket": "ws://localhost:8000/ws/stream",
        "dashboard": "/dashboard",
    }
