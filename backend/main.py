import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.config import settings
from backend.db.database import engine
from backend.db.timescale import init_schema
from backend.engine.ingestion import IngestionEngine
from backend.routes import prices, candles, system as system_routes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Schema
    logger.info("Initializing TimescaleDB schema...")
    await init_schema()

    # 2. Ingestion engine
    ingestion = IngestionEngine(settings=settings, db_engine=engine)

    if settings.alpaca_api_key and settings.alpaca_api_secret:
        from backend.adapters.alpaca import AlpacaAdapter
        ingestion.register_adapter(
            AlpacaAdapter(
                api_key=settings.alpaca_api_key,
                api_secret=settings.alpaca_api_secret,
                feed=settings.alpaca_feed,
            )
        )
        logger.info("Alpaca adapter registered (feed=%s, symbols=%s)", settings.alpaca_feed, settings.default_symbols)
    else:
        logger.warning(
            "Alpaca credentials not set — live ingestion disabled. "
            "Set ALPACA_API_KEY + ALPACA_API_SECRET in .env to enable."
        )

    system_routes.set_ingestion_engine(ingestion)
    await ingestion.start()
    logger.info("Meridian started")

    yield

    logger.info("Shutting down...")
    await ingestion.stop()


app = FastAPI(
    title="Meridian",
    description="Real-time market data pipeline — Bloomberg Terminal's data layer, built from scratch.",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(prices.router)
app.include_router(candles.router)
app.include_router(system_routes.router)


@app.get("/", tags=["root"])
async def root():
    return {
        "name": "Meridian",
        "version": "0.1.0",
        "phase": "1 — Ingestion + Storage",
        "docs": "/docs",
    }
