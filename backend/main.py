import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from backend.config import settings
from backend.db.database import engine
from backend.db.timescale import init_schema
from backend.engine.indicator_engine import IndicatorEngine
from backend.engine.ingestion import IngestionEngine
from backend.services.alpaca_market_data import AlpacaMarketDataClient
from backend.services.alert_engine import AlertEngine
from backend.services.coinbase_market_data import CoinbaseMarketDataClient
from backend.services.study_profile_alerts import StudyProfileAlertService
from backend.services.schwab_auth import SchwabOAuthClient, SchwabTokenStore
from backend.services.schwab_market_data import SchwabMarketDataClient
from backend.services.telegram_notifier import TelegramNotifier
from backend.services.warmup import warm_up
from backend.websocket.manager import ConnectionManager, set_manager
from backend.routes import (
    alerts as alerts_routes,
    candles,
    charts,
    dashboard as dashboard_routes,
    indicators,
    portfolios,
    prices,
    schwab as schwab_routes,
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

    notifier = None
    alpaca_market_data = None
    coinbase_market_data = None
    schwab_auth = None
    schwab_market_data = None
    if settings.telegram_configured:
        notifier = TelegramNotifier(
            bot_token=settings.telegram_bot_token,
            chat_id=settings.telegram_chat_id,
        )
        logger.info("Telegram notifier enabled")
    elif settings.telegram_enabled:
        logger.warning("Telegram notifier enabled but missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")

    # 4. Alert engine (DB-backed definitions, in-memory evaluation)
    alert_engine = AlertEngine(engine, notifier=notifier)
    await alert_engine.load_active_alerts()
    alerts_routes.set_alert_engine(alert_engine)
    study_profile_alert_service = StudyProfileAlertService(engine, notifier=notifier)
    await study_profile_alert_service.load_active_alerts()
    alerts_routes.set_study_profile_alert_service(study_profile_alert_service)

    # 5. Ingestion engine — wired to indicator engine + alert engine + broadcaster
    ingestion = IngestionEngine(
        settings=settings,
        db_engine=engine,
        indicator_engine=indicator_engine,
        alert_engine=alert_engine,
        study_profile_alert_service=study_profile_alert_service,
        broadcaster=ws_manager,
    )

    if settings.alpaca_api_key and settings.alpaca_api_secret:
        alpaca_market_data = AlpacaMarketDataClient(
            api_key=settings.alpaca_api_key,
            api_secret=settings.alpaca_api_secret,
            feed=settings.alpaca_feed,
            stock_symbols=settings.default_symbols,
        )
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
        coinbase_market_data = CoinbaseMarketDataClient()
        from backend.adapters.coinbase import CoinbaseAdapter
        ingestion.register_adapter(CoinbaseAdapter(), symbols=settings.coinbase_symbols)
        logger.info("Coinbase adapter registered (symbols=%s)", settings.coinbase_symbols)
    else:
        logger.info("Coinbase adapter disabled")

    if settings.schwab_configured:
        token_store = SchwabTokenStore(settings.schwab_token_path)
        schwab_auth = SchwabOAuthClient(
            client_id=settings.schwab_client_id,
            client_secret=settings.schwab_client_secret,
            redirect_uri=settings.schwab_redirect_uri,
            authorize_url=settings.schwab_authorize_url,
            token_url=settings.schwab_token_url,
            token_store=token_store,
            scope=settings.schwab_scope,
        )
        schwab_market_data = SchwabMarketDataClient(
            auth_client=schwab_auth,
            base_url=settings.schwab_market_data_base_url,
            stock_symbols=settings.default_symbols,
        )
        logger.info("Schwab auth scaffold configured")
    else:
        logger.info("Schwab credentials not set — auth scaffold inactive")

    system_routes.set_ingestion_engine(ingestion)
    prices.set_stock_market_data_client(alpaca_market_data)
    candles.set_stock_market_data_client(alpaca_market_data)
    candles.set_coinbase_market_data_client(coinbase_market_data)
    charts.set_stock_market_data_client(alpaca_market_data)
    charts.set_coinbase_market_data_client(coinbase_market_data)
    schwab_routes.set_schwab_services(schwab_auth, schwab_market_data)

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
app.include_router(charts.router)
app.include_router(indicators.router)
app.include_router(alerts_routes.router)
app.include_router(portfolios.router)
app.include_router(schwab_routes.router)
app.include_router(system_routes.router)
app.include_router(dashboard_routes.router)
app.include_router(ws_routes.router)


@app.get("/", tags=["root"])
async def root(request: Request):
    ws_scheme = "wss" if request.url.scheme == "https" else "ws"
    websocket_url = f"{ws_scheme}://{request.url.netloc}/ws/stream"
    return {
        "name": "Meridian",
        "version": "0.3.0",
        "phase": "3 — Alerts + WebSocket dashboard",
        "docs": "/docs",
        "websocket": websocket_url,
        "dashboard": "/dashboard",
        "portfolios": "/api/portfolios",
        "schwab_status": "/api/schwab/status",
    }
