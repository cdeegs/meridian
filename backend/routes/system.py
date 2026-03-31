import logging
from fastapi import APIRouter
from sqlalchemy import text
from backend.db.database import engine

router = APIRouter(prefix="/api", tags=["system"])
logger = logging.getLogger(__name__)

# Set by main.py after ingestion engine is started
_ingestion_engine = None


def set_ingestion_engine(engine_instance) -> None:
    global _ingestion_engine
    _ingestion_engine = engine_instance


@router.get("/health")
async def health():
    """Overall system health: DB connectivity + feed status."""
    db_ok = False
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
            db_ok = True
    except Exception as e:
        logger.warning("DB health check failed: %s", e)

    feeds = _ingestion_engine.get_feed_status() if _ingestion_engine else {}
    all_feeds_ok = all(f.get("connected") for f in feeds.values()) if feeds else False

    return {
        "status": "ok" if db_ok else "degraded",
        "feeds_status": "ok" if all_feeds_ok else "degraded",
        "database": "connected" if db_ok else "disconnected",
        "feeds_ok": all_feeds_ok,
        "feeds": feeds,
    }


@router.get("/feeds/status")
async def feeds_status():
    """Per-source connection status, last tick time, and latency."""
    if not _ingestion_engine:
        return {"feeds": {}}
    return {"feeds": _ingestion_engine.get_feed_status()}
