import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import bindparam, text

from backend.config import settings
from backend.db.database import get_db
from backend.utils.symbols import is_equity_symbol as _is_equity_symbol

router = APIRouter(prefix="/api", tags=["prices"])
logger = logging.getLogger(__name__)
_stock_market_data = None
SYMBOL_SNAPSHOT_QUERY = text("""
    SELECT DISTINCT ON (symbol, source)
        symbol,
        source,
        time AS last_tick,
        price AS last_price
    FROM ticks
    WHERE symbol IN :symbols
    ORDER BY symbol, source, time DESC
""").bindparams(bindparam("symbols", expanding=True))


def set_stock_market_data_client(client) -> None:
    global _stock_market_data
    _stock_market_data = client


def set_alpaca_market_data_client(client) -> None:
    set_stock_market_data_client(client)


@router.get("/symbols")
async def list_symbols(db: AsyncSession = Depends(get_db)):
    """List all symbols with data, their source, and time of last tick."""
    merged = {}
    tracked_symbols = list(dict.fromkeys(settings.all_symbols))

    if tracked_symbols:
        result = await db.execute(SYMBOL_SNAPSHOT_QUERY, {"symbols": tracked_symbols})
        rows = result.fetchall()
        merged = {
            (r.symbol, r.source): {
                "symbol": r.symbol,
                "source": r.source,
                "last_tick": r.last_tick,
                "tick_count": 0,
                "last_price": r.last_price,
            }
            for r in rows
        }

    if _stock_market_data is not None:
        try:
            snapshots = await _stock_market_data.fetch_watchlist_snapshots()
            for symbol, snapshot in snapshots.items():
                source = snapshot.get("source", "stock-market-data")
                existing = merged.get((symbol, source), {})
                merged[(symbol, source)] = {
                    "symbol": symbol,
                    "source": source,
                    "last_tick": snapshot.get("timestamp") or existing.get("last_tick"),
                    "tick_count": existing.get("tick_count", 0),
                    "last_price": snapshot.get("price") or existing.get("last_price"),
                }
        except Exception as exc:
            logger.warning("Unable to fetch stock watchlist snapshots: %s", exc)

    return sorted(merged.values(), key=lambda item: (item["symbol"], item["source"]))


@router.get("/prices/{symbol}")
async def get_latest_price(symbol: str, db: AsyncSession = Depends(get_db)):
    """Latest price + bid/ask/spread for a symbol."""
    result = await db.execute(
        text("""
            SELECT time, symbol, price, volume, bid, ask, spread, source
            FROM ticks
            WHERE symbol = :symbol
            ORDER BY time DESC
            LIMIT 1
        """),
        {"symbol": symbol.upper()},
    )
    row = result.fetchone()
    if not row:
        if _stock_market_data is not None and _is_equity_symbol(symbol.upper()):
            try:
                snapshot = await _stock_market_data.fetch_snapshot(symbol.upper())
            except Exception as exc:
                logger.warning("Unable to fetch stock snapshot for %s: %s", symbol.upper(), exc)
                snapshot = None

            if snapshot and snapshot.get("price") is not None:
                return {
                    "symbol": snapshot["symbol"],
                    "price": snapshot["price"],
                    "volume": snapshot.get("volume"),
                    "bid": snapshot.get("bid"),
                    "ask": snapshot.get("ask"),
                    "spread": snapshot.get("spread"),
                    "source": snapshot["source"],
                    "timestamp": snapshot.get("timestamp"),
                }

        raise HTTPException(status_code=404, detail=f"No data for symbol {symbol.upper()}")
    return {
        "symbol": row.symbol,
        "price": row.price,
        "volume": row.volume,
        "bid": row.bid,
        "ask": row.ask,
        "spread": row.spread,
        "source": row.source,
        "timestamp": row.time,
    }
