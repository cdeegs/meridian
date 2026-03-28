from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from backend.db.database import get_db

router = APIRouter(prefix="/api", tags=["prices"])


@router.get("/symbols")
async def list_symbols(db: AsyncSession = Depends(get_db)):
    """List all symbols with data, their source, and time of last tick."""
    result = await db.execute(text("""
        SELECT
            symbol,
            source,
            max(time)   AS last_tick,
            count(*)    AS tick_count,
            last(price, time) AS last_price
        FROM ticks
        GROUP BY symbol, source
        ORDER BY symbol
    """))
    rows = result.fetchall()
    return [
        {
            "symbol": r.symbol,
            "source": r.source,
            "last_tick": r.last_tick,
            "tick_count": r.tick_count,
            "last_price": r.last_price,
        }
        for r in rows
    ]


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
