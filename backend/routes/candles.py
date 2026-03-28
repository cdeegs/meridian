from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from backend.db.database import get_db

router = APIRouter(prefix="/api", tags=["candles"])

_TIMEFRAME_INTERVALS = {
    "1m":  "1 minute",
    "5m":  "5 minutes",
    "15m": "15 minutes",
    "1h":  "1 hour",
    "4h":  "4 hours",
    "1d":  "1 day",
}


@router.get("/candles/{symbol}")
async def get_candles(
    symbol: str,
    timeframe: str = Query(default="1m", description="1m | 5m | 15m | 1h | 4h | 1d"),
    start: Optional[datetime] = Query(default=None, description="ISO8601 start time (UTC)"),
    end: Optional[datetime] = Query(default=None, description="ISO8601 end time (UTC)"),
    limit: int = Query(default=200, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    OHLCV candles for a symbol.

    Uses TimescaleDB time_bucket() — no materialized view needed for arbitrary timeframes.
    Default: last 24 hours at 1-minute resolution.
    """
    if timeframe not in _TIMEFRAME_INTERVALS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Valid: {', '.join(_TIMEFRAME_INTERVALS)}",
        )

    now = datetime.now(timezone.utc)
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=24)

    interval = _TIMEFRAME_INTERVALS[timeframe]

    result = await db.execute(
        text(f"""
            SELECT
                time_bucket('{interval}', time) AS bucket,
                first(price, time)  AS open,
                max(price)          AS high,
                min(price)          AS low,
                last(price, time)   AS close,
                sum(volume)         AS volume,
                count(*)            AS ticks
            FROM ticks
            WHERE symbol = :symbol
              AND time >= :start
              AND time <= :end
            GROUP BY bucket
            ORDER BY bucket DESC
            LIMIT :limit
        """),
        {"symbol": symbol.upper(), "start": start, "end": end, "limit": limit},
    )
    rows = result.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No candle data for {symbol.upper()} in requested range")

    return {
        "symbol": symbol.upper(),
        "timeframe": timeframe,
        "candles": [
            {
                "time": row.bucket,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
                "ticks": row.ticks,
            }
            for row in rows
        ],
    }
