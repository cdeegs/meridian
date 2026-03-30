from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from backend.db.database import get_db

router = APIRouter(prefix="/api", tags=["indicators"])


@router.get("/indicators/{symbol}")
async def get_latest_indicators(
    symbol: str,
    indicators: Optional[str] = Query(
        default=None,
        description="Comma-separated filter, e.g. rsi_14,macd,bollinger_20",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Latest computed value for every indicator for a symbol.

    Returns the most recent value per indicator name.
    Filter with ?indicators=rsi_14,macd to get specific ones.
    """
    params: dict = {"symbol": symbol.upper()}
    indicator_clause = ""

    if indicators:
        names = [n.strip() for n in indicators.split(",") if n.strip()]
        indicator_clause = "AND indicator = ANY(:indicators)"
        params["indicators"] = names

    result = await db.execute(
        text(f"""
            SELECT DISTINCT ON (indicator)
                time, indicator, value
            FROM indicators
            WHERE symbol = :symbol {indicator_clause}
            ORDER BY indicator, time DESC
        """),
        params,
    )
    rows = result.fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No indicator data for {symbol.upper()}. "
                   "Data accumulates after the first few minutes of live ingestion.",
        )

    return {
        "symbol": symbol.upper(),
        "indicators": {
            row.indicator: {
                "value": row.value,
                "timestamp": row.time,
            }
            for row in rows
        },
    }


@router.get("/indicators/{symbol}/history")
async def get_indicator_history(
    symbol: str,
    indicator: str = Query(..., description="Indicator name, e.g. rsi_14"),
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    Time series of a single indicator for charting.

    Returns values ordered newest-first. Use `start` and `end` for a specific
    window, or just `limit` to get the last N values.
    """
    now = datetime.now(timezone.utc)
    if end is None:
        end = now
    if start is None:
        start = end - timedelta(hours=24)

    result = await db.execute(
        text("""
            SELECT time, value
            FROM indicators
            WHERE symbol    = :symbol
              AND indicator = :indicator
              AND time      >= :start
              AND time      <= :end
            ORDER BY time DESC
            LIMIT :limit
        """),
        {
            "symbol": symbol.upper(),
            "indicator": indicator,
            "start": start,
            "end": end,
            "limit": limit,
        },
    )
    rows = result.fetchall()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No history for indicator '{indicator}' on {symbol.upper()} in requested range.",
        )

    return {
        "symbol": symbol.upper(),
        "indicator": indicator,
        "data": [{"time": row.time, "value": row.value} for row in rows],
    }
