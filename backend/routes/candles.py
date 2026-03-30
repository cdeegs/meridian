import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.database import get_db
from backend.services.candle_history import coverage_looks_complete, merge_candles

router = APIRouter(prefix="/api", tags=["candles"])
logger = logging.getLogger(__name__)
_stock_market_data = None
_crypto_market_data = None

_TIMEFRAME_SPECS = {
    "1m": {"bucket_width": timedelta(minutes=1), "window": timedelta(hours=24)},
    "5m": {"bucket_width": timedelta(minutes=5), "window": timedelta(hours=24)},
    "15m": {"bucket_width": timedelta(minutes=15), "window": timedelta(days=3)},
    "30m": {"bucket_width": timedelta(minutes=30), "window": timedelta(days=5)},
    "1h": {"bucket_width": timedelta(hours=1), "window": timedelta(days=14)},
    "2h": {"bucket_width": timedelta(hours=2), "window": timedelta(days=21)},
    "4h": {"bucket_width": timedelta(hours=4), "window": timedelta(days=60)},
    "6h": {"bucket_width": timedelta(hours=6), "window": timedelta(days=90)},
    "12h": {"bucket_width": timedelta(hours=12), "window": timedelta(days=180)},
    "1d": {"bucket_width": timedelta(days=1), "window": timedelta(days=365)},
    "2d": {"bucket_width": timedelta(days=2), "window": timedelta(days=730)},
    "1w": {"bucket_width": timedelta(days=7), "window": timedelta(days=1825)},
}


def set_stock_market_data_client(client) -> None:
    global _stock_market_data
    _stock_market_data = client


def set_alpaca_market_data_client(client) -> None:
    set_stock_market_data_client(client)


def set_crypto_market_data_client(client) -> None:
    global _crypto_market_data
    _crypto_market_data = client


def set_coinbase_market_data_client(client) -> None:
    set_crypto_market_data_client(client)


def _is_equity_symbol(symbol: str) -> bool:
    return "-" not in symbol and "/" not in symbol


@router.get("/candles/{symbol}")
async def get_candles(
    symbol: str,
    timeframe: str = Query(default="1m", description="1m | 5m | 15m | 30m | 1h | 2h | 4h | 6h | 12h | 1d | 2d | 1w"),
    start: Optional[datetime] = Query(default=None, description="ISO8601 start time (UTC)"),
    end: Optional[datetime] = Query(default=None, description="ISO8601 end time (UTC)"),
    limit: int = Query(default=200, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    timeframe_spec = _TIMEFRAME_SPECS.get(timeframe)
    if timeframe_spec is None:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid timeframe '{timeframe}'. Valid: {', '.join(_TIMEFRAME_SPECS)}",
        )

    now = datetime.now(timezone.utc)
    if end is None:
        end = now
    if start is None:
        start = end - timeframe_spec["window"]
    if start > end:
        raise HTTPException(status_code=400, detail="start must be earlier than end")

    normalized_symbol = symbol.upper()

    if _stock_market_data is not None and _is_equity_symbol(normalized_symbol):
        try:
            bars = await _stock_market_data.fetch_bars(
                symbol=normalized_symbol,
                timeframe=timeframe,
                start=start,
                end=end,
                limit=limit,
            )
        except Exception as exc:
            logger.warning("Unable to fetch stock bars for %s: %s", normalized_symbol, exc)
            bars = []

        if bars:
            return {
                "symbol": normalized_symbol,
                "timeframe": timeframe,
                "candles": bars[-limit:],
            }

    db_candles = await _load_db_candles(
        symbol=normalized_symbol,
        start=start,
        end=end,
        limit=limit,
        bucket_width=timeframe_spec["bucket_width"],
        db=db,
    )

    if _crypto_market_data is not None and not _is_equity_symbol(normalized_symbol):
        provider_candles = []
        if not coverage_looks_complete(
            db_candles,
            start=start,
            end=end,
            bucket_width=timeframe_spec["bucket_width"],
            limit=limit,
        ):
            try:
                provider_candles = await _crypto_market_data.fetch_bars(
                    symbol=normalized_symbol,
                    timeframe=timeframe,
                    start=start,
                    end=end,
                    limit=limit,
                )
            except Exception as exc:
                logger.warning("Unable to fetch crypto bars for %s: %s", normalized_symbol, exc)
                provider_candles = []

        merged_candles = merge_candles(db_candles, provider_candles)
        if merged_candles:
            return {
                "symbol": normalized_symbol,
                "timeframe": timeframe,
                "candles": merged_candles[-limit:],
            }

    if not db_candles:
        raise HTTPException(status_code=404, detail=f"No candle data for {normalized_symbol} in requested range")

    return {
        "symbol": normalized_symbol,
        "timeframe": timeframe,
        "candles": db_candles,
    }


async def _load_db_candles(
    symbol: str,
    start: datetime,
    end: datetime,
    limit: int,
    bucket_width: timedelta,
    db: AsyncSession,
) -> list[dict]:
    result = await db.execute(
        text("""
            WITH bucketed AS (
                SELECT
                    date_bin(
                        :bucket_width,
                        time,
                        TIMESTAMPTZ '2001-01-01 00:00:00+00'
                    ) AS bucket,
                    time,
                    price,
                    volume,
                    row_number() OVER (
                        PARTITION BY date_bin(
                            :bucket_width,
                            time,
                            TIMESTAMPTZ '2001-01-01 00:00:00+00'
                        )
                        ORDER BY time ASC
                    ) AS rn_open,
                    row_number() OVER (
                        PARTITION BY date_bin(
                            :bucket_width,
                            time,
                            TIMESTAMPTZ '2001-01-01 00:00:00+00'
                        )
                        ORDER BY time DESC
                    ) AS rn_close
                FROM ticks
                WHERE symbol = :symbol
                  AND time >= :start
                  AND time <= :end
            ),
            aggregated AS (
                SELECT
                    bucket,
                    max(CASE WHEN rn_open = 1 THEN price END) AS open,
                    max(price)                                AS high,
                    min(price)                                AS low,
                    max(CASE WHEN rn_close = 1 THEN price END) AS close,
                    sum(volume)                               AS volume,
                    count(*)                                  AS ticks
                FROM bucketed
                GROUP BY bucket
            )
            SELECT bucket, open, high, low, close, volume, ticks
            FROM (
                SELECT *
                FROM aggregated
                ORDER BY bucket DESC
                LIMIT :limit
            ) recent
            ORDER BY bucket ASC
        """),
        {
            "symbol": symbol,
            "start": start,
            "end": end,
            "limit": limit,
            "bucket_width": bucket_width,
        },
    )
    rows = result.fetchall()
    return [
        {
            "time": row.bucket,
            "open": float(row.open),
            "high": float(row.high),
            "low": float(row.low),
            "close": float(row.close),
            "volume": float(row.volume or 0.0),
            "ticks": row.ticks,
        }
        for row in rows
        if row.open is not None and row.close is not None
    ]
