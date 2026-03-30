"""
Warmup Service — pre-fill indicator rolling windows from historical DB data.

Problem without warmup:
  RSI needs 15 prices before it returns a value. If you start fresh, the first
  15 minutes of live data show no RSI at all. That's bad for a demo.

Solution:
  On startup, query the last N 1-minute candles from TimescaleDB for each
  symbol and replay them through the indicator engine. No DB writes, no
  WebSocket broadcasts — just loading state so live ticks immediately produce
  valid indicator values.

Called in main.py AFTER init_schema() but BEFORE ingestion.start().
"""
import logging
from typing import List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from backend.engine.indicator_engine import IndicatorEngine
from backend.models.price_event import PriceEvent

logger = logging.getLogger(__name__)

# How many 1-minute candles to replay per symbol.
# 300 candles = 5 hours of data — enough to warm up even SMA-200.
_LOOKBACK_CANDLES = 300


async def warm_up(
    db_engine: AsyncEngine,
    indicator_engine: IndicatorEngine,
    symbols: List[str],
) -> None:
    """Replay recent 1-minute candles into indicator state. Safe to call on empty DB."""
    async with db_engine.connect() as conn:
        for symbol in symbols:
            result = await conn.execute(
                text("""
                    WITH bucketed AS (
                        SELECT
                            date_bin(
                                INTERVAL '1 minute',
                                time,
                                TIMESTAMPTZ '2001-01-01 00:00:00+00'
                            ) AS bucket,
                            time,
                            price,
                            volume,
                            row_number() OVER (
                                PARTITION BY date_bin(
                                    INTERVAL '1 minute',
                                    time,
                                    TIMESTAMPTZ '2001-01-01 00:00:00+00'
                                )
                                ORDER BY time DESC
                            ) AS rn_close
                        FROM ticks
                        WHERE symbol = :symbol
                          AND time >= now() - INTERVAL '8 hours'
                    )
                    SELECT
                        bucket,
                        max(CASE WHEN rn_close = 1 THEN price END) AS close,
                        sum(volume)                                AS volume
                    FROM bucketed
                    GROUP BY bucket
                    ORDER BY bucket ASC
                    LIMIT :limit
                """),
                {"symbol": symbol, "limit": _LOOKBACK_CANDLES},
            )
            rows = result.fetchall()

            if not rows:
                logger.info("Warmup: no history for %s — indicators will warm up from live data", symbol)
                continue

            for row in rows:
                fake_event = PriceEvent(
                    symbol=symbol,
                    price=float(row.close),
                    volume=float(row.volume) if row.volume else 0.0,
                    source="warmup",
                    timestamp=row.bucket,
                )
                # Direct call — no queue, no DB write, no broadcast
                indicator_engine.compute_event(fake_event)

            logger.info("Warmed up %s with %d candles", symbol, len(rows))

    logger.info("Warmup complete for %d symbols", len(symbols))
