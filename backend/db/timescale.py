"""
TimescaleDB schema initialization.

Runs on app startup. Uses AUTOCOMMIT for DDL that can't run inside a transaction
(create_hypertable, continuous aggregates, retention policies).
"""
import logging
from sqlalchemy import text
from backend.db.database import engine

logger = logging.getLogger(__name__)

# Statements that require AUTOCOMMIT (TimescaleDB DDL)
_AUTOCOMMIT_STMTS = [
    "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE",
    """
    CREATE TABLE IF NOT EXISTS ticks (
        time        TIMESTAMPTZ NOT NULL,
        symbol      TEXT NOT NULL,
        price       DOUBLE PRECISION NOT NULL,
        volume      DOUBLE PRECISION,
        bid         DOUBLE PRECISION,
        ask         DOUBLE PRECISION,
        spread      DOUBLE PRECISION,
        source      TEXT NOT NULL
    )
    """,
    "SELECT create_hypertable('ticks', 'time', if_not_exists => TRUE)",
    """
    CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time
        ON ticks (symbol, time DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS indicators (
        time        TIMESTAMPTZ NOT NULL,
        symbol      TEXT NOT NULL,
        timeframe   TEXT NOT NULL,
        indicator   TEXT NOT NULL,
        value       JSONB NOT NULL
    )
    """,
    "SELECT create_hypertable('indicators', 'time', if_not_exists => TRUE)",
    """
    CREATE INDEX IF NOT EXISTS idx_indicators_symbol_indicator_time
        ON indicators (symbol, indicator, time DESC)
    """,
    # Continuous aggregate for 1-minute candles
    """
    CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1m
    WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 minute', time) AS bucket,
        symbol,
        first(price, time)  AS open,
        max(price)          AS high,
        min(price)          AS low,
        last(price, time)   AS close,
        sum(volume)         AS volume
    FROM ticks
    GROUP BY bucket, symbol
    WITH NO DATA
    """,
    # Auto-refresh: keep last 2 hours up to date, refresh every minute
    """
    SELECT add_continuous_aggregate_policy(
        'candles_1m',
        start_offset     => INTERVAL '2 hours',
        end_offset       => INTERVAL '1 minute',
        schedule_interval => INTERVAL '1 minute',
        if_not_exists    => TRUE
    )
    """,
]


async def init_schema() -> None:
    """Initialize TimescaleDB schema. Safe to call on every startup."""
    async with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for stmt in _AUTOCOMMIT_STMTS:
            stmt = stmt.strip()
            if not stmt:
                continue
            try:
                await conn.execute(text(stmt))
            except Exception as e:
                msg = str(e).lower()
                # Ignore expected idempotency errors
                if any(k in msg for k in ("already exists", "already a hypertable")):
                    logger.debug("Skipped (already exists): %s...", stmt[:60])
                else:
                    logger.error("Schema init failed on: %s", stmt[:80])
                    raise

    logger.info("TimescaleDB schema ready")
