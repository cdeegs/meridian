"""
TimescaleDB schema initialization.

Runs on app startup. Uses AUTOCOMMIT for DDL that can't run inside a transaction
(create_hypertable, continuous aggregates, retention policies).
"""
import logging

from sqlalchemy import text

from backend.db.database import engine

logger = logging.getLogger(__name__)

_BASE_STMTS = [
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
    """
    CREATE INDEX IF NOT EXISTS idx_indicators_symbol_indicator_time
        ON indicators (symbol, indicator, time DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
        id            TEXT PRIMARY KEY,
        symbol        TEXT NOT NULL,
        condition     TEXT NOT NULL,
        threshold     DOUBLE PRECISION,
        status        TEXT NOT NULL DEFAULT 'active',
        triggered_at  TIMESTAMPTZ,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_alerts_symbol_status_created
        ON alerts (symbol, status, created_at DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS study_profile_alerts (
        id                 TEXT PRIMARY KEY,
        symbol             TEXT NOT NULL,
        timeframe          TEXT NOT NULL,
        profile_key        TEXT NOT NULL,
        delivery           TEXT NOT NULL DEFAULT 'telegram',
        status             TEXT NOT NULL DEFAULT 'active',
        last_signal        INTEGER NOT NULL DEFAULT 0,
        last_evaluated_at  TIMESTAMPTZ,
        last_triggered_at  TIMESTAMPTZ,
        created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE(symbol, timeframe, profile_key, delivery)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_study_profile_alerts_symbol_status_updated
        ON study_profile_alerts (symbol, status, updated_at DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolios (
        id          TEXT PRIMARY KEY,
        name        TEXT NOT NULL,
        strategy    TEXT,
        notes       TEXT,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_portfolios_updated_at
        ON portfolios (updated_at DESC)
    """,
    """
    CREATE TABLE IF NOT EXISTS portfolio_assets (
        id              TEXT PRIMARY KEY,
        portfolio_id    TEXT NOT NULL REFERENCES portfolios(id) ON DELETE CASCADE,
        symbol          TEXT NOT NULL,
        asset_type      TEXT NOT NULL,
        allocation_pct  DOUBLE PRECISION,
        strategy        TEXT,
        notes           TEXT,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_portfolio_assets_portfolio_created
        ON portfolio_assets (portfolio_id, created_at DESC)
    """,
]

# Statements that require AUTOCOMMIT (TimescaleDB DDL)
_TIMESCALE_STMTS = [
    "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE",
    "SELECT create_hypertable('ticks', 'time', if_not_exists => TRUE)",
    "SELECT create_hypertable('indicators', 'time', if_not_exists => TRUE)",
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
    """
    SELECT add_retention_policy(
        'ticks',
        INTERVAL '30 days',
        if_not_exists => TRUE
    )
    """,
]


async def init_schema() -> None:
    """Initialize TimescaleDB schema. Safe to call on every startup."""
    async with engine.begin() as conn:
        for stmt in _BASE_STMTS:
            stmt = stmt.strip()
            if not stmt:
                continue
            await conn.execute(text(stmt))

    async with engine.connect() as conn:
        result = await conn.execute(
            text("SELECT EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb')")
        )
        timescale_available = bool(result.scalar())

    if not timescale_available:
        logger.warning(
            "TimescaleDB extension not available — running in plain Postgres compatibility mode"
        )
        return

    async with engine.connect() as conn:
        conn = await conn.execution_options(isolation_level="AUTOCOMMIT")
        for stmt in _TIMESCALE_STMTS:
            stmt = stmt.strip()
            if not stmt:
                continue
            try:
                await conn.execute(text(stmt))
            except Exception as e:
                msg = str(e).lower()
                if any(k in msg for k in ("already exists", "already a hypertable")):
                    logger.debug("Skipped (already exists): %s...", stmt[:60])
                else:
                    logger.error("Schema init failed on: %s", stmt[:80])
                    raise

    logger.info("TimescaleDB schema ready (extension available)")
