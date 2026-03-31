import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.database import get_db
from backend.services.candle_history import coverage_looks_complete, merge_candles
from backend.services.chart_intelligence import (
    _TIMEFRAME_CONTEXT,
    _build_chart_payload,
    _build_study_profiles,
    _compress_matrix_row,
    _compute_indicator_series,
    _profile_sample_backtest,
)
from backend.utils.symbols import TIMEFRAME_SPECS as _TIMEFRAME_SPECS, asset_class as _asset_class, is_equity_symbol as _is_equity_symbol

router = APIRouter(prefix="/api", tags=["charts"])
logger = logging.getLogger(__name__)
_stock_market_data = None
_crypto_market_data = None

_DEFAULT_MATRIX_TIMEFRAMES = ("15m", "1h", "4h", "1d", "2d", "1w")


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


@router.get("/charts/{symbol}")
async def get_chart_data(
    symbol: str,
    timeframe: str = Query(default="15m", description="1m | 5m | 15m | 30m | 1h | 2h | 4h | 6h | 12h | 1d | 2d | 1w"),
    start: Optional[datetime] = Query(default=None),
    end: Optional[datetime] = Query(default=None),
    limit: int = Query(default=240, ge=30, le=1000),
    db: AsyncSession = Depends(get_db),
):
    return await _resolve_chart_payload(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
        limit=limit,
        db=db,
    )


@router.get("/charts/{symbol}/matrix")
async def get_chart_matrix(
    symbol: str,
    timeframes: str = Query(default="15m,1h,4h,1d,2d,1w"),
    limit: int = Query(default=120, ge=60, le=240),
    db: AsyncSession = Depends(get_db),
):
    requested_timeframes = _parse_timeframes(timeframes)
    rows = []

    for timeframe in requested_timeframes:
        try:
            payload = await _resolve_chart_payload(
                symbol=symbol,
                timeframe=timeframe,
                start=None,
                end=None,
                limit=limit,
                db=db,
            )
            rows.append(_compress_matrix_row(payload))
        except HTTPException as exc:
            if exc.status_code == 404:
                context = _TIMEFRAME_CONTEXT.get(
                    timeframe,
                    {"label": timeframe, "summary": "No timeframe context available."},
                )
                rows.append(
                    {
                        "timeframe": timeframe,
                        "label": context["label"],
                        "available": False,
                        "detail": exc.detail,
                    }
                )
            else:
                raise

    available_rows = [row for row in rows if row.get("available")]
    if not available_rows:
        raise HTTPException(status_code=404, detail=f"No chart data for {symbol.upper()} across requested timeframes")

    return {
        "symbol": symbol.upper(),
        "asset_class": _asset_class(symbol.upper()),
        "matrix": rows,
    }


async def resolve_study_profile_snapshot(
    symbol: str,
    timeframe: str,
    profile_key: Optional[str],
    db: AsyncSession,
    limit: int = 180,
) -> dict:
    payload = await _resolve_chart_payload(
        symbol=symbol,
        timeframe=timeframe,
        start=None,
        end=None,
        limit=limit,
        db=db,
    )
    return build_study_profile_snapshot(payload, profile_key=profile_key)


def build_study_profile_snapshot(payload: dict, profile_key: Optional[str] = None) -> dict:
    insights = payload.get("insights") or {}
    profiles = insights.get("study_profiles") or []
    if not profiles:
        raise HTTPException(status_code=404, detail="No study profile data available for this chart")

    selected = None
    if profile_key:
        selected = next((profile for profile in profiles if profile["key"] == profile_key), None)
    if selected is None:
        active_key = insights.get("active_study_profile")
        selected = next((profile for profile in profiles if profile["key"] == active_key), None)
    if selected is None:
        selected = profiles[0]

    summary = payload.get("summary") or {}
    market_regime = insights.get("market_regime") or {}
    return {
        "symbol": payload["symbol"],
        "timeframe": payload["timeframe"],
        "profile_key": selected["key"],
        "profile_title": selected["title"],
        "signal": selected.get("current_signal", 0),
        "signal_key": selected.get("current_signal_key", "neutral"),
        "signal_label": selected.get("current_signal_label", "Neutral"),
        "signal_tone": selected.get("current_signal_tone", "neutral"),
        "signal_summary": selected.get("current_signal_summary", "Profile state unavailable."),
        "fit_score_pct": selected.get("fit_score_pct"),
        "fit_label": selected.get("fit_label"),
        "market_regime": market_regime.get("label"),
        "headline": insights.get("headline"),
        "entry_guidance": selected.get("entry_guidance"),
        "timing_note": selected.get("timing_note"),
        "last_close": summary.get("close"),
        "change_pct": summary.get("change_pct"),
    }


async def _resolve_chart_payload(
    symbol: str,
    timeframe: str,
    start: Optional[datetime],
    end: Optional[datetime],
    limit: int,
    db: AsyncSession,
) -> dict:
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
    history_start = start - (timeframe_spec["bucket_width"] * timeframe_spec["warmup_candles"])
    history_limit = limit + timeframe_spec["warmup_candles"]

    candles = await _load_chart_candles(
        symbol=symbol.upper(),
        timeframe=timeframe,
        history_start=history_start,
        end=end,
        history_limit=history_limit,
        bucket_width=timeframe_spec["bucket_width"],
        db=db,
    )
    if not candles:
        raise HTTPException(status_code=404, detail=f"No chart data for {symbol.upper()} in requested range")

    return _build_chart_payload(
        symbol=symbol.upper(),
        timeframe=timeframe,
        candles=candles,
        start=start,
        limit=limit,
        warmup_candles=timeframe_spec["warmup_candles"],
    )


async def _load_chart_candles(
    symbol: str,
    timeframe: str,
    history_start: datetime,
    end: datetime,
    history_limit: int,
    bucket_width: timedelta,
    db: AsyncSession,
) -> list[dict]:
    if _stock_market_data is not None and _is_equity_symbol(symbol):
        try:
            candles = await _stock_market_data.fetch_bars(
                symbol=symbol,
                timeframe=timeframe,
                start=history_start,
                end=end,
                limit=history_limit,
            )
        except Exception as exc:
            logger.warning("Unable to fetch stock chart bars for %s: %s", symbol, exc)
            candles = []

        if candles:
            return candles

    db_candles = await _load_db_chart_candles(
        symbol=symbol,
        history_start=history_start,
        end=end,
        history_limit=history_limit,
        bucket_width=bucket_width,
        db=db,
    )

    if _crypto_market_data is not None and not _is_equity_symbol(symbol):
        provider_candles = []
        if not coverage_looks_complete(
            db_candles,
            start=history_start,
            end=end,
            bucket_width=bucket_width,
            limit=history_limit,
        ):
            try:
                provider_candles = await _crypto_market_data.fetch_bars(
                    symbol=symbol,
                    timeframe=timeframe,
                    start=history_start,
                    end=end,
                    limit=history_limit,
                )
            except Exception as exc:
                logger.warning("Unable to fetch crypto chart bars for %s: %s", symbol, exc)
                provider_candles = []

        if provider_candles:
            return merge_candles(db_candles, provider_candles)[-history_limit:]

    return db_candles


async def _load_db_chart_candles(
    symbol: str,
    history_start: datetime,
    end: datetime,
    history_limit: int,
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
                  AND time >= :history_start
                  AND time <= :end
            ),
            aggregated AS (
                SELECT
                    bucket,
                    max(CASE WHEN rn_open = 1 THEN price END)  AS open,
                    max(price)                                 AS high,
                    min(price)                                 AS low,
                    max(CASE WHEN rn_close = 1 THEN price END) AS close,
                    sum(volume)                                AS volume,
                    count(*)                                   AS ticks
                FROM bucketed
                GROUP BY bucket
            )
            SELECT bucket, open, high, low, close, volume, ticks
            FROM (
                SELECT *
                FROM aggregated
                ORDER BY bucket DESC
                LIMIT :history_limit
            ) recent
            ORDER BY bucket ASC
        """),
        {
            "symbol": symbol,
            "history_start": history_start,
            "end": end,
            "bucket_width": bucket_width,
            "history_limit": history_limit,
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


def _parse_timeframes(value: str) -> list[str]:
    seen = set()
    parsed = []
    for timeframe in [item.strip() for item in value.split(",") if item.strip()]:
        if timeframe not in _TIMEFRAME_SPECS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid timeframe '{timeframe}'. Valid: {', '.join(_TIMEFRAME_SPECS)}",
            )
        if timeframe not in seen:
            seen.add(timeframe)
            parsed.append(timeframe)

    if not parsed:
        return list(_DEFAULT_MATRIX_TIMEFRAMES)
    return parsed
