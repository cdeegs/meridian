import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.database import get_db
from backend.indicators import BollingerBands, EMA, MACD, RSI, SMA, VWAP
from backend.services.candle_history import coverage_looks_complete, merge_candles

router = APIRouter(prefix="/api", tags=["charts"])
logger = logging.getLogger(__name__)
_stock_market_data = None
_crypto_market_data = None

_TIMEFRAME_SPECS = {
    "1m": {
        "bucket_width": timedelta(minutes=1),
        "window": timedelta(hours=6),
        "warmup_candles": 120,
    },
    "5m": {
        "bucket_width": timedelta(minutes=5),
        "window": timedelta(hours=24),
        "warmup_candles": 120,
    },
    "15m": {
        "bucket_width": timedelta(minutes=15),
        "window": timedelta(days=3),
        "warmup_candles": 120,
    },
    "30m": {
        "bucket_width": timedelta(minutes=30),
        "window": timedelta(days=5),
        "warmup_candles": 120,
    },
    "1h": {
        "bucket_width": timedelta(hours=1),
        "window": timedelta(days=14),
        "warmup_candles": 120,
    },
    "2h": {
        "bucket_width": timedelta(hours=2),
        "window": timedelta(days=21),
        "warmup_candles": 120,
    },
    "4h": {
        "bucket_width": timedelta(hours=4),
        "window": timedelta(days=60),
        "warmup_candles": 120,
    },
    "6h": {
        "bucket_width": timedelta(hours=6),
        "window": timedelta(days=90),
        "warmup_candles": 140,
    },
    "12h": {
        "bucket_width": timedelta(hours=12),
        "window": timedelta(days=180),
        "warmup_candles": 160,
    },
    "1d": {
        "bucket_width": timedelta(days=1),
        "window": timedelta(days=365),
        "warmup_candles": 200,
    },
    "2d": {
        "bucket_width": timedelta(days=2),
        "window": timedelta(days=730),
        "warmup_candles": 220,
    },
    "1w": {
        "bucket_width": timedelta(days=7),
        "window": timedelta(days=1825),
        "warmup_candles": 260,
    },
}

_TIMEFRAME_CONTEXT = {
    "1m": {
        "label": "Fast tape",
        "summary": "Best for execution timing and short-term reaction. Expect more noise and false breaks, so anchor bias to higher timeframes.",
    },
    "5m": {
        "label": "Intraday precision",
        "summary": "Useful for intraday entries and pullbacks. Signals are cleaner than 1m, but still sensitive to headline-driven whipsaws.",
    },
    "15m": {
        "label": "Balanced intraday",
        "summary": "A good middle ground for intraday structure. Strong enough for trend reads without feeling as twitchy as the 1m tape.",
    },
    "30m": {
        "label": "Structured intraday",
        "summary": "Useful when you want cleaner intraday structure without jumping all the way to swing windows. Good for filtering out smaller tape noise.",
    },
    "1h": {
        "label": "Swing lens",
        "summary": "Good for multi-session trend and momentum context. Entries are slower, but the read is usually more trustworthy.",
    },
    "2h": {
        "label": "Session swing",
        "summary": "Bridges intraday and swing structure. Helpful when the 1h is still noisy but daily framing is too slow.",
    },
    "4h": {
        "label": "Multi-session trend",
        "summary": "Useful for holding-period decisions over several days. This window is better for regime and structure than for precise entries.",
    },
    "6h": {
        "label": "Position rhythm",
        "summary": "Good for tracking broader swing rhythm across multiple sessions. Better for patience and structure than for quick timing.",
    },
    "12h": {
        "label": "Position structure",
        "summary": "Useful when you want a slower, cleaner read that still responds faster than the daily chart.",
    },
    "1d": {
        "label": "Regime view",
        "summary": "Best for the big picture. Use this to judge dominant trend and risk backdrop, then drop lower for timing.",
    },
    "2d": {
        "label": "Multi-week regime",
        "summary": "Helps smooth out daily churn and focus on the broader swing path over several weeks.",
    },
    "1w": {
        "label": "Primary trend",
        "summary": "Best for long-cycle structure. Use this to see the dominant market regime before making shorter-term decisions.",
    },
}

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


def _is_equity_symbol(symbol: str) -> bool:
    return "-" not in symbol and "/" not in symbol


def _asset_class(symbol: str) -> str:
    return "stock" if _is_equity_symbol(symbol) else "crypto"


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


def _build_chart_payload(
    symbol: str,
    timeframe: str,
    candles: list[dict],
    start: datetime,
    limit: int,
    warmup_candles: int,
) -> dict:
    visible_candles = [candle for candle in candles if candle["time"] >= start]
    if len(visible_candles) > limit:
        visible_candles = visible_candles[-limit:]
    if not visible_candles:
        raise HTTPException(status_code=404, detail=f"No chart data for {symbol} in requested range")

    indicators = _compute_indicator_series(candles)
    visible_times = {candle["time"] for candle in visible_candles}
    visible_indicators = {
        name: [entry for entry in series if entry["time"] in visible_times]
        for name, series in indicators.items()
    }
    summary = _build_summary(visible_candles)
    insights = _build_insights(
        symbol=symbol,
        timeframe=timeframe,
        candles=candles,
        visible_candles=visible_candles,
        indicators=indicators,
        summary=summary,
    )

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "candles": visible_candles,
        "indicators": visible_indicators,
        "summary": summary,
        "insights": insights,
        "coverage": {
            "visible_candles": len(visible_candles),
            "history_candles": len(candles),
            "warmup_candles": warmup_candles,
        },
    }


def _compute_indicator_series(candles: list[dict]) -> dict:
    indicators = [
        ("sma_20", SMA(20)),
        ("sma_50", SMA(50)),
        ("sma_100", SMA(100)),
        ("sma_200", SMA(200)),
        ("ema_8", EMA(8)),
        ("ema_9", EMA(9)),
        ("ema_12", EMA(12)),
        ("ema_21", EMA(21)),
        ("ema_26", EMA(26)),
        ("ema_50", EMA(50)),
        ("ema_100", EMA(100)),
        ("ema_200", EMA(200)),
        ("rsi_7", RSI(7)),
        ("rsi_14", RSI(14)),
        ("rsi_21", RSI(21)),
        ("macd", MACD()),
        ("macd_8_21_5", MACD(8, 21, 5)),
        ("macd_21_55_9", MACD(21, 55, 9)),
        ("bollinger_20", BollingerBands(20)),
        ("bollinger_34", BollingerBands(34)),
        ("vwap", VWAP()),
    ]
    series = {name: [] for name, _ in indicators}

    for candle in candles:
        timestamp = candle["time"]
        price = candle["close"]
        volume = candle["volume"]

        for name, indicator in indicators:
            value = indicator.update(price=price, volume=volume, timestamp=timestamp)
            if value is None:
                continue
            if isinstance(value, dict):
                series[name].append({"time": timestamp, **value})
            else:
                series[name].append({"time": timestamp, "value": float(value)})

    return series


def _build_summary(candles: list[dict]) -> dict:
    first = candles[0]
    last = candles[-1]
    change = last["close"] - first["open"]
    change_pct = (change / first["open"] * 100.0) if first["open"] else 0.0

    return {
        "first_time": first["time"],
        "last_time": last["time"],
        "open": first["open"],
        "close": last["close"],
        "high": max(candle["high"] for candle in candles),
        "low": min(candle["low"] for candle in candles),
        "volume": sum(candle["volume"] for candle in candles),
        "ticks": sum(candle["ticks"] for candle in candles),
        "change": round(change, 6),
        "change_pct": round(change_pct, 4),
    }


def _build_insights(
    symbol: str,
    timeframe: str,
    candles: list[dict],
    visible_candles: list[dict],
    indicators: dict,
    summary: dict,
) -> dict:
    asset_class = _asset_class(symbol)
    close = visible_candles[-1]["close"]
    ema12 = _last_indicator_value(indicators.get("ema_12", []))
    ema26 = _last_indicator_value(indicators.get("ema_26", []))
    sma20 = _last_indicator_value(indicators.get("sma_20", []))
    vwap = _last_indicator_value(indicators.get("vwap", []))
    rsi = _last_indicator_value(indicators.get("rsi_14", []))
    macd = _last_indicator_entry(indicators.get("macd", []))
    bollinger = _last_indicator_entry(indicators.get("bollinger_20", []))

    trend = _build_trend_snapshot(
        close=close,
        ema12=ema12,
        ema26=ema26,
        sma20=sma20,
        summary=summary,
        ema12_values=_series_values(indicators.get("ema_12", [])),
    )
    momentum = _build_momentum_snapshot(
        rsi=rsi,
        rsi_values=_series_values(indicators.get("rsi_14", [])),
        macd=macd,
    )
    volatility = _build_volatility_snapshot(
        candles=candles,
        close=close,
        bollinger=bollinger,
        asset_class=asset_class,
        timeframe=timeframe,
    )
    participation = _build_participation_snapshot(candles)
    stretch = _build_stretch_snapshot(
        close=close,
        ema12=ema12,
        vwap=vwap,
        bollinger=bollinger,
        asset_class=asset_class,
        timeframe=timeframe,
    )
    timeframe_context = _TIMEFRAME_CONTEXT.get(
        timeframe,
        {"label": "Selected window", "summary": "This timeframe gives context for the current chart."},
    )
    indicator_guides = _build_indicator_guides(
        asset_class=asset_class,
        timeframe=timeframe,
        trend_score=trend["score_raw"],
        rsi=rsi,
        rsi_percentile=momentum.get("rsi_percentile"),
        macd=macd,
        close=close,
        atr_pct=volatility.get("atr_pct"),
        relative_volume=participation.get("relative_volume"),
        vwap_distance_pct=stretch.get("distance_to_vwap_pct"),
        bollinger=bollinger,
    )
    ai_overview = _build_ai_overview(
        symbol=symbol,
        timeframe=timeframe,
        timeframe_context=timeframe_context,
        trend=trend,
        momentum=momentum,
        volatility=volatility,
        participation=participation,
        stretch=stretch,
    )
    market_regime = _build_market_regime(
        trend=trend,
        momentum=momentum,
        volatility=volatility,
        participation=participation,
        stretch=stretch,
    )
    study_profiles, default_study_profile = _build_study_profiles(
        asset_class=asset_class,
        timeframe=timeframe,
        visible_instances=len(visible_candles),
    )
    study_profiles, active_study_profile = _rank_study_profiles(
        profiles=study_profiles,
        default_key=default_study_profile,
        asset_class=asset_class,
        timeframe=timeframe,
        visible_instances=len(visible_candles),
        candles=candles,
        visible_candles=visible_candles,
        indicators=indicators,
        trend=trend,
        momentum=momentum,
        volatility=volatility,
        participation=participation,
        stretch=stretch,
        market_regime=market_regime,
    )
    study_profiles = [
        {
            **profile,
            **_profile_live_state(
                profile=profile,
                candles=visible_candles,
                indicators=indicators,
            ),
        }
        for profile in study_profiles
    ]

    cards = [
        {
            "key": "trend",
            "title": "Trend",
            "label": trend["label"],
            "tone": trend["tone"],
            "value": f"{trend['score_pct']:.0f}/100",
            "summary": trend["summary"],
        },
        {
            "key": "momentum",
            "title": "Momentum",
            "label": momentum["label"],
            "tone": momentum["tone"],
            "value": f"RSI {rsi:.1f}" if rsi is not None else "Warming up",
            "summary": momentum["summary"],
        },
        {
            "key": "volatility",
            "title": "Volatility",
            "label": volatility["label"],
            "tone": volatility["tone"],
            "value": f"ATR {volatility['atr_pct']:.2f}%" if volatility.get("atr_pct") is not None else "Building",
            "summary": volatility["summary"],
        },
        {
            "key": "participation",
            "title": "Participation",
            "label": participation["label"],
            "tone": participation["tone"],
            "value": f"{participation['relative_volume']:.2f}x RVOL" if participation.get("relative_volume") is not None else "Waiting",
            "summary": participation["summary"],
        },
        {
            "key": "stretch",
            "title": "Stretch",
            "label": stretch["label"],
            "tone": stretch["tone"],
            "value": stretch["value"],
            "summary": stretch["summary"],
        },
    ]

    headline = (
        f"{symbol} on {timeframe} looks {trend['label'].lower()} with "
        f"{momentum['label'].lower()} momentum, {participation['label'].lower()} volume, "
        f"and {stretch['label'].lower()} positioning."
    )

    return {
        "asset_class": asset_class,
        "headline": headline,
        "timeframe_context": timeframe_context,
        "market_regime": market_regime,
        "trend": trend,
        "momentum": momentum,
        "volatility": volatility,
        "participation": participation,
        "stretch": stretch,
        "cards": cards,
        "indicator_guides": indicator_guides,
        "ai_overview": ai_overview,
        "study_profiles": study_profiles,
        "active_study_profile": active_study_profile,
    }


def _build_study_profiles(asset_class: str, timeframe: str, visible_instances: int) -> tuple[list[dict], str]:
    fast_window = timeframe in {"1m", "5m", "15m", "30m"}
    slow_window = timeframe in {"1d", "2d", "1w"}
    dense_view = visible_instances >= 240
    compressed_view = visible_instances <= 90

    responsive_fast = 8 if asset_class == "crypto" else 9
    balanced_fast = 12
    balanced_slow = 26

    profiles = [
        {
            "key": "responsive",
            "title": "Responsive Tape",
            "tone": "positive" if fast_window or compressed_view else "info",
            "summary": (
                "Faster averages for quick pullbacks, reclaim setups, and intraday tape changes."
                if asset_class == "crypto"
                else "A faster profile for active stocks when you want earlier turns and tighter structure."
            ),
            "best_for": "Best when the chart is fast or the visible instances are tight.",
            "why": (
                f"{asset_class.title()} can turn quickly on this window, so a faster EMA pair keeps the structure readable without waiting for very slow averages to react."
            ),
            "studies": {
                "fast_ema": {"period": responsive_fast, "key": f"ema_{responsive_fast}", "label": f"EMA {responsive_fast}"},
                "slow_ema": {"period": 21, "key": "ema_21", "label": "EMA 21"},
                "trend_sma": {"period": 20, "key": "sma_20", "label": "SMA 20"},
                "anchor_sma": {"period": 50, "key": "sma_50", "label": "SMA 50"},
                "rsi": {"period": 7, "key": "rsi_7", "label": "RSI 7"},
                "macd": {"fast": 8, "slow": 21, "signal": 5, "key": "macd_8_21_5", "label": "MACD 8/21/5"},
                "bollinger": {"period": 20, "key": "bollinger_20", "label": "Bollinger 20"},
                "vwap": {"key": "vwap", "label": "VWAP"},
            },
            "default_overlays": {
                "fast_ema": True,
                "slow_ema": True,
                "trend_sma": False,
                "anchor_sma": False,
                "vwap": True,
                "bollinger": False,
            },
        },
        {
            "key": "balanced",
            "title": "Balanced Structure",
            "tone": "positive" if not fast_window and not slow_window else "info",
            "summary": "The default blend for most charts: enough speed to stay useful, enough smoothing to avoid overreacting.",
            "best_for": "Best for most one-hour to daily reads and normal chart review.",
            "why": (
                "This profile keeps industry-standard studies on the board, which makes the read easier to compare against other platforms and trader workflows."
            ),
            "studies": {
                "fast_ema": {"period": balanced_fast, "key": f"ema_{balanced_fast}", "label": f"EMA {balanced_fast}"},
                "slow_ema": {"period": balanced_slow, "key": f"ema_{balanced_slow}", "label": f"EMA {balanced_slow}"},
                "trend_sma": {"period": 50, "key": "sma_50", "label": "SMA 50"},
                "anchor_sma": {"period": 100, "key": "sma_100", "label": "SMA 100"},
                "rsi": {"period": 14, "key": "rsi_14", "label": "RSI 14"},
                "macd": {"fast": 12, "slow": 26, "signal": 9, "key": "macd", "label": "MACD 12/26/9"},
                "bollinger": {"period": 20, "key": "bollinger_20", "label": "Bollinger 20"},
                "vwap": {"key": "vwap", "label": "VWAP"},
            },
            "default_overlays": {
                "fast_ema": True,
                "slow_ema": True,
                "trend_sma": True,
                "anchor_sma": False,
                "vwap": True,
                "bollinger": False,
            },
        },
        {
            "key": "trend",
            "title": "Trend / Anchor",
            "tone": "positive" if slow_window or dense_view else "info",
            "summary": "Slower studies for cleaner trend-following and higher-timeframe structure work.",
            "best_for": "Best when you care more about trend persistence than fast entries.",
            "why": (
                "Slower windows and larger instance counts benefit from slower moving averages because they reduce noise and make structural support or resistance easier to see."
            ),
            "studies": {
                "fast_ema": {"period": 21, "key": "ema_21", "label": "EMA 21"},
                "slow_ema": {"period": 50, "key": "ema_50", "label": "EMA 50"},
                "trend_sma": {"period": 100, "key": "sma_100", "label": "SMA 100"},
                "anchor_sma": {"period": 200, "key": "sma_200", "label": "SMA 200"},
                "rsi": {"period": 21, "key": "rsi_21", "label": "RSI 21"},
                "macd": {"fast": 21, "slow": 55, "signal": 9, "key": "macd_21_55_9", "label": "MACD 21/55/9"},
                "bollinger": {"period": 34, "key": "bollinger_34", "label": "Bollinger 34"},
                "vwap": {"key": "vwap", "label": "VWAP"},
            },
            "default_overlays": {
                "fast_ema": True,
                "slow_ema": True,
                "trend_sma": True,
                "anchor_sma": True,
                "vwap": False,
                "bollinger": False,
            },
        },
    ]

    if fast_window or compressed_view:
        active_key = "responsive"
    elif slow_window or dense_view:
        active_key = "trend"
    else:
        active_key = "balanced"

    return profiles, active_key


def _build_market_regime(
    trend: dict,
    momentum: dict,
    volatility: dict,
    participation: dict,
    stretch: dict,
) -> dict:
    trend_score = float(trend.get("score_raw") or 0.0)
    trend_abs = abs(trend_score)
    relative_volume = participation.get("relative_volume")

    breakout_score = 0.0
    chop_score = 0.0

    if volatility.get("label") == "Expanded":
        breakout_score += 1.6
    elif volatility.get("label") == "Compressed":
        chop_score += 1.6
    else:
        breakout_score += 0.3
        chop_score += 0.3

    if relative_volume is not None:
        if relative_volume >= 1.35:
            breakout_score += 1.5
        elif relative_volume < 0.9:
            chop_score += 1.0

    if trend_abs >= 2.5:
        breakout_score += 1.0
    elif trend_abs < 1.0:
        chop_score += 1.4
    else:
        breakout_score += 0.5

    if momentum.get("label") in {"Building", "Fading", "Heated", "Washed Out"} and trend_abs >= 1.0:
        breakout_score += 0.8
    elif momentum.get("label") == "Balanced":
        chop_score += 0.7

    if stretch.get("label") == "Near Fair Value":
        chop_score += 0.5
    elif stretch.get("tone") in {"caution", "negative"}:
        breakout_score += 0.4

    if breakout_score >= 3.4:
        if trend_score <= -1.0:
            key, label, tone = "breakdown", "Breakdown Expansion", "negative"
        elif trend_score >= 1.0:
            key, label, tone = "breakout", "Breakout Expansion", "positive"
        else:
            key, label, tone = "expansion", "Expansion Phase", "caution"
        summary = (
            "Range and participation are expanding together. Fast continuation signals matter more than mean-reversion assumptions."
        )
    elif trend_abs >= 2.5:
        key, label = "trend", "Trend Persistence"
        tone = "positive" if trend_score > 0 else "negative"
        summary = "Trend structure is doing most of the work. Slower anchors matter more than every intrabar wiggle."
    elif chop_score >= 3.0:
        key, label, tone = "chop", "Range / Chop", "neutral"
        summary = "The tape looks rotational rather than directional. Balanced studies usually read this better than very fast trend tools."
    else:
        key, label, tone = "rotation", "Balanced Rotation", "info"
        summary = "Conditions are mixed. A middle-speed profile is usually the cleanest compromise until the tape commits."

    return {
        "key": key,
        "label": label,
        "tone": tone,
        "summary": summary,
        "breakout_score": round(breakout_score, 2),
        "chop_score": round(chop_score, 2),
    }


def _rank_study_profiles(
    profiles: list[dict],
    default_key: str,
    asset_class: str,
    timeframe: str,
    visible_instances: int,
    candles: list[dict],
    visible_candles: list[dict],
    indicators: dict,
    trend: dict,
    momentum: dict,
    volatility: dict,
    participation: dict,
    stretch: dict,
    market_regime: dict,
) -> tuple[list[dict], str]:
    ranked = []
    for profile in profiles:
        score = 50.0
        contributions: list[tuple[float, str]] = []

        def add(points: float, reason: str) -> None:
            nonlocal score
            score += points
            contributions.append((points, reason))

        key = profile["key"]
        fast_window = timeframe in {"1m", "5m", "15m", "30m"}
        slow_window = timeframe in {"1d", "2d", "1w"}
        dense_view = visible_instances >= 240
        compressed_view = visible_instances <= 90

        if key == default_key:
            add(6.0, "default fit for this window")

        if key == "responsive":
            if fast_window:
                add(12.0, "fast timeframe rewards quicker studies")
            if compressed_view:
                add(6.0, "tight visible window favors more responsive overlays")
            if asset_class == "crypto":
                add(3.0, "crypto usually benefits from faster anchors")
        elif key == "trend":
            if slow_window:
                add(12.0, "slower timeframe rewards longer anchors")
            if dense_view:
                add(6.0, "larger instance count favors slower trend structure")
            if asset_class == "stock":
                add(2.0, "stocks usually respect slower anchors better than fast crypto tape")
        else:
            add(4.0, "balanced profile is a solid baseline")
            if not fast_window and not slow_window:
                add(6.0, "mid-speed windows usually fit balanced studies best")

        trend_strength = abs(float(trend.get("score_raw") or 0.0))
        if trend_strength >= 3.0:
            if key == "trend":
                add(12.0, "strong trend structure favors slower anchors")
            elif key == "balanced":
                add(6.0, "balanced studies still read strong trends well")
            else:
                add(2.0, "responsive studies can help with entries inside a strong trend")
        elif trend_strength >= 1.0:
            if key == "balanced":
                add(8.0, "moderate trend usually fits balanced studies")
            elif key == "responsive" and fast_window:
                add(5.0, "fast chart plus moderate trend keeps responsive studies useful")
            elif key == "trend":
                add(4.0, "trend profile can still help frame structure")
        else:
            if key == "balanced":
                add(8.0, "low trend strength favors a middle-speed read")
            elif key == "responsive" and fast_window:
                add(4.0, "responsive studies can help when the tape is choppy but active")
            elif key == "trend":
                add(-4.0, "very slow studies add little when trend strength is weak")

        volatility_label = volatility.get("label")
        if volatility_label == "Expanded":
            if key == "responsive":
                add(9.0, "expanded volatility rewards faster confirmation")
            elif key == "balanced":
                add(4.0, "balanced studies can still frame expansion without overreacting")
            else:
                add(1.0, "slow studies help anchor expansion but may react later")
        elif volatility_label == "Compressed":
            if key == "balanced":
                add(8.0, "compressed conditions usually read best with balanced studies")
            elif key == "trend":
                add(2.0, "slower structure can help if compression is building for a bigger move")
            else:
                add(-2.0, "fast studies can overreact during compression")
        else:
            if key == "balanced":
                add(5.0, "normal volatility keeps balanced studies comfortable")
            elif key == "trend":
                add(3.0, "slower anchors are still readable in normal volatility")

        regime_key = market_regime.get("key")
        if regime_key in {"breakout", "breakdown", "expansion"}:
            if key == "responsive":
                add(10.0, "expansion regime favors quicker confirmation")
            elif key == "trend":
                add(5.0, "trend anchors help keep expansion moves in context")
            else:
                add(3.0, "balanced studies can still frame breakout follow-through")
        elif regime_key == "trend":
            if key == "trend":
                add(10.0, "persistent trend favors slower anchors")
            elif key == "balanced":
                add(5.0, "balanced studies remain a good compromise in persistent trends")
        elif regime_key == "chop":
            if key == "balanced":
                add(10.0, "range / chop usually needs a balanced read")
            elif key == "responsive" and fast_window:
                add(4.0, "responsive studies can help scalp chop on faster windows")
            elif key == "trend":
                add(-5.0, "trend profile tends to lag during chop")
        else:
            if key == "balanced":
                add(8.0, "mixed rotation usually fits balanced studies best")

        if stretch.get("label") == "Near Fair Value":
            if key == "balanced":
                add(2.0, "clean location supports the balanced profile")
        elif stretch.get("tone") == "caution":
            if key == "trend":
                add(2.0, "slower anchors help frame stretched moves")

        backtest = _profile_sample_backtest(
            profile=profile,
            candles=candles,
            visible_candles=visible_candles,
            indicators=indicators,
        )
        backtest_score = _score_backtest_fit(backtest)
        if backtest_score:
            add(backtest_score, _backtest_reason(backtest))

        sorted_positive_reasons = [reason for points, reason in sorted(contributions, key=lambda item: item[0], reverse=True) if points > 0][:3]
        sorted_negative_reasons = [reason for points, reason in sorted(contributions, key=lambda item: item[0]) if points < 0][:2]
        fit_score = max(0.0, min(100.0, round(score, 1)))
        if fit_score >= 78:
            fit_label = "Best Match"
        elif fit_score >= 64:
            fit_label = "Good Match"
        else:
            fit_label = "Secondary"
        entry_guidance, timing_note = _profile_entry_guidance(
            profile=profile,
            market_regime=market_regime,
            trend=trend,
            momentum=momentum,
            participation=participation,
            stretch=stretch,
        )

        enriched = {
            **profile,
            "fit_score_pct": fit_score,
            "fit_label": fit_label,
            "fit_summary": "; ".join(sorted_positive_reasons) if sorted_positive_reasons else "Waiting for more context.",
            "fit_risks": sorted_negative_reasons,
            "market_regime": market_regime["label"],
            "backtest": backtest,
            "entry_guidance": entry_guidance,
            "timing_note": timing_note,
        }
        ranked.append(enriched)

    ranked.sort(key=lambda profile: (profile["fit_score_pct"], profile["key"] == default_key), reverse=True)
    active_key = ranked[0]["key"] if ranked else default_key
    for profile in ranked:
        profile["recommended"] = profile["key"] == active_key
    return ranked, active_key


def _profile_entry_guidance(
    profile: dict,
    market_regime: dict,
    trend: dict,
    momentum: dict,
    participation: dict,
    stretch: dict,
) -> tuple[str, str]:
    studies = profile["studies"]
    fast_ema = studies["fast_ema"]["label"]
    slow_ema = studies["slow_ema"]["label"]
    trend_sma = studies["trend_sma"]["label"]
    anchor_sma = studies["anchor_sma"]["label"]
    rsi = studies["rsi"]["label"]
    macd = studies["macd"]["label"]
    vwap = studies["vwap"]["label"]

    if profile["key"] == "responsive":
        entry_guidance = (
            f"Better buy setups usually come when price reclaims {fast_ema} and holds above {slow_ema}, "
            f"{macd} flips back above signal, and {rsi} turns up through its midline. "
            f"On faster charts, a retest of {vwap} is usually cleaner than buying the first spike."
        )
    elif profile["key"] == "balanced":
        entry_guidance = (
            f"Better buy setups usually come on pullbacks that hold {fast_ema}, {slow_ema}, or {trend_sma}, "
            f"with {rsi} staying constructive and {macd} remaining above signal. "
            f"This profile is usually strongest when the market is trending but not wildly extended."
        )
    else:
        entry_guidance = (
            f"Better buy setups usually come after a controlled pullback into {fast_ema}, {trend_sma}, or {anchor_sma}, "
            f"while {rsi} cools without breaking down and {macd} starts curling back up. "
            f"This profile is for buying trend continuation, not chasing vertical candles."
        )

    relative_volume = participation.get("relative_volume")
    if trend.get("tone") == "negative":
        timing_note = (
            f"Current timing: not a clean buy yet. Wait for price to reclaim {fast_ema} and for {macd} to stop leaning down before treating this like a long setup."
        )
    elif stretch.get("tone") == "caution":
        anchor = trend_sma if profile["key"] == "trend" else fast_ema
        timing_note = (
            f"Current timing: structure may still be healthy, but price is stretched. Better after a pullback toward {anchor} or {vwap} than on a vertical candle."
        )
    elif relative_volume is not None and relative_volume < 0.9:
        timing_note = (
            "Current timing: volume is still light, so breakouts are easier to fade. Better when participation firms up."
        )
    elif market_regime.get("key") in {"breakout", "expansion"} and trend.get("tone") == "positive":
        timing_note = (
            f"Current timing: strongest buys usually come on breakout retests that hold {fast_ema} or {vwap}, not on the very first impulse candle."
        )
    elif momentum.get("tone") == "Balanced":
        timing_note = (
            f"Current timing: momentum is still neutral. Better when {rsi} turns up and {macd} starts widening in the same direction."
        )
    else:
        timing_note = (
            f"Current timing: the profile is constructive now as long as price stays above {fast_ema} and {macd} does not slip back under signal."
        )

    return entry_guidance, timing_note


def _profile_sample_backtest(
    profile: dict,
    candles: list[dict],
    visible_candles: list[dict],
    indicators: dict,
) -> dict:
    if len(candles) < 30 or len(visible_candles) < 20:
        return {
            "sample_bars": len(visible_candles),
            "active_bars": 0,
            "trades": 0,
            "return_pct": None,
            "buy_hold_pct": None,
            "edge_pct": None,
            "hit_rate_pct": None,
            "summary": "Need more history before the sample backtest is meaningful.",
        }

    studies = profile["studies"]
    visible_start = visible_candles[0]["time"]
    signal_maps = {
        name: {entry["time"]: entry for entry in indicators.get(config["key"], [])}
        for name, config in studies.items()
        if config.get("key")
    }

    equity = 1.0
    active_bars = 0
    winning_bars = 0
    trades = 0
    previous_signal = 0
    sample_bars = 0

    for index in range(1, len(candles)):
        previous_candle = candles[index - 1]
        candle = candles[index]
        if previous_candle["time"] < visible_start:
            continue
        previous_close = float(previous_candle["close"])
        current_close = float(candle["close"])
        if previous_close <= 0:
            continue

        sample_bars += 1
        signal = _profile_signal(
            profile=profile,
            candle=previous_candle,
            signal_maps=signal_maps,
        )
        if signal != 0 and signal != previous_signal:
            trades += 1
        previous_signal = signal

        move = (current_close - previous_close) / previous_close
        if signal != 0:
            active_bars += 1
            realized = signal * move
            equity *= max(0.01, 1.0 + realized)
            if realized > 0:
                winning_bars += 1

    first_close = float(visible_candles[0]["close"])
    last_close = float(visible_candles[-1]["close"])
    buy_hold_pct = ((last_close - first_close) / first_close * 100.0) if first_close else None
    strategy_return_pct = (equity - 1.0) * 100.0 if active_bars else 0.0
    edge_pct = strategy_return_pct - buy_hold_pct if buy_hold_pct is not None else None
    hit_rate_pct = (winning_bars / active_bars * 100.0) if active_bars else None

    if active_bars < 8:
        summary = "Signal sample is still thin, so use the score as a hint rather than proof."
    else:
        summary = (
            f"Recent sample ran {strategy_return_pct:+.2f}% versus {buy_hold_pct:+.2f}% buy-and-hold"
            if buy_hold_pct is not None
            else f"Recent sample ran {strategy_return_pct:+.2f}% on active bars."
        )

    return {
        "sample_bars": sample_bars,
        "active_bars": active_bars,
        "trades": trades,
        "return_pct": round(strategy_return_pct, 2),
        "buy_hold_pct": round(buy_hold_pct, 2) if buy_hold_pct is not None else None,
        "edge_pct": round(edge_pct, 2) if edge_pct is not None else None,
        "hit_rate_pct": round(hit_rate_pct, 1) if hit_rate_pct is not None else None,
        "summary": summary,
    }


def _profile_signal(profile: dict, candle: dict, signal_maps: dict[str, dict]) -> int:
    time = candle["time"]
    close = float(candle["close"])

    fast_entry = signal_maps.get("fast_ema", {}).get(time)
    slow_entry = signal_maps.get("slow_ema", {}).get(time)
    macd_entry = signal_maps.get("macd", {}).get(time)
    if fast_entry is None or slow_entry is None or macd_entry is None:
        return 0

    fast_value = float(fast_entry["value"])
    slow_value = float(slow_entry["value"])
    macd_value = float(macd_entry["macd"])
    signal_value = float(macd_entry["signal"])

    trend_entry = signal_maps.get("trend_sma", {}).get(time)
    trend_value = float(trend_entry["value"]) if trend_entry and trend_entry.get("value") is not None else None
    rsi_entry = signal_maps.get("rsi", {}).get(time)
    rsi_value = float(rsi_entry["value"]) if rsi_entry and rsi_entry.get("value") is not None else None

    long_votes = 0.0
    short_votes = 0.0

    if close >= fast_value:
        long_votes += 1.0
    else:
        short_votes += 1.0

    if fast_value >= slow_value:
        long_votes += 1.0
    else:
        short_votes += 1.0

    if macd_value >= signal_value:
        long_votes += 1.0
    else:
        short_votes += 1.0

    if trend_value is not None:
        if close >= trend_value:
            long_votes += 0.6
        else:
            short_votes += 0.6

    if rsi_value is not None:
        if rsi_value >= 62.0:
            long_votes += 0.5
        elif rsi_value <= 38.0:
            short_votes += 0.5

        if 40.0 <= rsi_value <= 92.0:
            long_votes += 0.3
        if 8.0 <= rsi_value <= 60.0:
            short_votes += 0.3

    if long_votes >= 2.5 and long_votes >= short_votes + 1.0:
        return 1
    if short_votes >= 2.5 and short_votes >= long_votes + 1.0:
        return -1
    return 0


def _profile_live_state(profile: dict, candles: list[dict], indicators: dict) -> dict:
    if not candles:
        return {
            "current_signal": 0,
            "current_signal_key": "neutral",
            "current_signal_label": "Waiting",
            "current_signal_tone": "neutral",
            "current_signal_summary": "Waiting for enough chart data to evaluate this profile.",
        }

    signal_maps = {
        name: {entry["time"]: entry for entry in indicators.get(config["key"], [])}
        for name, config in profile["studies"].items()
        if config.get("key")
    }
    signal = _profile_signal(profile, candles[-1], signal_maps)

    if signal > 0:
        summary = (
            f"{profile['title']} is aligned for a constructive long read right now. "
            f"{profile.get('timing_note') or ''}"
        ).strip()
        return {
            "current_signal": 1,
            "current_signal_key": "long",
            "current_signal_label": "Constructive",
            "current_signal_tone": "positive",
            "current_signal_summary": summary,
        }

    if signal < 0:
        summary = (
            f"{profile['title']} is leaning defensive or bearish right now. "
            "Better to wait for structure to repair before treating it like a buy setup."
        )
        return {
            "current_signal": -1,
            "current_signal_key": "short",
            "current_signal_label": "Defensive",
            "current_signal_tone": "negative",
            "current_signal_summary": summary,
        }

    return {
        "current_signal": 0,
        "current_signal_key": "neutral",
        "current_signal_label": "Not Ready",
        "current_signal_tone": "neutral",
        "current_signal_summary": profile.get("timing_note")
        or "The profile is not fully aligned yet. Wait for structure and momentum to confirm together.",
    }


def _score_backtest_fit(backtest: dict) -> float:
    edge_pct = backtest.get("edge_pct")
    if edge_pct is None:
        return 0.0
    score = max(-14.0, min(14.0, float(edge_pct) * 1.4))
    active_bars = int(backtest.get("active_bars") or 0)
    trades = int(backtest.get("trades") or 0)
    if active_bars < 12:
        score *= 0.55
    elif active_bars < 24:
        score *= 0.75
    if trades < 3:
        score *= 0.75
    hit_rate = backtest.get("hit_rate_pct")
    if hit_rate is not None:
        if hit_rate >= 58:
            score += 2.0
        elif hit_rate <= 42:
            score -= 2.0
    return round(score, 2)


def _backtest_reason(backtest: dict) -> str:
    edge_pct = backtest.get("edge_pct")
    hit_rate = backtest.get("hit_rate_pct")
    trades = int(backtest.get("trades") or 0)
    if edge_pct is None:
        return "recent sample is still too thin"
    if trades < 3:
        return "recent sample looks promising but trade count is still light"
    if edge_pct >= 0:
        if hit_rate is not None:
            return f"recent sample beat buy-and-hold by {edge_pct:+.2f}% with a {hit_rate:.1f}% hit rate"
        return f"recent sample beat buy-and-hold by {edge_pct:+.2f}%"
    return f"recent sample lagged buy-and-hold by {edge_pct:+.2f}%"


def _build_trend_snapshot(
    close: float,
    ema12: Optional[float],
    ema26: Optional[float],
    sma20: Optional[float],
    summary: dict,
    ema12_values: list[float],
) -> dict:
    score = 0.0
    reasons = []

    if ema12 is not None:
        if close >= ema12:
            score += 1.0
            reasons.append("price is above EMA12")
        else:
            score -= 1.0
            reasons.append("price is below EMA12")

    if ema26 is not None:
        if close >= ema26:
            score += 1.0
            reasons.append("price is above EMA26")
        else:
            score -= 1.0
            reasons.append("price is below EMA26")

    if ema12 is not None and ema26 is not None:
        if ema12 >= ema26:
            score += 1.0
            reasons.append("fast trend is above slow trend")
        else:
            score -= 1.0
            reasons.append("fast trend is below slow trend")

    if sma20 is not None:
        if close >= sma20:
            score += 0.5
            reasons.append("price is holding above SMA20")
        else:
            score -= 0.5
            reasons.append("price is trading below SMA20")

    slope = _recent_slope(ema12_values, lookback=5)
    if slope is not None:
        if slope > 0:
            score += 1.0
            reasons.append("EMA12 slope is rising")
        elif slope < 0:
            score -= 1.0
            reasons.append("EMA12 slope is falling")

    if summary["change_pct"] > 0:
        score += 0.5
    elif summary["change_pct"] < 0:
        score -= 0.5

    if score >= 3:
        label, tone = "Strong Uptrend", "positive"
    elif score >= 1:
        label, tone = "Bullish", "positive"
    elif score <= -3:
        label, tone = "Strong Downtrend", "negative"
    elif score <= -1:
        label, tone = "Bearish", "negative"
    else:
        label, tone = "Neutral", "neutral"

    score_pct = max(0.0, min(100.0, round(50.0 + score * 10.0, 1)))
    top_reasons = ", ".join(reasons[:2]) if reasons else "trend inputs are still warming up"
    return {
        "label": label,
        "tone": tone,
        "score_raw": round(score, 3),
        "score_pct": score_pct,
        "summary": f"{label}. Right now {top_reasons}.",
    }


def _build_momentum_snapshot(
    rsi: Optional[float],
    rsi_values: list[float],
    macd: Optional[dict],
) -> dict:
    score = 0.0
    details = []

    if rsi is not None:
        if rsi >= 72:
            score += 2.0
            details.append(f"RSI is hot at {rsi:.1f}")
        elif rsi >= 60:
            score += 1.0
            details.append(f"RSI is constructive at {rsi:.1f}")
        elif rsi <= 28:
            score -= 2.0
            details.append(f"RSI is washed out at {rsi:.1f}")
        elif rsi <= 40:
            score -= 1.0
            details.append(f"RSI is soft at {rsi:.1f}")
        else:
            details.append(f"RSI is balanced at {rsi:.1f}")

    if macd is not None:
        histogram = float(macd["histogram"])
        line = float(macd["macd"])
        signal = float(macd["signal"])
        if histogram > 0:
            score += 1.0
            details.append("MACD histogram is above zero")
        elif histogram < 0:
            score -= 1.0
            details.append("MACD histogram is below zero")

        if line > signal:
            score += 0.5
        elif line < signal:
            score -= 0.5

    if score >= 2.5:
        label, tone = "Heated", "caution"
    elif score >= 1.0:
        label, tone = "Building", "positive"
    elif score <= -2.5:
        label, tone = "Washed Out", "negative"
    elif score <= -1.0:
        label, tone = "Fading", "negative"
    else:
        label, tone = "Balanced", "neutral"

    rsi_percentile = _percentile_rank(rsi_values, rsi)
    percentile_text = (
        f" That puts the current RSI around the {rsi_percentile:.0f}th percentile for the loaded window."
        if rsi_percentile is not None
        else ""
    )
    detail_text = ", ".join(details[:2]) if details else "Momentum inputs are still warming up"
    return {
        "label": label,
        "tone": tone,
        "rsi": rsi,
        "rsi_percentile": rsi_percentile,
        "summary": f"{detail_text}.{percentile_text}",
    }


def _build_volatility_snapshot(
    candles: list[dict],
    close: float,
    bollinger: Optional[dict],
    asset_class: str,
    timeframe: str,
) -> dict:
    atr = _compute_atr(candles)
    atr_pct = _pct_distance(atr, close)
    bandwidth = float(bollinger["bandwidth"]) if bollinger and bollinger.get("bandwidth") is not None else None
    low_threshold, high_threshold = _atr_thresholds(asset_class, timeframe)

    if atr_pct is None and bandwidth is None:
        label, tone = "Warming Up", "neutral"
        summary = "Not enough range history yet to classify volatility."
    elif atr_pct is not None and atr_pct < low_threshold:
        label, tone = "Compressed", "neutral"
        summary = f"ATR is running at {atr_pct:.2f}% of price, which is quiet for this window."
    elif atr_pct is not None and atr_pct < high_threshold:
        label, tone = "Normal", "info"
        summary = f"ATR is {atr_pct:.2f}% of price, which looks like a normal trading range for this timeframe."
    else:
        label, tone = "Expanded", "caution"
        atr_text = f"{atr_pct:.2f}%" if atr_pct is not None else "an elevated level"
        summary = f"Range is expanded. ATR is running near {atr_text} of price, so expect wider swings and looser stops."

    if bandwidth is not None:
        summary += f" Bollinger bandwidth is {bandwidth:.2f}%."

    return {
        "label": label,
        "tone": tone,
        "atr": atr,
        "atr_pct": atr_pct,
        "bandwidth_pct": bandwidth,
        "summary": summary,
    }


def _build_participation_snapshot(candles: list[dict]) -> dict:
    if not candles:
        return {
            "label": "Waiting",
            "tone": "neutral",
            "relative_volume": None,
            "summary": "No candles available yet.",
        }

    current_volume = float(candles[-1].get("volume") or 0.0)
    baseline = [float(candle.get("volume") or 0.0) for candle in candles[-21:-1] if (candle.get("volume") or 0.0) > 0]
    if not baseline:
        return {
            "label": "Waiting",
            "tone": "neutral",
            "relative_volume": None,
            "summary": "Need more volume history before relative participation can be judged.",
        }

    avg_volume = sum(baseline) / len(baseline)
    relative_volume = current_volume / avg_volume if avg_volume else None
    if relative_volume is None:
        label, tone = "Waiting", "neutral"
        summary = "Need more volume history before relative participation can be judged."
    elif relative_volume < 0.8:
        label, tone = "Light", "neutral"
        summary = f"Current bar volume is only {relative_volume:.2f}x the recent average, so conviction looks light."
    elif relative_volume < 1.3:
        label, tone = "Healthy", "positive"
        summary = f"Current bar volume is {relative_volume:.2f}x the recent average, which looks healthy."
    elif relative_volume < 2.0:
        label, tone = "Elevated", "positive"
        summary = f"Current bar volume is {relative_volume:.2f}x the recent average, so participation is clearly above normal."
    else:
        label, tone = "Surging", "caution"
        summary = f"Current bar volume is {relative_volume:.2f}x the recent average, which usually means event-driven or breakout conditions."

    return {
        "label": label,
        "tone": tone,
        "relative_volume": round(relative_volume, 4) if relative_volume is not None else None,
        "summary": summary,
    }


def _build_stretch_snapshot(
    close: float,
    ema12: Optional[float],
    vwap: Optional[float],
    bollinger: Optional[dict],
    asset_class: str,
    timeframe: str,
) -> dict:
    distance_to_vwap_pct = _pct_distance(close - vwap, vwap) if vwap is not None else None
    distance_to_ema12_pct = _pct_distance(close - ema12, ema12) if ema12 is not None else None
    percent_b = None
    if bollinger and bollinger.get("upper") is not None and bollinger.get("lower") is not None:
        upper = float(bollinger["upper"])
        lower = float(bollinger["lower"])
        width = upper - lower
        if width > 0:
            percent_b = round(((close - lower) / width) * 100.0, 2)

    if timeframe in {"1m", "5m"}:
        extension_threshold = 0.6
    elif timeframe in {"15m", "30m", "1h", "2h"}:
        extension_threshold = 1.2
    elif timeframe in {"4h", "6h", "12h"}:
        extension_threshold = 2.0
    else:
        extension_threshold = 3.5
    if asset_class == "crypto":
        extension_threshold *= 1.35

    if percent_b is not None and percent_b >= 90:
        label, tone = "Extended High", "caution"
    elif percent_b is not None and percent_b <= 10:
        label, tone = "Pressed Low", "negative"
    elif distance_to_vwap_pct is not None and abs(distance_to_vwap_pct) >= extension_threshold:
        label, tone = "Extended", "caution"
    else:
        label, tone = "Near Fair Value", "info"

    if distance_to_vwap_pct is not None:
        basis_text = f"{distance_to_vwap_pct:+.2f}% vs VWAP"
    elif distance_to_ema12_pct is not None:
        basis_text = f"{distance_to_ema12_pct:+.2f}% vs EMA12"
    else:
        basis_text = "Warming up"

    if label == "Near Fair Value":
        summary = "Price is still trading close to its short-term anchor instead of getting dramatically stretched."
    elif label == "Extended High":
        summary = "Price is crowding the upper end of its recent range. Strong moves can keep running, but mean reversion risk is higher."
    elif label == "Pressed Low":
        summary = "Price is crowding the lower end of its recent range. That can mark weakness or a mean-reversion setup, depending on trend."
    else:
        summary = "Price is moving away from its short-term anchor, so chasing here carries more stretch risk than buying a pullback."

    return {
        "label": label,
        "tone": tone,
        "distance_to_vwap_pct": distance_to_vwap_pct,
        "distance_to_ema12_pct": distance_to_ema12_pct,
        "percent_b": percent_b,
        "summary": summary,
        "value": basis_text,
    }


def _build_indicator_guides(
    asset_class: str,
    timeframe: str,
    trend_score: float,
    rsi: Optional[float],
    rsi_percentile: Optional[float],
    macd: Optional[dict],
    close: Optional[float],
    atr_pct: Optional[float],
    relative_volume: Optional[float],
    vwap_distance_pct: Optional[float],
    bollinger: Optional[dict],
) -> dict:
    rsi_profile = _rsi_profile(asset_class, timeframe, trend_score)
    timeframe_note = _indicator_timeframe_note(timeframe)
    asset_note = _asset_class_note(asset_class)

    macd_summary = "Warming up"
    if macd is not None:
        line = float(macd["macd"])
        signal = float(macd["signal"])
        histogram = float(macd["histogram"])
        relation = "above" if line >= signal else "below"
        zero_side = "above zero" if line >= 0 else "below zero"
        macd_summary = f"MACD is {relation} signal and {zero_side}; histogram is {histogram:+.4f}."

    vwap_band = _vwap_balanced_band(asset_class, timeframe)
    vwap_summary = (
        f"Price is {vwap_distance_pct:+.2f}% from VWAP."
        if vwap_distance_pct is not None
        else "VWAP needs more volume history before it becomes reliable."
    )
    vwap_range = f"Balanced action usually stays inside about +/-{vwap_band:.1f}% of VWAP on this window."

    percent_b = None
    bandwidth = None
    if bollinger is not None:
        bandwidth = float(bollinger.get("bandwidth")) if bollinger.get("bandwidth") is not None else None
        upper = bollinger.get("upper")
        lower = bollinger.get("lower")
        middle = bollinger.get("middle")
        if upper is not None and lower is not None and middle is not None:
            width = float(upper) - float(lower)
            if width > 0:
                if close is not None:
                    percent_b = ((float(close) - float(lower)) / width) * 100

    atr_band = _atr_band(asset_class, timeframe)
    volume_range = _relative_volume_band(timeframe)
    bollinger_band = _bollinger_balanced_band(asset_class, timeframe)

    return {
        "rsi": {
            "key": "rsi",
            "title": "RSI 14",
            "tone": _rsi_tone(rsi, rsi_profile),
            "current": f"{rsi:.1f}" if rsi is not None else "Warming up",
            "good_range": f"Healthy band: {rsi_profile['preferred_band'][0]}-{rsi_profile['preferred_band'][1]} on this setup",
            "summary": (
                f"Current RSI is {rsi:.1f}. {rsi_profile['summary']}"
                + (
                    f" That is roughly the {rsi_percentile:.0f}th percentile of the loaded window."
                    if rsi_percentile is not None
                    else ""
                )
            ) if rsi is not None else rsi_profile["summary"],
            "why_range": rsi_profile["range_reason"],
            "timeframe_note": timeframe_note,
        },
        "macd": {
            "key": "macd",
            "title": "MACD",
            "tone": "positive" if macd and float(macd["histogram"]) > 0 else "negative" if macd and float(macd["histogram"]) < 0 else "neutral",
            "current": (
                f"{float(macd['histogram']):+.4f} hist"
                if macd is not None
                else "Warming up"
            ),
            "good_range": "Healthy read: align side of zero, side of signal, and histogram direction instead of chasing one number.",
            "summary": macd_summary,
            "why_range": (
                "MACD works best as a regime check. Above zero and above signal usually supports bullish continuation; "
                "below zero and below signal usually supports bearish continuation."
            ),
            "timeframe_note": (
                f"{timeframe_note} {asset_note} On faster windows, expect more signal flips before a move truly sticks."
            ),
        },
        "vwap": {
            "key": "vwap",
            "title": "VWAP",
            "tone": "positive" if vwap_distance_pct is not None and vwap_distance_pct >= 0 else "negative" if vwap_distance_pct is not None else "neutral",
            "current": f"{vwap_distance_pct:+.2f}% vs VWAP" if vwap_distance_pct is not None else "Warming up",
            "good_range": vwap_range,
            "summary": vwap_summary,
            "why_range": (
                "VWAP is the session's fair-value anchor. Staying near it keeps trade location cleaner; "
                "stretching too far away raises chase risk unless momentum is exceptional."
            ),
            "timeframe_note": (
                "VWAP matters most on intraday and short swing windows. On multi-session and weekly charts it is better used as a stretch gauge than as a hard support line."
            ),
        },
        "bollinger": {
            "key": "bollinger",
            "title": "Bollinger Bands",
            "tone": "caution" if bandwidth is not None and bandwidth > 8 else "info",
            "current": (
                f"%B {percent_b:.0f} • BW {bandwidth:.2f}%"
                if percent_b is not None and bandwidth is not None
                else f"{bandwidth:.2f}% bandwidth"
                if bandwidth is not None
                else "Warming up"
            ),
            "good_range": f"Balanced trade usually keeps %B around {bollinger_band[0]}-{bollinger_band[1]} before extension starts to matter.",
            "summary": (
                f"Bandwidth is {bandwidth:.2f}%. Use Bollinger mostly for stretch and compression, not for trend by itself."
                if bandwidth is not None
                else "Bollinger needs more history before it can classify squeeze vs expansion."
            ),
            "why_range": (
                "Bands are best for spotting squeeze versus expansion. Riding outside a band can be healthy in trend, "
                "but repeated closes outside the bands usually mean the move is stretched."
            ),
            "timeframe_note": (
                f"{timeframe_note} {asset_note} Slower windows can 'walk the band' longer than fast charts before actually mean reverting."
            ),
        },
        "atr": {
            "key": "atr",
            "title": "ATR %",
            "tone": "positive" if atr_pct is not None and atr_band[0] <= atr_pct <= atr_band[1] else "caution" if atr_pct is not None else "neutral",
            "current": f"{atr_pct:.2f}% ATR" if atr_pct is not None else "Warming up",
            "good_range": f"Typical working range: about {atr_band[0]:.2f}-{atr_band[1]:.2f}% on this asset class and window.",
            "summary": (
                f"ATR is running at {atr_pct:.2f}% of price."
                if atr_pct is not None
                else "ATR needs enough candles before it can judge whether the tape is calm or expanding."
            ),
            "why_range": (
                "ATR tells you how much room price normally needs. Using a normal ATR band helps size stops and expectations so normal noise does not feel like signal."
            ),
            "timeframe_note": f"{timeframe_note} {asset_note}",
        },
        "volume": {
            "key": "volume",
            "title": "Relative Volume",
            "tone": "positive" if relative_volume is not None and relative_volume >= 1.0 else "neutral" if relative_volume is not None else "neutral",
            "current": f"{relative_volume:.2f}x RVOL" if relative_volume is not None else "Warming up",
            "good_range": f"Normal participation is roughly {volume_range[0]:.1f}x-{volume_range[1]:.1f}x. Above 1.5x usually means the move has real attention.",
            "summary": (
                f"Relative volume is {relative_volume:.2f}x versus the recent baseline."
                if relative_volume is not None
                else "Relative volume needs a little more history before the baseline is trustworthy."
            ),
            "why_range": (
                "Volume helps separate real moves from flimsy ones. Breakouts on healthy relative volume are usually more durable than moves on thin participation."
            ),
            "timeframe_note": (
                "Fast windows can print one-off volume spikes, while multi-hour, daily, and weekly participation usually carries more conviction."
            ),
        },
    }


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


def _build_ai_overview(
    symbol: str,
    timeframe: str,
    timeframe_context: dict,
    trend: dict,
    momentum: dict,
    volatility: dict,
    participation: dict,
    stretch: dict,
) -> dict:
    trend_score = float(trend.get("score_raw") or 0.0)
    confidence = 48.0 + min(abs(trend_score), 4.5) * 8.0

    if trend["tone"] == "positive" and momentum["tone"] in {"positive", "caution"}:
        confidence += 7.0
    elif trend["tone"] == "negative" and momentum["tone"] == "negative":
        confidence += 7.0
    else:
        confidence -= 4.0

    if participation["tone"] == "positive":
        confidence += 4.0
    elif participation["tone"] == "caution":
        confidence += 2.0

    if stretch["tone"] == "caution":
        confidence -= 6.0
    if volatility["tone"] == "caution":
        confidence -= 3.0

    confidence_pct = max(35, min(89, round(confidence)))

    if trend["tone"] == "positive" and momentum["tone"] in {"positive", "caution"}:
        if stretch["tone"] == "caution":
            stance = "Constructive, But Wait For Better Location"
            tone = "caution"
            action = "Trend and momentum still lean higher, but the current candle is already stretched. Let price pull back toward EMA12 or VWAP before getting aggressive."
            watch_next = "Watch whether pullbacks hold above fast trend support and whether RSI stays above its healthy floor."
        else:
            stance = "Constructive Long Bias"
            tone = "positive"
            action = "The tape is aligned to the upside. Favor continuation or buy-the-dip thinking over fading strength, especially if volume stays healthy."
            watch_next = "Watch for pullbacks that respect EMA12, VWAP, or the mid-band instead of immediately losing them."
    elif trend["tone"] == "negative" and momentum["tone"] == "negative":
        if momentum["label"] == "Washed Out" or stretch["label"] == "Pressed Low":
            stance = "Weak Tape With Bounce Risk"
            tone = "negative"
            action = "Primary conditions still lean weak, but the move is washed out enough that chasing the downside late can get sloppy. Treat pops as relief-rally risk until structure improves."
            watch_next = "Watch whether rebounds fail near VWAP or EMA12, and whether RSI can reclaim the middle of its working band."
        else:
            stance = "Defensive / Weak Structure"
            tone = "negative"
            action = "Conditions still favor caution. Avoid forcing bottom calls until momentum and trend stop pointing in the same weak direction."
            watch_next = "Watch for trend repair first: price back above fast averages, MACD improving, and participation not fading."
    else:
        stance = "Wait For Confirmation"
        tone = "info"
        action = "There is movement here, but not a clean edge yet. Let price prove direction before committing to a strong bias."
        watch_next = "Watch whether VWAP, EMA12, and MACD all start leaning the same way instead of sending mixed signals."

    if volatility["tone"] == "caution":
        risk_note = "Volatility is expanded for this window, so expect wider swings and give setups more room than usual."
    elif timeframe in {"1m", "5m", "15m", "30m"}:
        risk_note = "This is still a relatively fast window, so noise and false breaks matter more than they do on the slower swing charts."
    else:
        risk_note = "This window is cleaner than fast tape, but stretched moves can still persist longer than expected before mean reverting."

    summary = (
        f"{symbol} on {timeframe} currently reads as {stance.lower()}. "
        f"{timeframe_context.get('summary', '')}"
    ).strip()

    return {
        "title": "Meridian Read",
        "stance": stance,
        "tone": tone,
        "confidence_pct": confidence_pct,
        "summary": summary,
        "action": action,
        "risk_note": risk_note,
        "watch_next": watch_next,
        "disclaimer": "Educational market context only, not personal financial advice.",
    }


def _compress_matrix_row(payload: dict) -> dict:
    insights = payload["insights"]
    summary = payload["summary"]
    return {
        "timeframe": payload["timeframe"],
        "label": insights["timeframe_context"]["label"],
        "available": True,
        "headline": insights["headline"],
        "summary": {
            "close": summary["close"],
            "change_pct": summary["change_pct"],
        },
        "trend": {
            "label": insights["trend"]["label"],
            "tone": insights["trend"]["tone"],
            "score_pct": insights["trend"]["score_pct"],
        },
        "momentum": {
            "label": insights["momentum"]["label"],
            "tone": insights["momentum"]["tone"],
            "rsi": insights["momentum"]["rsi"],
        },
        "participation": {
            "label": insights["participation"]["label"],
            "tone": insights["participation"]["tone"],
            "relative_volume": insights["participation"]["relative_volume"],
        },
        "stretch": {
            "label": insights["stretch"]["label"],
            "tone": insights["stretch"]["tone"],
            "value": insights["stretch"]["value"],
        },
    }


def _rsi_profile(asset_class: str, timeframe: str, trend_score: float) -> dict:
    if trend_score >= 1:
        preferred_band = (45, 82) if asset_class == "crypto" else (40, 80)
        summary = "Strong trends often hold a higher RSI floor, so pullbacks into the low-40s can still be healthy."
        regime_note = "When trend is strong, RSI staying elevated is usually a feature of persistence, not a warning by itself."
    elif trend_score <= -1:
        preferred_band = (18, 58) if asset_class == "crypto" else (20, 60)
        summary = "Weak trends often fail before RSI reaches classic overbought readings, so rallies into the 50s can already stall."
        regime_note = "In weak trends, the market often cannot sustain high RSI readings, so lower ceilings matter more than classic 70 tags."
    else:
        preferred_band = (35, 72) if asset_class == "crypto" and timeframe in {"1m", "5m", "15m", "30m"} else (30, 70)
        summary = "In neutral conditions, classic 30-70 RSI framing still works reasonably well."
        regime_note = "When trend is mixed, RSI mean reversion bands matter more because momentum does not have a strong directional bias."

    timeframe_note = _indicator_timeframe_note(timeframe)
    if asset_class == "crypto" and timeframe in {"1m", "5m", "15m", "30m"}:
        summary += " Crypto on fast timeframes tends to overshoot more than large-cap stocks, so expect wider swings."

    return {
        "preferred_band": preferred_band,
        "summary": summary,
        "range_reason": f"{regime_note} {timeframe_note} {_asset_class_note(asset_class)}",
    }


def _rsi_tone(rsi: Optional[float], profile: dict) -> str:
    if rsi is None:
        return "neutral"
    low, high = profile["preferred_band"]
    if rsi > high:
        return "caution"
    if rsi < low:
        return "negative"
    return "positive"


def _last_indicator_value(series: list[dict], field: str = "value") -> Optional[float]:
    for entry in reversed(series):
        value = entry.get(field)
        if value is not None:
            return float(value)
    return None


def _last_indicator_entry(series: list[dict]) -> Optional[dict]:
    return series[-1] if series else None


def _series_values(series: list[dict], field: str = "value") -> list[float]:
    values = []
    for entry in series:
        value = entry.get(field)
        if value is not None:
            values.append(float(value))
    return values


def _indicator_timeframe_note(timeframe: str) -> str:
    if timeframe in {"1m", "5m"}:
        return "Fast windows are useful for timing, but they also whipsaw the most, so healthy ranges need a little more breathing room."
    if timeframe in {"15m", "30m", "1h", "2h"}:
        return "Mid-speed windows balance signal and noise, so range guidance tends to be more dependable here."
    if timeframe in {"4h", "6h", "12h"}:
        return f"{timeframe} structure carries across multiple sessions, so indicators can stay stretched longer before reversing."
    return "Daily structure moves slowly, so trends can hold a regime for longer and single prints matter less than repeated closes."


def _asset_class_note(asset_class: str) -> str:
    if asset_class == "crypto":
        return "Crypto usually overshoots more than large-cap stocks, especially on short windows and around session transitions."
    return "Stocks usually mean revert a little more cleanly unless earnings, macro, or open/close flows distort the tape."


def _vwap_balanced_band(asset_class: str, timeframe: str) -> float:
    if timeframe in {"1m", "5m"}:
        return 0.9 if asset_class == "crypto" else 0.5
    if timeframe in {"15m", "30m"}:
        return 1.2 if asset_class == "crypto" else 0.8
    if timeframe in {"1h", "2h"}:
        return 1.8 if asset_class == "crypto" else 1.2
    if timeframe in {"4h", "6h", "12h"}:
        return 2.8 if asset_class == "crypto" else 2.0
    return 4.0 if asset_class == "crypto" else 3.0


def _bollinger_balanced_band(asset_class: str, timeframe: str) -> tuple[int, int]:
    if asset_class == "crypto" and timeframe in {"1m", "5m", "15m", "30m"}:
        return (15, 85)
    if timeframe in {"4h", "6h", "12h", "1d", "2d", "1w"}:
        return (25, 75)
    return (20, 80)


def _atr_band(asset_class: str, timeframe: str) -> tuple[float, float]:
    if asset_class == "crypto":
        if timeframe in {"1m", "5m"}:
            return (0.35, 1.80)
        if timeframe in {"15m", "30m", "1h", "2h"}:
            return (0.80, 3.50)
        if timeframe in {"4h", "6h", "12h"}:
            return (1.50, 5.50)
        return (2.00, 8.00)

    if timeframe in {"1m", "5m"}:
        return (0.15, 0.90)
    if timeframe in {"15m", "30m", "1h", "2h"}:
        return (0.35, 1.80)
    if timeframe in {"4h", "6h", "12h"}:
        return (0.70, 3.20)
    return (1.00, 5.00)


def _relative_volume_band(timeframe: str) -> tuple[float, float]:
    if timeframe in {"1m", "5m"}:
        return (0.8, 1.2)
    if timeframe in {"15m", "30m", "1h", "2h"}:
        return (0.85, 1.25)
    return (0.9, 1.35)


def _recent_slope(values: list[float], lookback: int = 5) -> Optional[float]:
    if len(values) < 2:
        return None
    window = values[-(lookback + 1):]
    if len(window) < 2:
        return None
    return window[-1] - window[0]


def _percentile_rank(values: list[float], current: Optional[float]) -> Optional[float]:
    if current is None or not values:
        return None
    ordered = sorted(values)
    count = sum(1 for value in ordered if value <= current)
    return round((count / len(ordered)) * 100.0, 1)


def _compute_atr(candles: list[dict], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None

    true_ranges = []
    previous_close = candles[0]["close"]
    for candle in candles[1:]:
        high = float(candle["high"])
        low = float(candle["low"])
        tr = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close),
        )
        true_ranges.append(tr)
        previous_close = float(candle["close"])

    if len(true_ranges) < period:
        return None
    window = true_ranges[-period:]
    return round(sum(window) / len(window), 6)


def _pct_distance(value: Optional[float], anchor: Optional[float]) -> Optional[float]:
    if value is None or anchor in (None, 0):
        return None
    return round((float(value) / float(anchor)) * 100.0, 4)


def _atr_thresholds(asset_class: str, timeframe: str) -> tuple[float, float]:
    base = {
        "1m": (0.18, 0.45),
        "5m": (0.35, 0.9),
        "15m": (0.6, 1.5),
        "30m": (0.8, 1.9),
        "1h": (1.2, 2.8),
        "2h": (1.5, 3.2),
        "4h": (2.0, 4.5),
        "6h": (2.3, 4.9),
        "12h": (2.7, 5.4),
        "1d": (3.0, 6.0),
        "2d": (3.5, 6.8),
        "1w": (4.2, 7.8),
    }.get(timeframe, (1.0, 2.5))
    multiplier = 1.35 if asset_class == "crypto" else 1.0
    return round(base[0] * multiplier, 4), round(base[1] * multiplier, 4)
