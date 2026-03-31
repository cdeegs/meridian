from datetime import datetime

from fastapi import HTTPException

from .indicators import (
    _compute_indicator_series,
)
from .narrative import _build_insights, _build_summary


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


build_chart_payload = _build_chart_payload
