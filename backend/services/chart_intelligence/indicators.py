from typing import Optional

from backend.indicators import BollingerBands, EMA, MACD, RSI, SMA, VWAP

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


def _is_equity_symbol(symbol: str) -> bool:
    return "-" not in symbol and "/" not in symbol


def _asset_class(symbol: str) -> str:
    return "stock" if _is_equity_symbol(symbol) else "crypto"


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
