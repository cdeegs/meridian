from typing import Optional

from .indicators import (
    _asset_class,
    _atr_thresholds,
    _compute_atr,
    _compute_indicator_series,
    _last_indicator_entry,
    _last_indicator_value,
    _pct_distance,
    _percentile_rank,
    _recent_slope,
    _series_values,
)


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


def _score_from_pct(score_pct: Optional[float]) -> int:
    if score_pct is None:
        return 1
    return max(1, min(5, int(round(float(score_pct) / 25.0)) + 1))


def _score_trend(candles: list[dict], indicators: Optional[dict] = None) -> dict:
    if not candles:
        return {"score": 1, "label": "Warming Up", "summary": "Not enough candles to score trend."}

    indicators = indicators or _compute_indicator_series(candles)
    first = candles[0]
    last = candles[-1]
    change = last["close"] - first["open"]
    change_pct = (change / first["open"] * 100.0) if first["open"] else 0.0
    snapshot = _build_trend_snapshot(
        close=float(last["close"]),
        ema12=_last_indicator_value(indicators.get("ema_12", [])),
        ema26=_last_indicator_value(indicators.get("ema_26", [])),
        sma20=_last_indicator_value(indicators.get("sma_20", [])),
        summary={"change_pct": round(change_pct, 4)},
        ema12_values=_series_values(indicators.get("ema_12", [])),
    )
    return {
        "score": _score_from_pct(snapshot.get("score_pct")),
        "label": snapshot["label"],
        "summary": snapshot["summary"],
    }


def _score_momentum(candles: list[dict], indicators: Optional[dict] = None) -> dict:
    indicators = indicators or _compute_indicator_series(candles)
    snapshot = _build_momentum_snapshot(
        rsi=_last_indicator_value(indicators.get("rsi_14", [])),
        rsi_values=_series_values(indicators.get("rsi_14", [])),
        macd=_last_indicator_entry(indicators.get("macd", [])),
    )
    label_scores = {
        "Washed Out": 1,
        "Fading": 2,
        "Balanced": 3,
        "Building": 4,
        "Heated": 5,
    }
    return {
        "score": label_scores.get(snapshot["label"], 3),
        "label": snapshot["label"],
        "summary": snapshot["summary"],
    }


def _score_volatility(
    candles: list[dict],
    symbol: str = "BTC-USD",
    timeframe: str = "1h",
    indicators: Optional[dict] = None,
) -> dict:
    if not candles:
        return {"score": 1, "label": "Warming Up", "summary": "Not enough candles to score volatility."}

    indicators = indicators or _compute_indicator_series(candles)
    snapshot = _build_volatility_snapshot(
        candles=candles,
        close=float(candles[-1]["close"]),
        bollinger=_last_indicator_entry(indicators.get("bollinger_20", [])),
        asset_class=_asset_class(symbol.upper()),
        timeframe=timeframe,
    )
    label_scores = {
        "Compressed": 1,
        "Normal": 3,
        "Expanded": 5,
        "Warming Up": 1,
    }
    return {
        "score": label_scores.get(snapshot["label"], 3),
        "label": snapshot["label"],
        "summary": snapshot["summary"],
    }


def _score_participation(candles: list[dict]) -> dict:
    snapshot = _build_participation_snapshot(candles)
    label_scores = {
        "Waiting": 1,
        "Light": 2,
        "Healthy": 3,
        "Elevated": 4,
        "Surging": 5,
    }
    return {
        "score": label_scores.get(snapshot["label"], 3),
        "label": snapshot["label"],
        "summary": snapshot["summary"],
    }


def _score_stretch(
    candles: list[dict],
    symbol: str = "BTC-USD",
    timeframe: str = "1h",
    indicators: Optional[dict] = None,
) -> dict:
    if not candles:
        return {"score": 1, "label": "Warming Up", "summary": "Not enough candles to score stretch."}

    indicators = indicators or _compute_indicator_series(candles)
    snapshot = _build_stretch_snapshot(
        close=float(candles[-1]["close"]),
        ema12=_last_indicator_value(indicators.get("ema_12", [])),
        vwap=_last_indicator_value(indicators.get("vwap", [])),
        bollinger=_last_indicator_entry(indicators.get("bollinger_20", [])),
        asset_class=_asset_class(symbol.upper()),
        timeframe=timeframe,
    )
    label_scores = {
        "Near Fair Value": 1,
        "Extended": 4,
        "Extended High": 5,
        "Pressed Low": 5,
    }
    return {
        "score": label_scores.get(snapshot["label"], 3),
        "label": snapshot["label"],
        "summary": snapshot["summary"],
    }


def _detect_regime(
    trend: dict,
    momentum: dict,
    volatility: dict,
    participation: dict,
    stretch: dict,
) -> dict:
    return _build_market_regime(
        trend=trend,
        momentum=momentum,
        volatility=volatility,
        participation=participation,
        stretch=stretch,
    )
