from typing import Optional

from .indicators import (
    _TIMEFRAME_CONTEXT,
    _asset_class,
    _asset_class_note,
    _atr_band,
    _bollinger_balanced_band,
    _indicator_timeframe_note,
    _last_indicator_entry,
    _last_indicator_value,
    _relative_volume_band,
    _rsi_profile,
    _rsi_tone,
    _series_values,
    _vwap_balanced_band,
)
from .profiles import _build_study_profiles, _profile_live_state, _rank_study_profiles
from .scoring import (
    _build_market_regime,
    _build_momentum_snapshot,
    _build_participation_snapshot,
    _build_stretch_snapshot,
    _build_trend_snapshot,
    _build_volatility_snapshot,
)


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
