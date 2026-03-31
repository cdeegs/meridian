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
