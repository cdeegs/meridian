from datetime import datetime, timedelta, timezone

import pytest

from backend.services.chart_intelligence import (
    _build_momentum_snapshot,
    _build_participation_snapshot,
    _build_stretch_snapshot,
    _build_trend_snapshot,
    _build_volatility_snapshot,
    _compute_indicator_series,
    _detect_regime,
    _score_momentum,
    _score_stretch,
    _score_trend,
    _score_volatility,
)


def _make_candles(
    *,
    count: int = 60,
    start_price: float = 100.0,
    step: float = 1.0,
    range_width: float = 2.0,
    volume_start: float = 1000.0,
    volume_step: float = 25.0,
):
    base = datetime(2026, 3, 1, 14, 30, tzinfo=timezone.utc)
    candles = []
    price = start_price

    for index in range(count):
        open_price = price
        close_price = price + step
        high = max(open_price, close_price) + (range_width / 2)
        low = min(open_price, close_price) - (range_width / 2)
        candles.append(
            {
                "time": base + timedelta(minutes=index),
                "open": round(open_price, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(close_price, 4),
                "volume": round(volume_start + (index * volume_step), 4),
            }
        )
        price = close_price

    return candles


def _change_summary(candles: list[dict]) -> dict:
    first = candles[0]
    last = candles[-1]
    change = last["close"] - first["open"]
    return {
        "change_pct": (change / first["open"] * 100.0) if first["open"] else 0.0,
    }


@pytest.fixture
def bullish_candles():
    return _make_candles(step=1.2, range_width=2.4, volume_step=35.0)


@pytest.fixture
def bearish_candles():
    return _make_candles(start_price=180.0, step=-1.2, range_width=2.4, volume_step=35.0)


@pytest.fixture
def tight_range_candles():
    return _make_candles(step=0.08, range_width=0.24, volume_step=6.0)


@pytest.fixture
def volatile_candles():
    return _make_candles(step=0.9, range_width=8.0, volume_step=60.0)


@pytest.fixture
def flat_candles():
    base = datetime(2026, 3, 1, 14, 30, tzinfo=timezone.utc)
    candles = []
    price = 100.0

    for index in range(60):
        open_price = price
        close_price = 100.0 + (0.04 if index % 2 == 0 else -0.04)
        high = max(open_price, close_price) + 0.08
        low = min(open_price, close_price) - 0.08
        candles.append(
            {
                "time": base + timedelta(minutes=index),
                "open": round(open_price, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(close_price, 4),
                "volume": round(1000.0 + (index * 3.0), 4),
            }
        )
        price = close_price

    return candles


def test_score_trend_returns_shape_and_varies_with_bullish_vs_bearish_sequences(
    bullish_candles,
    bearish_candles,
):
    bullish = _score_trend(bullish_candles)
    bearish = _score_trend(bearish_candles)

    for snapshot in (bullish, bearish):
        assert isinstance(snapshot["score"], int)
        assert isinstance(snapshot["label"], str)
        assert isinstance(snapshot["summary"], str)
        assert 1 <= snapshot["score"] <= 5

    assert bullish["score"] > bearish["score"]


def test_score_momentum_returns_shape_and_scores_uptrend_with_rsi_above_sixty(
    bullish_candles,
):
    indicators = _compute_indicator_series(bullish_candles)
    latest_rsi = indicators["rsi_14"][-1]["value"]
    snapshot = _score_momentum(bullish_candles, indicators=indicators)

    assert latest_rsi > 60
    assert isinstance(snapshot["score"], int)
    assert isinstance(snapshot["label"], str)
    assert isinstance(snapshot["summary"], str)
    assert snapshot["score"] >= 3


def test_score_volatility_returns_shape_and_tighter_ranges_score_lower(
    tight_range_candles,
    volatile_candles,
):
    quiet = _score_volatility(tight_range_candles, symbol="BTC-USD", timeframe="1h")
    active = _score_volatility(volatile_candles, symbol="BTC-USD", timeframe="1h")

    for snapshot in (quiet, active):
        assert isinstance(snapshot["score"], int)
        assert isinstance(snapshot["label"], str)
        assert isinstance(snapshot["summary"], str)
        assert 1 <= snapshot["score"] <= 5

    assert quiet["score"] < active["score"]


def test_score_stretch_returns_shape_and_near_anchor_candles_score_low(flat_candles):
    snapshot = _score_stretch(flat_candles, symbol="BTC-USD", timeframe="1h")

    assert isinstance(snapshot["score"], int)
    assert isinstance(snapshot["label"], str)
    assert isinstance(snapshot["summary"], str)
    assert snapshot["score"] <= 2


def test_detect_regime_returns_label_and_summary(bullish_candles):
    indicators = _compute_indicator_series(bullish_candles)
    close = bullish_candles[-1]["close"]
    trend = _build_trend_snapshot(
        close=close,
        ema12=indicators["ema_12"][-1]["value"],
        ema26=indicators["ema_26"][-1]["value"],
        sma20=indicators["sma_20"][-1]["value"],
        summary=_change_summary(bullish_candles),
        ema12_values=[entry["value"] for entry in indicators["ema_12"]],
    )
    momentum = _build_momentum_snapshot(
        rsi=indicators["rsi_14"][-1]["value"],
        rsi_values=[entry["value"] for entry in indicators["rsi_14"]],
        macd=indicators["macd"][-1],
    )
    volatility = _build_volatility_snapshot(
        candles=bullish_candles,
        close=close,
        bollinger=indicators["bollinger_20"][-1],
        asset_class="crypto",
        timeframe="1h",
    )
    participation = _build_participation_snapshot(bullish_candles)
    stretch = _build_stretch_snapshot(
        close=close,
        ema12=indicators["ema_12"][-1]["value"],
        vwap=indicators["vwap"][-1]["value"],
        bollinger=indicators["bollinger_20"][-1],
        asset_class="crypto",
        timeframe="1h",
    )

    regime = _detect_regime(trend, momentum, volatility, participation, stretch)

    assert "label" in regime
    assert "summary" in regime
    assert isinstance(regime["label"], str)
    assert isinstance(regime["summary"], str)


def test_compute_indicator_series_returns_expected_keys(bullish_candles):
    indicators = _compute_indicator_series(bullish_candles)

    assert "sma_20" in indicators
    assert "ema_12" in indicators
    assert "rsi_14" in indicators
    assert "macd" in indicators
    assert "bollinger_20" in indicators
    assert "vwap" in indicators
    assert indicators["sma_20"]
    assert indicators["ema_12"]
    assert indicators["rsi_14"]
    assert indicators["macd"]
    assert indicators["bollinger_20"]
    assert indicators["vwap"]
