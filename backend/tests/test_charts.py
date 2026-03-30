from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from backend.routes import charts as charts_routes
from backend.routes.charts import (
    _build_chart_payload,
    build_study_profile_snapshot,
    get_chart_data,
    get_chart_matrix,
)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows
        self.params = None

    async def execute(self, statement, params):
        self.params = params
        return _FakeResult(self._rows)


@pytest.mark.asyncio
async def test_chart_route_uses_timedelta_bucket_width():
    start = datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 30, 13, 0, tzinfo=timezone.utc)
    db = _FakeSession(
        [
            SimpleNamespace(
                bucket=start,
                open=100.0,
                high=101.5,
                low=99.5,
                close=101.0,
                volume=1200.0,
                ticks=24,
            )
        ]
    )

    payload = await get_chart_data(
        "BTC-USD",
        timeframe="5m",
        start=start,
        end=end,
        limit=60,
        db=db,
    )

    assert db.params["bucket_width"] == timedelta(minutes=5)
    assert db.params["history_start"] == start - timedelta(minutes=5 * 120)
    assert db.params["history_limit"] == 180
    assert payload["symbol"] == "BTC-USD"
    assert payload["timeframe"] == "5m"
    assert len(payload["candles"]) == 1
    assert payload["coverage"]["history_candles"] == 1
    assert payload["summary"]["close"] == 101.0


@pytest.mark.asyncio
async def test_chart_route_supports_extended_timeframes():
    start = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc)
    db = _FakeSession(
        [
            SimpleNamespace(
                bucket=start,
                open=100.0,
                high=110.0,
                low=95.0,
                close=108.0,
                volume=5000.0,
                ticks=12,
            )
        ]
    )

    payload = await get_chart_data(
        "BTC-USD",
        timeframe="2d",
        start=start,
        end=end,
        limit=30,
        db=db,
    )

    assert db.params["bucket_width"] == timedelta(days=2)
    assert payload["timeframe"] == "2d"
    assert payload["summary"]["close"] == 108.0


@pytest.mark.asyncio
async def test_chart_route_rejects_invalid_window_order():
    start = datetime(2026, 3, 30, 14, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 30, 13, 0, tzinfo=timezone.utc)

    with pytest.raises(HTTPException) as exc_info:
        await get_chart_data(
            "BTC-USD",
            timeframe="5m",
            start=start,
            end=end,
            limit=60,
            db=_FakeSession([]),
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "start must be earlier than end"


@pytest.mark.asyncio
async def test_chart_route_trims_to_visible_window_and_builds_summary():
    start = datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 30, 12, 10, tzinfo=timezone.utc)
    db = _FakeSession(
        [
            SimpleNamespace(
                bucket=start - timedelta(minutes=5),
                open=95.0,
                high=96.0,
                low=94.0,
                close=95.5,
                volume=100.0,
                ticks=10,
            ),
            SimpleNamespace(
                bucket=start,
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=200.0,
                ticks=20,
            ),
            SimpleNamespace(
                bucket=start + timedelta(minutes=5),
                open=100.5,
                high=102.0,
                low=100.0,
                close=101.5,
                volume=250.0,
                ticks=30,
            ),
            SimpleNamespace(
                bucket=end,
                open=101.5,
                high=103.0,
                low=101.0,
                close=102.0,
                volume=300.0,
                ticks=40,
            ),
        ]
    )

    payload = await get_chart_data(
        "BTC-USD",
        timeframe="5m",
        start=start,
        end=end,
        limit=3,
        db=db,
    )

    assert [candle["time"] for candle in payload["candles"]] == [
        start,
        start + timedelta(minutes=5),
        end,
    ]
    assert payload["summary"]["open"] == 100.0
    assert payload["summary"]["close"] == 102.0
    assert payload["summary"]["ticks"] == 90
    assert payload["coverage"]["visible_candles"] == 3


def test_build_chart_payload_includes_contextual_insights():
    base = datetime(2026, 3, 30, 9, 30, tzinfo=timezone.utc)
    candles = []
    for index in range(180):
        open_price = 100.0 + (index * 0.28)
        close_price = open_price + 0.18 + ((index % 4) * 0.02)
        candles.append(
            {
                "time": base + timedelta(minutes=index),
                "open": round(open_price, 4),
                "high": round(close_price + 0.24, 4),
                "low": round(open_price - 0.19, 4),
                "close": round(close_price, 4),
                "volume": 1000.0 + (index * 15.0),
                "ticks": 12 + (index % 5),
            }
        )

    start = candles[-60]["time"]
    payload = _build_chart_payload(
        symbol="BTC-USD",
        timeframe="5m",
        candles=candles,
        start=start,
        limit=60,
        warmup_candles=120,
    )

    insights = payload["insights"]
    assert payload["coverage"]["visible_candles"] == 60
    assert insights["asset_class"] == "crypto"
    assert insights["headline"]
    assert insights["timeframe_context"]["label"] == "Intraday precision"
    assert {card["key"] for card in insights["cards"]} == {
        "trend",
        "momentum",
        "volatility",
        "participation",
        "stretch",
    }
    assert "rsi" in insights["indicator_guides"]
    assert "macd" in insights["indicator_guides"]
    assert "atr" in insights["indicator_guides"]
    assert "volume" in insights["indicator_guides"]
    assert insights["indicator_guides"]["rsi"]["good_range"]
    assert insights["indicator_guides"]["rsi"]["why_range"]
    assert insights["indicator_guides"]["rsi"]["timeframe_note"]
    assert insights["ai_overview"]["stance"]
    assert insights["ai_overview"]["action"]
    assert insights["ai_overview"]["risk_note"]
    assert insights["market_regime"]["label"]
    assert insights["market_regime"]["summary"]
    assert insights["study_profiles"]
    assert insights["active_study_profile"] == "responsive"
    assert {profile["key"] for profile in insights["study_profiles"]} == {"responsive", "balanced", "trend"}
    responsive = next(profile for profile in insights["study_profiles"] if profile["key"] == "responsive")
    assert responsive["studies"]["fast_ema"]["key"] == "ema_8"
    assert responsive["studies"]["macd"]["key"] == "macd_8_21_5"
    assert responsive["default_overlays"]["fast_ema"] is True
    assert responsive["fit_score_pct"] >= 50
    assert responsive["fit_summary"]
    assert responsive["entry_guidance"]
    assert responsive["timing_note"]
    assert responsive["current_signal_label"]
    assert responsive["current_signal_summary"]
    assert responsive["backtest"]["summary"]
    assert responsive["recommended"] is True


def test_build_study_profile_snapshot_selects_requested_profile():
    base = datetime(2026, 3, 30, 9, 30, tzinfo=timezone.utc)
    candles = []
    for index in range(180):
        open_price = 100.0 + (index * 0.22)
        close_price = open_price + 0.25 + ((index % 3) * 0.03)
        candles.append(
            {
                "time": base + timedelta(minutes=index),
                "open": round(open_price, 4),
                "high": round(close_price + 0.2, 4),
                "low": round(open_price - 0.18, 4),
                "close": round(close_price, 4),
                "volume": 1000.0 + (index * 14.0),
                "ticks": 10 + (index % 4),
            }
        )

    payload = _build_chart_payload(
        symbol="BTC-USD",
        timeframe="15m",
        candles=candles,
        start=candles[-60]["time"],
        limit=60,
        warmup_candles=120,
    )

    snapshot = build_study_profile_snapshot(payload, profile_key="balanced")

    assert snapshot["symbol"] == "BTC-USD"
    assert snapshot["timeframe"] == "15m"
    assert snapshot["profile_key"] == "balanced"
    assert snapshot["profile_title"] == "Balanced Structure"
    assert snapshot["signal_label"]
    assert snapshot["signal_summary"]


def test_study_profiles_shift_with_timeframe_and_density():
    profiles, active_key = charts_routes._build_study_profiles(
        asset_class="stock",
        timeframe="1w",
        visible_instances=320,
    )

    assert active_key == "trend"
    trend = next(profile for profile in profiles if profile["key"] == "trend")
    assert trend["studies"]["fast_ema"]["key"] == "ema_21"
    assert trend["studies"]["anchor_sma"]["key"] == "sma_200"
    assert trend["studies"]["macd"]["key"] == "macd_21_55_9"


def test_profile_sample_backtest_reports_edge_and_hit_rate():
    base = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
    candles = []
    for index in range(140):
        open_price = 100.0 + index * 0.5
        close_offset = 0.35 if index % 6 in {0, 1, 2, 3} else -0.15
        close_price = open_price + close_offset
        candles.append(
            {
                "time": base + timedelta(hours=index),
                "open": round(open_price, 4),
                "high": round(max(open_price, close_price) + 0.35, 4),
                "low": round(min(open_price, close_price) - 0.2, 4),
                "close": round(close_price, 4),
                "volume": 2000.0 + index * 10.0,
                "ticks": 5,
            }
        )

    indicators = charts_routes._compute_indicator_series(candles)
    profiles, _ = charts_routes._build_study_profiles("crypto", "1h", 120)
    balanced = next(profile for profile in profiles if profile["key"] == "balanced")
    result = charts_routes._profile_sample_backtest(
        profile=balanced,
        candles=candles,
        visible_candles=candles[-120:],
        indicators=indicators,
    )

    assert result["sample_bars"] > 50
    assert result["trades"] >= 1
    assert result["return_pct"] is not None
    assert result["hit_rate_pct"] is not None


@pytest.mark.asyncio
async def test_chart_matrix_reuses_compact_insight_rows(monkeypatch):
    async def fake_resolve_chart_payload(symbol, timeframe, start, end, limit, db):
        return {
            "symbol": symbol.upper(),
            "timeframe": timeframe,
            "summary": {"close": 123.45, "change_pct": 1.75},
            "insights": {
                "headline": f"{symbol} {timeframe} headline",
                "timeframe_context": {"label": f"{timeframe} context"},
                "trend": {"label": "Bullish", "tone": "positive", "score_pct": 72.0},
                "momentum": {"label": "Building", "tone": "positive", "rsi": 61.2},
                "participation": {"label": "Healthy", "tone": "positive", "relative_volume": 1.12},
                "stretch": {"label": "Near Fair Value", "tone": "info", "value": "+0.15% vs VWAP"},
            },
        }

    monkeypatch.setattr(charts_routes, "_resolve_chart_payload", fake_resolve_chart_payload)

    payload = await get_chart_matrix("BTC-USD", timeframes="1m,5m,1h", limit=90, db=object())

    assert payload["symbol"] == "BTC-USD"
    assert payload["asset_class"] == "crypto"
    assert [row["timeframe"] for row in payload["matrix"]] == ["1m", "5m", "1h"]
    assert payload["matrix"][0]["trend"]["label"] == "Bullish"
    assert payload["matrix"][1]["momentum"]["rsi"] == pytest.approx(61.2, abs=1e-6)


@pytest.mark.asyncio
async def test_chart_matrix_marks_missing_timeframe_without_failing(monkeypatch):
    async def fake_resolve_chart_payload(symbol, timeframe, start, end, limit, db):
        if timeframe == "1h":
            raise HTTPException(status_code=404, detail=f"No chart data for {symbol} in requested range")
        return {
            "symbol": symbol.upper(),
            "timeframe": timeframe,
            "summary": {"close": 110.0, "change_pct": -0.5},
            "insights": {
                "headline": f"{symbol} {timeframe} headline",
                "timeframe_context": {"label": f"{timeframe} context"},
                "trend": {"label": "Neutral", "tone": "neutral", "score_pct": 50.0},
                "momentum": {"label": "Balanced", "tone": "neutral", "rsi": 49.8},
                "participation": {"label": "Healthy", "tone": "positive", "relative_volume": 1.0},
                "stretch": {"label": "Near Fair Value", "tone": "info", "value": "+0.05% vs VWAP"},
            },
        }

    monkeypatch.setattr(charts_routes, "_resolve_chart_payload", fake_resolve_chart_payload)

    payload = await get_chart_matrix("BTC-USD", timeframes="1m,1h", db=object())

    assert payload["matrix"][0]["available"] is True
    assert payload["matrix"][1]["available"] is False
    assert payload["matrix"][1]["timeframe"] == "1h"
