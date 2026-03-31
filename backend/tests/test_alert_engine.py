from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.models.price_event import PriceEvent
from backend.services.alert_engine import AlertEngine, AlertRule


def _alert(**overrides) -> AlertRule:
    base = {
        "id": "alert-1",
        "symbol": "AAPL",
        "condition": "price_above",
        "threshold": 200.0,
        "status": "active",
        "created_at": datetime.now(timezone.utc),
        "triggered_at": None,
    }
    base.update(overrides)
    return AlertRule(**base)


def _price_event(price: float, symbol: str = "AAPL") -> PriceEvent:
    return PriceEvent(
        symbol=symbol,
        price=price,
        source="alpaca",
        timestamp=datetime.now(timezone.utc),
    )


def _indicator_result(indicator: str, value: dict, symbol: str = "AAPL") -> dict:
    return {
        "time": datetime.now(timezone.utc),
        "symbol": symbol,
        "timeframe": "1m",
        "indicator": indicator,
        "value": value,
    }


@pytest.fixture
def notifier():
    mock = MagicMock()
    mock.notify_alert = AsyncMock()
    return mock


@pytest.fixture
def engine(notifier):
    alert_engine = AlertEngine(AsyncMock(), notifier=notifier)
    alert_engine._mark_triggered = AsyncMock()
    return alert_engine


@pytest.mark.asyncio
async def test_price_above_triggers_once_and_does_not_retrigger(engine, notifier):
    engine._active_by_symbol["AAPL"].append(_alert(condition="price_above", threshold=200.0))

    first = await engine.process_batch([_price_event(201.25)], [])
    second = await engine.process_batch([_price_event(205.0)], [])

    assert len(first) == 1
    assert first[0]["condition"] == "price_above"
    assert "above 200.00" in first[0]["message"]
    assert second == []
    engine._mark_triggered.assert_awaited_once()
    notifier.notify_alert.assert_awaited_once()


@pytest.mark.asyncio
async def test_price_below_triggers_once_and_does_not_retrigger(engine):
    engine._active_by_symbol["AAPL"].append(_alert(condition="price_below", threshold=200.0))

    first = await engine.process_batch([_price_event(199.5)], [])
    second = await engine.process_batch([_price_event(190.0)], [])

    assert len(first) == 1
    assert first[0]["condition"] == "price_below"
    assert "below 200.00" in first[0]["message"]
    assert second == []


@pytest.mark.asyncio
async def test_rsi_above_triggers_when_threshold_is_exceeded(engine):
    engine._active_by_symbol["AAPL"].append(_alert(condition="rsi_above", threshold=70.0))

    alerts = await engine.process_batch([], [_indicator_result("rsi_14", {"v": 72.4})])

    assert len(alerts) == 1
    assert alerts[0]["condition"] == "rsi_above"
    assert alerts[0]["observed_value"] == pytest.approx(72.4, abs=1e-6)


@pytest.mark.asyncio
async def test_rsi_below_triggers_when_value_drops_under_threshold(engine):
    engine._active_by_symbol["AAPL"].append(_alert(condition="rsi_below", threshold=30.0))

    alerts = await engine.process_batch([], [_indicator_result("rsi_14", {"v": 28.7})])

    assert len(alerts) == 1
    assert alerts[0]["condition"] == "rsi_below"
    assert alerts[0]["observed_value"] == pytest.approx(28.7, abs=1e-6)


@pytest.mark.asyncio
async def test_macd_cross_up_triggers_on_positive_transition(engine):
    engine._active_by_symbol["AAPL"].append(_alert(condition="macd_cross_up", threshold=None))
    engine._last_indicator_values["AAPL"]["macd"] = {
        "macd": -0.25,
        "signal": -0.10,
        "histogram": -0.15,
    }

    alerts = await engine.process_batch([], [_indicator_result("macd", {"macd": 0.18, "signal": 0.10, "histogram": 0.08})])

    assert len(alerts) == 1
    assert alerts[0]["condition"] == "macd_cross_up"
    assert alerts[0]["message"] == "AAPL MACD crossed above signal"


@pytest.mark.asyncio
async def test_macd_cross_down_triggers_on_negative_transition(engine):
    engine._active_by_symbol["AAPL"].append(_alert(condition="macd_cross_down", threshold=None))
    engine._last_indicator_values["AAPL"]["macd"] = {
        "macd": 0.22,
        "signal": 0.10,
        "histogram": 0.12,
    }

    alerts = await engine.process_batch([], [_indicator_result("macd", {"macd": -0.18, "signal": -0.05, "histogram": -0.13})])

    assert len(alerts) == 1
    assert alerts[0]["condition"] == "macd_cross_down"
    assert alerts[0]["message"] == "AAPL MACD crossed below signal"


@pytest.mark.asyncio
async def test_inactive_alert_is_never_triggered(engine):
    engine._active_by_symbol["AAPL"].append(_alert(status="disabled", condition="price_above", threshold=200.0))

    alerts = await engine.process_batch([_price_event(250.0)], [])

    assert alerts == []
    engine._mark_triggered.assert_not_awaited()
