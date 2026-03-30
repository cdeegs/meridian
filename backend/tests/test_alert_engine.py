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


@pytest.mark.asyncio
async def test_price_alert_triggers_on_batch():
    engine = AlertEngine(MagicMock())
    engine._mark_triggered = AsyncMock()
    engine._active_by_symbol["AAPL"].append(_alert())

    batch = [
        PriceEvent(
            symbol="AAPL",
            price=201.25,
            source="alpaca",
            timestamp=datetime.now(timezone.utc),
        )
    ]

    alerts = await engine.process_batch(batch, [])
    assert len(alerts) == 1
    assert alerts[0]["condition"] == "price_above"
    assert "above 200.00" in alerts[0]["message"]


@pytest.mark.asyncio
async def test_rsi_alert_triggers_from_indicator_results():
    engine = AlertEngine(MagicMock())
    engine._mark_triggered = AsyncMock()
    engine._active_by_symbol["AAPL"].append(
        _alert(id="alert-rsi", condition="rsi_above", threshold=70.0)
    )

    alerts = await engine.process_batch(
        [],
        [
            {
                "time": datetime.now(timezone.utc),
                "symbol": "AAPL",
                "timeframe": "1m",
                "indicator": "rsi_14",
                "value": {"v": 72.4},
            }
        ],
    )

    assert len(alerts) == 1
    assert alerts[0]["observed_value"] == pytest.approx(72.4, abs=1e-6)


@pytest.mark.asyncio
async def test_macd_cross_up_requires_previous_state():
    engine = AlertEngine(MagicMock())
    engine._mark_triggered = AsyncMock()
    engine._active_by_symbol["AAPL"].append(
        _alert(id="alert-macd", condition="macd_cross_up", threshold=None)
    )
    engine._last_indicator_values["AAPL"]["macd"] = {
        "macd": -0.25,
        "signal": -0.10,
        "histogram": -0.15,
    }

    alerts = await engine.process_batch(
        [],
        [
            {
                "time": datetime.now(timezone.utc),
                "symbol": "AAPL",
                "timeframe": "1m",
                "indicator": "macd",
                "value": {"macd": 0.18, "signal": 0.10, "histogram": 0.08},
            }
        ],
    )

    assert len(alerts) == 1
    assert alerts[0]["condition"] == "macd_cross_up"
