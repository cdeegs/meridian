from datetime import datetime, timedelta, timezone

import pytest

from backend.routes.candles import get_candles, set_alpaca_market_data_client as set_candles_alpaca
from backend.routes.charts import get_chart_data, set_alpaca_market_data_client as set_charts_alpaca
from backend.routes.prices import get_latest_price, set_alpaca_market_data_client as set_prices_alpaca


class _EmptyResult:
    def fetchall(self):
        return []

    def fetchone(self):
        return None


class _EmptySession:
    async def execute(self, statement, params=None):
        return _EmptyResult()


class _FakeAlpacaClient:
    def __init__(self):
        self.stock_symbols = ["AAPL", "MSFT"]

    async def fetch_snapshot(self, symbol: str):
        return {
            "symbol": symbol,
            "price": 201.25,
            "volume": 100.0,
            "bid": 201.2,
            "ask": 201.3,
            "spread": 0.1,
            "source": "alpaca",
            "timestamp": datetime(2026, 3, 30, 14, 30, tzinfo=timezone.utc),
        }

    async def fetch_bars(self, symbol: str, timeframe: str, start=None, end=None, limit=200):
        base = datetime(2026, 3, 30, 14, 30, tzinfo=timezone.utc)
        bars = []
        for index in range(80):
            bars.append(
                {
                    "time": base + timedelta(minutes=index),
                    "open": 100.0 + index,
                    "high": 100.5 + index,
                    "low": 99.5 + index,
                    "close": 100.25 + index,
                    "volume": 1000.0 + index,
                    "ticks": 10 + index,
                }
            )
        return bars[-limit:]


@pytest.mark.asyncio
async def test_latest_price_falls_back_to_alpaca_snapshot():
    client = _FakeAlpacaClient()
    set_prices_alpaca(client)
    try:
        payload = await get_latest_price("AAPL", db=_EmptySession())
    finally:
        set_prices_alpaca(None)

    assert payload["symbol"] == "AAPL"
    assert payload["price"] == pytest.approx(201.25, abs=1e-6)
    assert payload["source"] == "alpaca"


@pytest.mark.asyncio
async def test_candles_fall_back_to_alpaca_bars():
    client = _FakeAlpacaClient()
    set_candles_alpaca(client)
    try:
        payload = await get_candles(
            "AAPL",
            timeframe="1m",
            start=None,
            end=None,
            limit=20,
            db=_EmptySession(),
        )
    finally:
        set_candles_alpaca(None)

    assert payload["symbol"] == "AAPL"
    assert len(payload["candles"]) == 20
    assert payload["candles"][-1]["close"] == pytest.approx(179.25, abs=1e-6)


@pytest.mark.asyncio
async def test_chart_data_falls_back_to_alpaca_bars():
    client = _FakeAlpacaClient()
    set_charts_alpaca(client)
    start = datetime(2026, 3, 30, 15, 20, tzinfo=timezone.utc)
    end = datetime(2026, 3, 30, 15, 49, tzinfo=timezone.utc)
    try:
        payload = await get_chart_data(
            "AAPL",
            timeframe="1m",
            start=start,
            end=end,
            limit=30,
            db=_EmptySession(),
        )
    finally:
        set_charts_alpaca(None)

    assert payload["symbol"] == "AAPL"
    assert payload["coverage"]["visible_candles"] == 30
    assert payload["summary"]["close"] == pytest.approx(179.25, abs=1e-6)
    assert len(payload["indicators"]["rsi_14"]) > 0
