from datetime import datetime, timedelta, timezone

import pytest

from backend.routes.candles import get_candles, set_coinbase_market_data_client as set_candles_coinbase
from backend.routes.charts import get_chart_data, set_coinbase_market_data_client as set_charts_coinbase
from backend.services.coinbase_market_data import CoinbaseMarketDataClient


class _EmptyResult:
    def fetchall(self):
        return []


class _RowsResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows):
        self._rows = rows

    async def execute(self, statement, params=None):
        return _RowsResult(self._rows)


class _FakeCoinbaseRouteClient:
    async def fetch_bars(self, symbol: str, timeframe: str, start=None, end=None, limit=200):
        assert symbol == "BTC-USD"
        base = datetime(2026, 3, 30, 14, 0, tzinfo=timezone.utc)
        bars = []
        for index in range(200):
            bars.append(
                {
                    "time": base + timedelta(minutes=index),
                    "open": 100.0 + index,
                    "high": 100.5 + index,
                    "low": 99.5 + index,
                    "close": 100.25 + index,
                    "volume": 2000.0 + index,
                    "ticks": 0,
                    "source": "coinbase",
                }
            )
        if start is not None:
            bars = [bar for bar in bars if bar["time"] >= start]
        if end is not None:
            bars = [bar for bar in bars if bar["time"] <= end]
        return bars[-limit:]


@pytest.mark.asyncio
async def test_candles_merge_coinbase_history_with_local_crypto_rows():
    base = datetime(2026, 3, 30, 14, 0, tzinfo=timezone.utc)
    local_rows = [
        type(
            "Row",
            (),
            {
                "bucket": base + timedelta(minutes=2),
                "open": 999.0,
                "high": 1000.0,
                "low": 998.5,
                "close": 999.5,
                "volume": 90.0,
                "ticks": 4,
            },
        )(),
        type(
            "Row",
            (),
            {
                "bucket": base + timedelta(minutes=3),
                "open": 1000.0,
                "high": 1001.0,
                "low": 999.5,
                "close": 1000.5,
                "volume": 100.0,
                "ticks": 5,
            },
        )(),
    ]

    client = _FakeCoinbaseRouteClient()
    set_candles_coinbase(client)
    try:
        payload = await get_candles(
            "BTC-USD",
            timeframe="1m",
            start=base,
            end=base + timedelta(minutes=3),
            limit=4,
            db=_FakeSession(local_rows),
        )
    finally:
        set_candles_coinbase(None)

    assert [candle["time"] for candle in payload["candles"]] == [
        base,
        base + timedelta(minutes=1),
        base + timedelta(minutes=2),
        base + timedelta(minutes=3),
    ]
    assert payload["candles"][2]["close"] == pytest.approx(999.5, abs=1e-6)
    assert payload["candles"][0]["close"] == pytest.approx(100.25, abs=1e-6)


@pytest.mark.asyncio
async def test_chart_data_falls_back_to_coinbase_history():
    client = _FakeCoinbaseRouteClient()
    set_charts_coinbase(client)
    start = datetime(2026, 3, 30, 16, 50, tzinfo=timezone.utc)
    end = datetime(2026, 3, 30, 17, 19, tzinfo=timezone.utc)
    try:
        payload = await get_chart_data(
            "BTC-USD",
            timeframe="1m",
            start=start,
            end=end,
            limit=30,
            db=_FakeSession([]),
        )
    finally:
        set_charts_coinbase(None)

    assert payload["symbol"] == "BTC-USD"
    assert payload["coverage"]["visible_candles"] == 30
    assert payload["summary"]["close"] == pytest.approx(299.25, abs=1e-6)
    assert len(payload["indicators"]["rsi_14"]) > 0


class _PagingCoinbaseClient(CoinbaseMarketDataClient):
    def __init__(self):
        super().__init__(base_url="https://example.com")
        self.requests = []

    async def _get(self, path: str, params=None) -> dict:
        self.requests.append(params)
        start = datetime.fromtimestamp(int(params["start"]), tz=timezone.utc)
        end = datetime.fromtimestamp(int(params["end"]), tz=timezone.utc)
        step = {
            "ONE_MINUTE": timedelta(minutes=1),
            "FIVE_MINUTE": timedelta(minutes=5),
            "FIFTEEN_MINUTE": timedelta(minutes=15),
            "ONE_HOUR": timedelta(hours=1),
            "ONE_DAY": timedelta(days=1),
        }[params["granularity"]]
        candles = []
        current = end
        count = 0
        while current >= start and count < int(params["limit"]):
            minute_index = int((current - datetime(2026, 3, 30, tzinfo=timezone.utc)).total_seconds() // 60)
            candles.append(
                {
                    "start": str(int(current.timestamp())),
                    "open": str(1000.0 + minute_index),
                    "high": str(1000.5 + minute_index),
                    "low": str(999.5 + minute_index),
                    "close": str(1000.25 + minute_index),
                    "volume": str(10.0 + count),
                }
            )
            current -= step
            count += 1
        return {"candles": candles}


@pytest.mark.asyncio
async def test_coinbase_market_data_paginates_beyond_provider_limit():
    client = _PagingCoinbaseClient()
    end = datetime(2026, 3, 30, 20, 0, tzinfo=timezone.utc)
    start = end - timedelta(minutes=499)

    bars = await client.fetch_bars(
        "BTC-USD",
        timeframe="1m",
        start=start,
        end=end,
        limit=500,
    )

    assert len(client.requests) >= 2
    assert len(bars) == 500
    assert bars[0]["time"] == start
    assert bars[-1]["time"] == end


@pytest.mark.asyncio
async def test_coinbase_market_data_aggregates_derived_timeframes():
    client = _PagingCoinbaseClient()
    end = datetime(2026, 3, 30, 20, 0, tzinfo=timezone.utc)
    start = end - timedelta(hours=11)

    bars = await client.fetch_bars(
        "BTC-USD",
        timeframe="2h",
        start=start,
        end=end,
        limit=6,
    )

    assert len(client.requests) >= 1
    assert client.requests[0]["granularity"] == "ONE_HOUR"
    assert len(bars) == 6
    assert bars[0]["time"] == datetime(2026, 3, 30, 10, 0, tzinfo=timezone.utc)
    assert bars[-1]["time"] == datetime(2026, 3, 30, 20, 0, tzinfo=timezone.utc)
