import logging
import math
from datetime import datetime, timedelta
from typing import Optional

import httpx

from backend.services.candle_history import aggregate_candles

logger = logging.getLogger(__name__)

_REQUEST_TIMEFRAME_MAP = {
    "1m": "1Min",
    "5m": "5Min",
    "15m": "15Min",
    "30m": "15Min",
    "1h": "1Hour",
    "2h": "1Hour",
    "4h": "1Hour",
    "6h": "1Hour",
    "12h": "1Hour",
    "1d": "1Day",
    "2d": "1Day",
    "1w": "1Day",
}
_TIMEFRAME_WIDTHS = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "12h": 720,
    "1d": 1440,
    "2d": 2880,
    "1w": 10080,
}
_REQUEST_WIDTHS = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 15,
    "1h": 60,
    "2h": 60,
    "4h": 60,
    "6h": 60,
    "12h": 60,
    "1d": 1440,
    "2d": 1440,
    "1w": 1440,
}


class AlpacaMarketDataClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        feed: str = "iex",
        stock_symbols: Optional[list[str]] = None,
    ):
        self._api_key = api_key
        self._api_secret = api_secret
        self._feed = feed
        self._base_url = "https://data.alpaca.markets/v2"
        self._stock_symbols = [symbol.upper() for symbol in (stock_symbols or [])]

    @property
    def stock_symbols(self) -> list[str]:
        return list(self._stock_symbols)

    async def fetch_watchlist_snapshots(self) -> dict[str, dict]:
        return await self.fetch_snapshots(self._stock_symbols)

    async def fetch_snapshots(self, symbols: list[str]) -> dict[str, dict]:
        symbols = [symbol.upper() for symbol in symbols if symbol]
        if not symbols:
            return {}

        payload = await self._get(
            "/stocks/snapshots",
            params={
                "symbols": ",".join(symbols),
                "feed": self._feed,
            },
        )
        snapshots = payload.get("snapshots")
        if snapshots is None and isinstance(payload, dict):
            snapshots = payload

        normalized: dict[str, dict] = {}
        for symbol, snapshot in (snapshots or {}).items():
            if isinstance(snapshot, dict):
                normalized[symbol.upper()] = self._normalize_snapshot(symbol, snapshot)
        return normalized

    async def fetch_snapshot(self, symbol: str) -> Optional[dict]:
        payload = await self._get(
            f"/stocks/{symbol.upper()}/snapshot",
            params={"feed": self._feed},
        )
        if not isinstance(payload, dict) or not payload:
            return None
        return self._normalize_snapshot(symbol, payload)

    async def fetch_bars(
        self,
        symbol: str,
        timeframe: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 200,
    ) -> list[dict]:
        alpaca_timeframe = _REQUEST_TIMEFRAME_MAP[timeframe]
        request_limit = max(1, math.ceil(limit * (_TIMEFRAME_WIDTHS[timeframe] / _REQUEST_WIDTHS[timeframe])))
        params = {
            "timeframe": alpaca_timeframe,
            "limit": request_limit,
            "adjustment": "raw",
            "feed": self._feed,
        }
        if start is not None:
            params["start"] = start.isoformat().replace("+00:00", "Z")
        if end is not None:
            params["end"] = end.isoformat().replace("+00:00", "Z")

        payload = await self._get(f"/stocks/{symbol.upper()}/bars", params=params)
        bars = payload.get("bars", []) if isinstance(payload, dict) else []
        normalized: list[dict] = []

        for bar in bars:
            if not isinstance(bar, dict):
                continue
            timestamp = _parse_timestamp(bar.get("t"))
            if timestamp is None:
                continue
            normalized.append(
                {
                    "time": timestamp,
                    "open": float(bar["o"]),
                    "high": float(bar["h"]),
                    "low": float(bar["l"]),
                    "close": float(bar["c"]),
                    "volume": float(bar.get("v", 0.0) or 0.0),
                    "ticks": int(bar.get("n", 0) or 0),
                    "vwap": float(bar["vw"]) if bar.get("vw") is not None else None,
                }
            )

        normalized.sort(key=lambda bar: bar["time"])
        if _TIMEFRAME_WIDTHS[timeframe] != _REQUEST_WIDTHS[timeframe]:
            normalized = aggregate_candles(normalized, timedelta(minutes=_TIMEFRAME_WIDTHS[timeframe]))
        return normalized

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        headers = {
            "APCA-API-KEY-ID": self._api_key,
            "APCA-API-SECRET-KEY": self._api_secret,
        }
        async with httpx.AsyncClient(base_url=self._base_url, timeout=10.0) as client:
            response = await client.get(path, params=params, headers=headers)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.warning("Alpaca market data request failed for %s: %s", path, exc)
                raise
            return response.json()

    def _normalize_snapshot(self, symbol: str, snapshot: dict) -> dict:
        latest_trade = snapshot.get("latestTrade") or snapshot.get("latest_trade") or {}
        latest_quote = snapshot.get("latestQuote") or snapshot.get("latest_quote") or {}
        minute_bar = snapshot.get("minuteBar") or snapshot.get("minute_bar") or {}
        daily_bar = snapshot.get("dailyBar") or snapshot.get("daily_bar") or {}
        previous_daily_bar = (
            snapshot.get("prevDailyBar")
            or snapshot.get("previousDailyBar")
            or snapshot.get("prev_daily_bar")
            or {}
        )

        bid = latest_quote.get("bp")
        ask = latest_quote.get("ap")
        spread = round(float(ask) - float(bid), 4) if bid is not None and ask is not None else None

        return {
            "symbol": symbol.upper(),
            "price": _coalesce_number(
                latest_trade.get("p"),
                minute_bar.get("c"),
                daily_bar.get("c"),
            ),
            "timestamp": _parse_timestamp(
                latest_trade.get("t")
                or minute_bar.get("t")
                or daily_bar.get("t")
            ),
            "volume": _coalesce_number(
                latest_trade.get("s"),
                minute_bar.get("v"),
                daily_bar.get("v"),
            ),
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            "spread": spread,
            "minute_bar": self._normalize_bar(minute_bar),
            "daily_bar": self._normalize_bar(daily_bar),
            "previous_daily_bar": self._normalize_bar(previous_daily_bar),
            "source": "alpaca",
        }

    @staticmethod
    def _normalize_bar(bar: dict) -> Optional[dict]:
        if not isinstance(bar, dict) or not bar:
            return None
        timestamp = _parse_timestamp(bar.get("t"))
        if timestamp is None:
            return None
        return {
            "time": timestamp,
            "open": _coalesce_number(bar.get("o")),
            "high": _coalesce_number(bar.get("h")),
            "low": _coalesce_number(bar.get("l")),
            "close": _coalesce_number(bar.get("c")),
            "volume": _coalesce_number(bar.get("v")),
            "ticks": int(bar.get("n", 0) or 0),
            "vwap": _coalesce_number(bar.get("vw")),
        }


def _coalesce_number(*values) -> Optional[float]:
    for value in values:
        if value is None:
            continue
        return float(value)
    return None


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
