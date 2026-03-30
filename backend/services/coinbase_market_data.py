from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

import certifi
import httpx

from backend.services.candle_history import aggregate_candles

logger = logging.getLogger(__name__)

_MAX_LIMIT = 350
_REQUEST_GRANULARITY_MAP = {
    "1m": "ONE_MINUTE",
    "5m": "FIVE_MINUTE",
    "15m": "FIFTEEN_MINUTE",
    "30m": "FIFTEEN_MINUTE",
    "1h": "ONE_HOUR",
    "2h": "ONE_HOUR",
    "4h": "ONE_HOUR",
    "6h": "ONE_HOUR",
    "12h": "ONE_HOUR",
    "1d": "ONE_DAY",
    "2d": "ONE_DAY",
    "1w": "ONE_DAY",
}
_TIMEFRAME_WIDTHS = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "2h": timedelta(hours=2),
    "4h": timedelta(hours=4),
    "6h": timedelta(hours=6),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
    "2d": timedelta(days=2),
    "1w": timedelta(days=7),
}
_REQUEST_WIDTHS = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "2h": timedelta(hours=1),
    "4h": timedelta(hours=1),
    "6h": timedelta(hours=1),
    "12h": timedelta(hours=1),
    "1d": timedelta(days=1),
    "2d": timedelta(days=1),
    "1w": timedelta(days=1),
}


class CoinbaseMarketDataClient:
    def __init__(self, base_url: str = "https://api.coinbase.com/api/v3/brokerage/market"):
        self._base_url = base_url.rstrip("/")

    async def fetch_bars(
        self,
        symbol: str,
        timeframe: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 200,
    ) -> list[dict]:
        granularity = _REQUEST_GRANULARITY_MAP[timeframe]
        bucket_width = _REQUEST_WIDTHS[timeframe]
        target_bucket_width = _TIMEFRAME_WIDTHS[timeframe]
        target_limit = max(limit, 1)
        request_limit = max(1, math.ceil(target_limit * (target_bucket_width / bucket_width)))

        if end is None:
            end = datetime.now(timezone.utc)
        if start is None:
            start = end - (target_bucket_width * target_limit)
        if start > end:
            return []

        bars_by_time: dict[datetime, dict] = {}
        remaining_end = end

        while remaining_end >= start and len(bars_by_time) < request_limit:
            remaining = request_limit - len(bars_by_time)
            chunk_limit = min(_MAX_LIMIT, remaining)
            chunk_start = max(start, remaining_end - (bucket_width * chunk_limit))

            payload = await self._get(
                f"/products/{symbol.upper()}/candles",
                params={
                    "start": str(int(chunk_start.timestamp())),
                    "end": str(int(remaining_end.timestamp())),
                    "granularity": granularity,
                    "limit": chunk_limit,
                },
            )
            bars = self._normalize_candles(payload.get("candles", []))
            if not bars:
                if chunk_start <= start:
                    break
                remaining_end = chunk_start - bucket_width
                continue

            for bar in bars:
                if start <= bar["time"] <= end:
                    bars_by_time[bar["time"]] = bar

            earliest = bars[0]["time"]
            next_end = earliest - bucket_width
            if earliest <= start or next_end >= remaining_end:
                break
            remaining_end = next_end

        normalized = sorted(bars_by_time.values(), key=lambda bar: bar["time"])
        if target_bucket_width != bucket_width:
            normalized = aggregate_candles(normalized, target_bucket_width)
        return normalized[-target_limit:]

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=10.0,
            verify=certifi.where(),
            headers={"cache-control": "no-cache"},
        ) as client:
            response = await client.get(path, params=params)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.warning("Coinbase market data request failed for %s: %s", path, exc)
                raise
            return response.json()

    @staticmethod
    def _normalize_candles(candles: list[dict]) -> list[dict]:
        normalized = []
        for candle in candles:
            if not isinstance(candle, dict):
                continue
            timestamp = _parse_timestamp(candle.get("start"))
            if timestamp is None:
                continue
            normalized.append(
                {
                    "time": timestamp,
                    "open": float(candle["open"]),
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": float(candle["close"]),
                    "volume": float(candle.get("volume", 0.0) or 0.0),
                    "ticks": 0,
                    "source": "coinbase",
                }
            )

        normalized.sort(key=lambda bar: bar["time"])
        return normalized


def _parse_timestamp(value) -> Optional[datetime]:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None
