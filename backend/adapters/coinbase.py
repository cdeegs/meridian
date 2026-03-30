"""
Coinbase Advanced Trade WebSocket adapter.

Uses Coinbase's public market-data endpoint, so Meridian can run live without
exchange credentials. We subscribe to:
- ticker         → best bid / ask cache
- market_trades  → trade price + size events
- heartbeats     → keeps sparse subscriptions alive
"""
import json
import logging
import ssl
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Optional

import certifi
import websockets

from backend.adapters.base import BaseAdapter
from backend.models.price_event import PriceEvent

logger = logging.getLogger(__name__)

_WS_URL = "wss://advanced-trade-ws.coinbase.com"
_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


class CoinbaseAdapter(BaseAdapter):
    source = "coinbase"

    def __init__(self):
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        # Latest ticker-derived quote per product_id.
        self._quotes: Dict[str, Dict[str, float]] = {}

    async def connect(self) -> None:
        self._ws = await websockets.connect(
            _WS_URL,
            ping_interval=20,
            ping_timeout=10,
            ssl=_SSL_CONTEXT,
        )
        logger.info("Coinbase WebSocket connected")

    async def subscribe(self, symbols: List[str]) -> None:
        if self._ws is None:
            raise ConnectionError("Coinbase WebSocket is not connected")

        subscriptions = [
            {"type": "subscribe", "channel": "ticker", "product_ids": symbols},
            {"type": "subscribe", "channel": "market_trades", "product_ids": symbols},
            {"type": "subscribe", "channel": "heartbeats"},
        ]
        for message in subscriptions:
            await self._ws.send(json.dumps(message))
        logger.info("Coinbase subscribed to %s", symbols)

    async def disconnect(self) -> None:
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def stream(self) -> AsyncIterator[PriceEvent]:
        if self._ws is None:
            raise ConnectionError("Coinbase WebSocket is not connected")

        async for raw in self._ws:
            payload = json.loads(raw)
            for event in self._parse(payload):
                yield event

    def _parse(self, msg: dict) -> List[PriceEvent]:
        channel = msg.get("channel")
        if channel == "ticker":
            self._cache_quotes(msg)
            return []
        if channel != "market_trades":
            return []

        events: List[PriceEvent] = []
        for event in msg.get("events", []):
            for trade in event.get("trades", []):
                symbol = trade.get("product_id", "")
                price = self._to_float(trade.get("price"))
                volume = self._to_float(trade.get("size"))
                if not symbol or price is None:
                    continue

                timestamp = self._parse_timestamp(trade.get("time"))
                quote = self._quotes.get(symbol, {})
                bid = quote.get("bid")
                ask = quote.get("ask")
                spread = round(ask - bid, 4) if bid is not None and ask is not None else None

                events.append(
                    PriceEvent(
                        symbol=symbol,
                        price=price,
                        volume=volume,
                        bid=bid,
                        ask=ask,
                        spread=spread,
                        source=self.source,
                        timestamp=timestamp,
                    )
                )
        return events

    def _cache_quotes(self, msg: dict) -> None:
        for event in msg.get("events", []):
            for ticker in event.get("tickers", []):
                symbol = ticker.get("product_id", "")
                if not symbol:
                    continue

                bid = self._to_float(ticker.get("best_bid"))
                ask = self._to_float(ticker.get("best_ask"))
                self._quotes[symbol] = {"bid": bid, "ask": ask}

    @staticmethod
    def _parse_timestamp(value: Optional[str]) -> datetime:
        if not value:
            return datetime.now(timezone.utc)
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return datetime.now(timezone.utc)

    @staticmethod
    def _to_float(value) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
