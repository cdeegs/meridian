"""
Alpaca Markets WebSocket adapter.

Free tier: sign up at alpaca.markets (no credit card).
Set ALPACA_FEED=iex for the free IEX data feed.

Docs: https://docs.alpaca.markets/reference/stockdatastream
"""
import json
import logging
import ssl
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Optional

import certifi
import websockets
from websockets.exceptions import ConnectionClosed

from backend.adapters.base import BaseAdapter
from backend.models.price_event import PriceEvent

logger = logging.getLogger(__name__)

_WS_URL = "wss://stream.data.alpaca.markets/v2/{feed}"
_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


class AlpacaAdapter(BaseAdapter):
    source = "alpaca"

    def __init__(self, api_key: str, api_secret: str, feed: str = "iex"):
        self._api_key = api_key
        self._api_secret = api_secret
        self._feed = feed
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        # Latest quote per symbol — merged into the next trade tick
        self._quotes: Dict[str, Dict] = {}

    async def connect(self) -> None:
        url = _WS_URL.format(feed=self._feed)
        self._ws = await websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=10,
            ssl=_SSL_CONTEXT,
        )

        # Receive connection welcome
        raw = await self._ws.recv()
        msgs = json.loads(raw)
        logger.debug("Alpaca connect: %s", msgs)

        # Authenticate
        await self._ws.send(json.dumps({
            "action": "auth",
            "key": self._api_key,
            "secret": self._api_secret,
        }))
        raw = await self._ws.recv()
        msgs = json.loads(raw)
        for msg in (msgs if isinstance(msgs, list) else [msgs]):
            if msg.get("T") == "error":
                raise ConnectionError(f"Alpaca auth failed: {msg.get('msg')}")
        logger.info("Alpaca authenticated (feed=%s)", self._feed)

    async def subscribe(self, symbols: List[str]) -> None:
        await self._ws.send(json.dumps({
            "action": "subscribe",
            "trades": symbols,
            "quotes": symbols,
        }))
        raw = await self._ws.recv()
        logger.info("Alpaca subscribed to %s: %s", symbols, json.loads(raw))

    async def disconnect(self) -> None:
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def stream(self) -> AsyncIterator[PriceEvent]:
        async for raw in self._ws:
            msgs = json.loads(raw)
            if not isinstance(msgs, list):
                msgs = [msgs]

            for msg in msgs:
                event = self._parse(msg)
                if event is not None:
                    yield event

    def _parse(self, msg: dict) -> Optional[PriceEvent]:
        msg_type = msg.get("T")

        if msg_type == "q":  # Quote update — cache bid/ask, don't emit yet
            symbol = msg.get("S", "")
            self._quotes[symbol] = {
                "bid": msg.get("bp"),
                "ask": msg.get("ap"),
            }
            return None

        if msg_type == "t":  # Trade — emit with latest bid/ask
            symbol = msg.get("S", "")
            price = msg.get("p")
            volume = msg.get("s")

            ts_str = msg.get("t", "")
            try:
                timestamp = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                timestamp = datetime.now(timezone.utc)

            quote = self._quotes.get(symbol, {})
            bid = quote.get("bid")
            ask = quote.get("ask")
            spread = round(ask - bid, 4) if bid is not None and ask is not None else None

            return PriceEvent(
                symbol=symbol,
                price=price,
                volume=volume,
                bid=bid,
                ask=ask,
                spread=spread,
                source=self.source,
                timestamp=timestamp,
            )

        return None  # control messages, subscription confirmations, etc.
