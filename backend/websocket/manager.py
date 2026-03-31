"""
WebSocket Connection Manager.

Tracks all connected clients and their symbol subscriptions.
Broadcasts combined price + indicator updates to relevant sockets.

Protocol (client → server):
    {"action": "subscribe", "symbols": ["AAPL", "SPY"]}
    {"action": "subscribe", "symbols": ["*"]}   ← wildcard: all symbols

Protocol (server → client):
    {
      "symbol": "AAPL",
      "price": {"price": 187.42, "bid": 187.41, "ask": 187.43, "spread": 0.02, ...},
      "indicators": {"rsi_14": {"v": 65.3}, "macd": {"macd": 0.02, "signal": 0.01, ...}}
    }
"""
import logging
from collections import defaultdict
from typing import DefaultDict, List, Optional, Set

from fastapi import WebSocket
from starlette.websockets import WebSocketDisconnect

from backend.models.price_event import PriceEvent

logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self):
        # symbol → set of WebSocket connections ("*" = wildcard)
        self._subs: DefaultDict[str, Set[WebSocket]] = defaultdict(set)

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()

    def disconnect(self, ws: WebSocket) -> None:
        for symbol in list(self._subs):
            self._subs[symbol].discard(ws)
            if not self._subs[symbol]:
                del self._subs[symbol]

    def subscribe(self, ws: WebSocket, symbols: List[str]) -> None:
        for symbol in symbols:
            self._subs[symbol.upper()].add(ws)

    async def publish_batch(
        self,
        batch: List[PriceEvent],
        indicator_results: List[dict],
        alert_results: Optional[List[dict]] = None,
    ) -> None:
        """Fan out latest price + indicators to subscribed clients."""
        if not self._subs:
            return

        # Reduce batch: keep only the last price event per symbol
        latest_prices: dict[str, PriceEvent] = {}
        for event in batch:
            latest_prices[event.symbol] = event

        # Group indicator results by symbol
        indicators_by_symbol: DefaultDict[str, dict] = defaultdict(dict)
        for result in indicator_results:
            indicators_by_symbol[result["symbol"]][result["indicator"]] = result["value"]

        symbols_updated = set(latest_prices) | set(indicators_by_symbol)

        for symbol in symbols_updated:
            sockets = self._subs.get(symbol, set()) | self._subs.get("*", set())
            if not sockets:
                continue

            payload: dict = {"type": "market_update", "symbol": symbol}

            if symbol in latest_prices:
                e = latest_prices[symbol]
                payload["price"] = {
                    "price": e.price,
                    "bid": e.bid,
                    "ask": e.ask,
                    "spread": e.spread,
                    "volume": e.volume,
                    "timestamp": e.timestamp.isoformat(),
                    "latency_ms": e.latency_ms(),
                }

            if symbol in indicators_by_symbol:
                payload["indicators"] = dict(indicators_by_symbol[symbol])

            dead: List[WebSocket] = []
            for ws in list(sockets):
                try:
                    await ws.send_json(payload)
                except Exception:
                    dead.append(ws)

            for ws in dead:
                self.disconnect(ws)
                logger.debug("Removed stale WebSocket for %s", symbol)

        for alert in alert_results or []:
            symbol = alert["symbol"]
            sockets = self._subs.get(symbol, set()) | self._subs.get("*", set())
            if not sockets:
                continue

            payload = {
                "type": "alert_triggered",
                "symbol": symbol,
                "alert": alert,
            }

            dead: List[WebSocket] = []
            for ws in list(sockets):
                try:
                    await ws.send_json(payload)
                except Exception:
                    dead.append(ws)

            for ws in dead:
                self.disconnect(ws)
                logger.debug("Removed stale WebSocket after alert %s", alert["id"])


# Module-level singleton — set by main.py at startup
_manager: Optional[ConnectionManager] = None


def get_manager() -> ConnectionManager:
    return _manager


def set_manager(instance: ConnectionManager) -> None:
    global _manager
    _manager = instance
