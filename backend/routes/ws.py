"""
WebSocket streaming endpoint.

Single endpoint: /ws/stream
Clients subscribe to symbols and receive combined price + indicator updates.

Connect:
    ws://localhost:8000/ws/stream

Subscribe (send after connect):
    {"action": "subscribe", "symbols": ["AAPL", "SPY"]}
    {"action": "subscribe", "symbols": ["*"]}   ← all symbols

Receive market updates:
    {
      "type": "market_update",
      "symbol": "AAPL",
      "price": {
        "price": 187.42, "bid": 187.41, "ask": 187.43, "spread": 0.02,
        "volume": 150, "timestamp": "2026-03-27T14:30:00.123Z", "latency_ms": 8.2
      },
      "indicators": {
        "sma_20": {"v": 186.10},
        "rsi_14": {"v": 65.3},
        "macd":   {"macd": 0.24, "signal": 0.18, "histogram": 0.06},
        "bollinger_20": {"upper": 190.0, "middle": 187.5, "lower": 185.0, "bandwidth": 2.4}
      }
    }

Receive alerts:
    {
      "type": "alert_triggered",
      "symbol": "AAPL",
      "alert": {
        "id": "...",
        "condition": "rsi_above",
        "message": "AAPL RSI rose above 70.0 (current 72.3)"
      }
    }
"""
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from backend.websocket.manager import get_manager

router = APIRouter(tags=["websocket"])
logger = logging.getLogger(__name__)


@router.websocket("/ws/stream")
async def stream(websocket: WebSocket):
    manager = get_manager()
    if manager is None:
        await websocket.close(code=1011, reason="Server not ready")
        return

    await manager.connect(websocket)
    logger.info("WebSocket client connected")

    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "subscribe":
                symbols = data.get("symbols", [])
                manager.subscribe(websocket, symbols)
                await websocket.send_json({
                    "type": "subscribed",
                    "symbols": [s.upper() for s in symbols],
                })
                logger.info("Client subscribed to %s", symbols)

            elif action == "ping":
                await websocket.send_json({"type": "pong"})

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
        manager.disconnect(websocket)
    except Exception as e:
        logger.warning("WebSocket error: %s", e)
        manager.disconnect(websocket)
