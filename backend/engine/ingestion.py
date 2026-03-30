"""
Ingestion engine — coordinates adapters → queue → batch DB writes.

Design:
- Each adapter runs in its own asyncio task with exponential backoff reconnection
- Price events flow through asyncio.Queue (bounded at 10k)
- Batch writer drains the queue every BATCH_INTERVAL_MS and bulk inserts to TimescaleDB
- After each successful DB write, fans out to the indicator engine and WebSocket broadcaster
- Feed status (connected, last_tick, latency_ms) tracked per source
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from backend.adapters.base import BaseAdapter
from backend.models.price_event import PriceEvent

if TYPE_CHECKING:
    from backend.engine.indicator_engine import IndicatorEngine
    from backend.services.alert_engine import AlertEngine
    from backend.websocket.manager import ConnectionManager

logger = logging.getLogger(__name__)


class FeedStatus:
    def __init__(self):
        self.connected: bool = False
        self.last_tick: Optional[datetime] = None
        self.latency_ms: Optional[float] = None
        self.error: Optional[str] = None
        self.reconnects: int = 0

    def to_dict(self) -> dict:
        return {
            "connected": self.connected,
            "last_tick": self.last_tick.isoformat() if self.last_tick else None,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "reconnects": self.reconnects,
        }


class IngestionEngine:
    def __init__(
        self,
        settings,
        db_engine: AsyncEngine,
        indicator_engine: Optional["IndicatorEngine"] = None,
        alert_engine: Optional["AlertEngine"] = None,
        broadcaster: Optional["ConnectionManager"] = None,
    ):
        self._settings = settings
        self._db_engine = db_engine
        self._indicator_engine = indicator_engine
        self._alert_engine = alert_engine
        self._broadcaster = broadcaster
        self._adapters: Dict[str, BaseAdapter] = {}
        self._adapter_symbols: Dict[str, List[str]] = {}
        self._queue: asyncio.Queue[PriceEvent] = asyncio.Queue(maxsize=10_000)
        self._feed_status: Dict[str, FeedStatus] = {}
        self._tasks: List[asyncio.Task] = []

    def register_adapter(self, adapter: BaseAdapter, symbols: Optional[List[str]] = None) -> None:
        self._adapters[adapter.source] = adapter
        self._adapter_symbols[adapter.source] = symbols or list(self._settings.default_symbols)
        self._feed_status[adapter.source] = FeedStatus()

    async def start(self) -> None:
        for adapter in self._adapters.values():
            task = asyncio.create_task(
                self._run_adapter(adapter),
                name=f"adapter-{adapter.source}",
            )
            self._tasks.append(task)

        writer_task = asyncio.create_task(self._batch_writer(), name="batch-writer")
        self._tasks.append(writer_task)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for adapter in self._adapters.values():
            try:
                await adapter.disconnect()
            except Exception:
                pass

    def get_feed_status(self) -> dict:
        return {source: status.to_dict() for source, status in self._feed_status.items()}

    async def _run_adapter(self, adapter: BaseAdapter) -> None:
        """Run adapter with exponential backoff reconnection."""
        status = self._feed_status[adapter.source]
        backoff = 1.0

        while True:
            status.connected = False
            try:
                await adapter.connect()
                await adapter.subscribe(self._adapter_symbols[adapter.source])
                status.connected = True
                status.error = None
                backoff = 1.0
                logger.info("%s adapter connected", adapter.source)

                async for event in adapter.stream():
                    status.last_tick = event.received_at
                    status.latency_ms = event.latency_ms()
                    try:
                        self._queue.put_nowait(event)
                    except asyncio.QueueFull:
                        logger.warning("%s queue full — dropping tick for %s", adapter.source, event.symbol)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                status.connected = False
                status.error = str(e)
                status.reconnects += 1
                logger.warning(
                    "%s adapter error (reconnect #%d in %.0fs): %s",
                    adapter.source, status.reconnects, backoff, e,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            finally:
                try:
                    await adapter.disconnect()
                except Exception:
                    pass

    async def _batch_writer(self) -> None:
        """Collect events for batch_interval_ms, then bulk insert + fan out."""
        interval = self._settings.batch_interval_ms / 1000.0

        while True:
            batch: List[PriceEvent] = []
            loop = asyncio.get_running_loop()
            deadline = loop.time() + interval

            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    event = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                    batch.append(event)
                except asyncio.TimeoutError:
                    break
                except asyncio.CancelledError:
                    if batch:
                        await self._write_batch(batch)
                    raise

            if batch:
                try:
                    await self._write_batch(batch)
                    await self._fan_out(batch)
                except Exception as e:
                    logger.error("Batch write failed (%d events): %s", len(batch), e)

    async def _write_batch(self, batch: List[PriceEvent]) -> None:
        rows = [
            {
                "time": e.timestamp,
                "symbol": e.symbol,
                "price": e.price,
                "volume": e.volume,
                "bid": e.bid,
                "ask": e.ask,
                "spread": e.spread,
                "source": e.source,
            }
            for e in batch
        ]
        async with self._db_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO ticks (time, symbol, price, volume, bid, ask, spread, source)
                    VALUES (:time, :symbol, :price, :volume, :bid, :ask, :spread, :source)
                """),
                rows,
            )
        logger.debug("Wrote %d ticks to TimescaleDB", len(batch))

    async def _fan_out(self, batch: List[PriceEvent]) -> None:
        """After a successful DB write, compute indicators and broadcast to WebSocket clients."""
        indicator_results: List[dict] = []
        alert_results: List[dict] = []

        if self._indicator_engine is not None:
            try:
                indicator_results = self._indicator_engine.process_batch(batch)
                if indicator_results:
                    await self._write_indicators(indicator_results)
            except Exception as e:
                logger.error("Indicator processing failed: %s", e)

        if self._alert_engine is not None:
            try:
                alert_results = await self._alert_engine.process_batch(batch, indicator_results)
            except Exception as e:
                logger.error("Alert processing failed: %s", e)

        if self._broadcaster is not None:
            try:
                await self._broadcaster.publish_batch(batch, indicator_results, alert_results)
            except Exception as e:
                logger.error("WebSocket broadcast failed: %s", e)

    async def _write_indicators(self, results: List[dict]) -> None:
        rows = [
            {
                "time": r["time"],
                "symbol": r["symbol"],
                "timeframe": r["timeframe"],
                "indicator": r["indicator"],
                "value": json.dumps(r["value"]),
            }
            for r in results
        ]
        async with self._db_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO indicators (time, symbol, timeframe, indicator, value)
                    VALUES (:time, :symbol, :timeframe, :indicator, CAST(:value AS JSONB))
                """),
                rows,
            )
        logger.debug("Wrote %d indicator values", len(rows))
