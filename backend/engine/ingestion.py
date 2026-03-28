"""
Ingestion engine — coordinates adapters → queue → batch DB writes.

Design:
- Each adapter runs in its own asyncio task with exponential backoff reconnection
- Price events flow through asyncio.Queue (bounded at 10k)
- Batch writer drains the queue every BATCH_INTERVAL_MS and bulk inserts to TimescaleDB
- Feed status (connected, last_tick, latency_ms) tracked per source
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from backend.adapters.base import BaseAdapter
from backend.models.price_event import PriceEvent

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
    def __init__(self, settings, db_engine: AsyncEngine):
        self._settings = settings
        self._db_engine = db_engine
        self._adapters: Dict[str, BaseAdapter] = {}
        self._queue: asyncio.Queue[PriceEvent] = asyncio.Queue(maxsize=10_000)
        self._feed_status: Dict[str, FeedStatus] = {}
        self._tasks: List[asyncio.Task] = []

    def register_adapter(self, adapter: BaseAdapter) -> None:
        self._adapters[adapter.source] = adapter
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
                await adapter.subscribe(self._settings.default_symbols)
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
                logger.warning("%s adapter error (reconnect #%d in %ds): %s", adapter.source, status.reconnects, backoff, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            finally:
                try:
                    await adapter.disconnect()
                except Exception:
                    pass

    async def _batch_writer(self) -> None:
        """Collect events for batch_interval_ms, then bulk insert."""
        interval = self._settings.batch_interval_ms / 1000.0

        while True:
            batch: List[PriceEvent] = []
            deadline = asyncio.get_event_loop().time() + interval

            while True:
                remaining = deadline - asyncio.get_event_loop().time()
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
