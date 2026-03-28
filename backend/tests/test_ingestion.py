"""
Tests for the ingestion layer — adapter normalization and batch writer logic.
Run with: pytest backend/tests/ -v
"""
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.models.price_event import PriceEvent
from backend.adapters.alpaca import AlpacaAdapter


class TestPriceEvent:
    def test_latency_ms_with_timezone(self):
        ts = datetime(2026, 3, 27, 14, 30, 0, tzinfo=timezone.utc)
        event = PriceEvent(
            symbol="AAPL",
            price=187.42,
            source="alpaca",
            timestamp=ts,
        )
        # received_at is set to now — latency should be a small positive number
        assert event.latency_ms() is not None
        assert event.latency_ms() >= 0

    def test_latency_ms_without_timezone(self):
        ts = datetime(2026, 3, 27, 14, 30, 0)  # naive datetime
        event = PriceEvent(symbol="AAPL", price=187.42, source="alpaca", timestamp=ts)
        assert event.latency_ms() is None

    def test_spread_calculation(self):
        event = PriceEvent(
            symbol="AAPL",
            price=187.42,
            bid=187.41,
            ask=187.43,
            spread=round(187.43 - 187.41, 4),
            source="alpaca",
            timestamp=datetime.now(timezone.utc),
        )
        assert event.spread == pytest.approx(0.02, abs=1e-6)


class TestAlpacaAdapter:
    def _make_adapter(self):
        return AlpacaAdapter(api_key="test", api_secret="test", feed="iex")

    def test_parse_trade_no_quote(self):
        adapter = self._make_adapter()
        msg = {
            "T": "t",
            "S": "AAPL",
            "p": 187.42,
            "s": 150,
            "t": "2026-03-27T14:30:00.123Z",
        }
        event = adapter._parse(msg)
        assert event is not None
        assert event.symbol == "AAPL"
        assert event.price == 187.42
        assert event.volume == 150
        assert event.bid is None
        assert event.ask is None
        assert event.spread is None
        assert event.source == "alpaca"

    def test_parse_trade_with_prior_quote(self):
        adapter = self._make_adapter()
        # First a quote update
        adapter._parse({"T": "q", "S": "AAPL", "bp": 187.41, "ap": 187.43})
        # Then a trade
        event = adapter._parse({
            "T": "t",
            "S": "AAPL",
            "p": 187.42,
            "s": 100,
            "t": "2026-03-27T14:30:00.500Z",
        })
        assert event is not None
        assert event.bid == 187.41
        assert event.ask == 187.43
        assert event.spread == pytest.approx(0.02, abs=1e-6)

    def test_parse_quote_returns_none(self):
        adapter = self._make_adapter()
        result = adapter._parse({"T": "q", "S": "SPY", "bp": 450.10, "ap": 450.11})
        assert result is None  # quote cached, not emitted

    def test_parse_unknown_type_returns_none(self):
        adapter = self._make_adapter()
        result = adapter._parse({"T": "subscription", "trades": ["AAPL"]})
        assert result is None

    def test_parse_invalid_timestamp_falls_back_to_now(self):
        adapter = self._make_adapter()
        event = adapter._parse({
            "T": "t",
            "S": "TSLA",
            "p": 250.0,
            "s": 50,
            "t": "not-a-timestamp",
        })
        assert event is not None
        assert event.timestamp.tzinfo is not None  # should have timezone


class TestIngestionEngine:
    @pytest.mark.asyncio
    async def test_batch_writer_writes_on_data(self):
        """Batch writer should call _write_batch when events are queued."""
        from backend.config import Settings
        from backend.engine.ingestion import IngestionEngine

        settings = Settings(
            database_url="postgresql+asyncpg://x:x@localhost/x",
            batch_interval_ms=50,
        )
        mock_engine = MagicMock()

        engine = IngestionEngine(settings=settings, db_engine=mock_engine)
        engine._write_batch = AsyncMock()

        # Put an event in the queue
        event = PriceEvent(
            symbol="AAPL",
            price=187.42,
            source="alpaca",
            timestamp=datetime.now(timezone.utc),
        )
        await engine._queue.put(event)

        # Run batch writer for one cycle
        task = asyncio.create_task(engine._batch_writer())
        await asyncio.sleep(0.1)  # let one batch interval pass
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        engine._write_batch.assert_called_once()
        batch = engine._write_batch.call_args[0][0]
        assert len(batch) == 1
        assert batch[0].symbol == "AAPL"
