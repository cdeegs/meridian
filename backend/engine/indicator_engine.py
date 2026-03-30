"""
Indicator Engine — manages per-symbol rolling state and computes indicators.

Design:
- One set of stateful indicator instances per symbol (instantiated on first tick)
- process_batch() ingests a batch and returns ONE result per (symbol, indicator)
  i.e. only the final value after all ticks in the batch have been processed
- _compute_event() is the hot path — called directly by the warmup service
  without writing to DB or broadcasting
"""
import logging
from typing import Dict, List, Optional

from backend.indicators import SMA, EMA, RSI, MACD, BollingerBands, VWAP
from backend.indicators.base import BaseIndicator
from backend.models.price_event import PriceEvent

logger = logging.getLogger(__name__)

# Indicators to compute per symbol — add more as phases progress
_INDICATOR_FACTORIES = [
    lambda: SMA(20),
    lambda: SMA(50),
    lambda: EMA(12),
    lambda: EMA(26),
    lambda: RSI(14),
    lambda: MACD(),
    lambda: BollingerBands(20),
    lambda: VWAP(),
]


class IndicatorEngine:
    def __init__(self):
        # symbol → {indicator_name → indicator_instance}
        self._states: Dict[str, Dict[str, BaseIndicator]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process_batch(self, batch: List[PriceEvent]) -> List[dict]:
        """
        Ingest a batch. Returns one result per (symbol, indicator) —
        only the last value computed after all ticks in the batch.
        Skips indicators that haven't warmed up yet.
        """
        # symbol → indicator_name → result dict (overwritten on each tick)
        latest: Dict[str, Dict[str, dict]] = {}

        for event in batch:
            state = self._get_state(event.symbol)
            for name, indicator in state.items():
                value = self._call_indicator(indicator, event)
                if value is not None:
                    if event.symbol not in latest:
                        latest[event.symbol] = {}
                    latest[event.symbol][name] = {
                        "time": event.timestamp,
                        "symbol": event.symbol,
                        "timeframe": "1m",
                        "indicator": name,
                        "value": value if isinstance(value, dict) else {"v": round(float(value), 6)},
                    }

        return [r for symbol_results in latest.values() for r in symbol_results.values()]

    def compute_event(self, event: PriceEvent) -> None:
        """
        Feed one event into indicator state without returning results.
        Used by the warmup service to pre-fill rolling windows from DB history.
        """
        state = self._get_state(event.symbol)
        for indicator in state.values():
            self._call_indicator(indicator, event)

    def latest_for_symbol(self, symbol: str) -> Optional[Dict[str, BaseIndicator]]:
        return self._states.get(symbol)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_state(self, symbol: str) -> Dict[str, BaseIndicator]:
        if symbol not in self._states:
            state: Dict[str, BaseIndicator] = {}
            for factory in _INDICATOR_FACTORIES:
                indicator = factory()
                state[indicator.name] = indicator
            self._states[symbol] = state
        return self._states[symbol]

    @staticmethod
    def _call_indicator(indicator: BaseIndicator, event: PriceEvent):
        try:
            return indicator.update(
                price=event.price,
                volume=event.volume,
                timestamp=event.timestamp,
            )
        except Exception as e:
            logger.warning("Indicator %s failed on %s: %s", indicator.name, event.symbol, e)
            return None
