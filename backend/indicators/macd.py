from typing import Optional
from backend.indicators.base import BaseIndicator
from backend.indicators.ema import EMA


class MACD(BaseIndicator):
    """
    Moving Average Convergence Divergence.

    Three lines:
    - MACD line    = EMA(fast) − EMA(slow)   [momentum]
    - Signal line  = EMA(signal) of MACD     [trigger]
    - Histogram    = MACD − Signal            [crossover strength]

    Crossovers are the key signal:
    - MACD crosses above Signal → bullish (potential buy)
    - MACD crosses below Signal → bearish (potential sell)

    Default params (12, 26, 9) are the industry standard.
    """

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        self.name = "macd"
        self.min_periods = slow + signal
        self._fast = EMA(fast)
        self._slow = EMA(slow)
        self._signal_ema = EMA(signal)

    def update(self, price: float, **kwargs) -> Optional[dict]:
        fast_val = self._fast.update(price)
        slow_val = self._slow.update(price)

        if fast_val is None or slow_val is None:
            return None

        macd_line = fast_val - slow_val
        signal_val = self._signal_ema.update(macd_line)

        if signal_val is None:
            return None

        return {
            "macd": round(macd_line, 6),
            "signal": round(signal_val, 6),
            "histogram": round(macd_line - signal_val, 6),
        }
