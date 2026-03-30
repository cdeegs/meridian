from collections import deque
from typing import Optional
from backend.indicators.base import BaseIndicator


class SMA(BaseIndicator):
    """
    Simple Moving Average.

    The most basic trend indicator — just the arithmetic mean of the last
    `period` prices. Useful for spotting direction but lags the market.
    """

    def __init__(self, period: int):
        self.name = f"sma_{period}"
        self.min_periods = period
        self._period = period
        self._window: deque = deque(maxlen=period)

    def update(self, price: float, **kwargs) -> Optional[float]:
        self._window.append(price)
        if len(self._window) < self._period:
            return None
        return round(sum(self._window) / self._period, 6)
