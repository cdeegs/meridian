from typing import Optional
from backend.indicators.base import BaseIndicator


class EMA(BaseIndicator):
    """
    Exponential Moving Average.

    Like SMA but gives more weight to recent prices (via the multiplier k).
    React faster to new data — useful for MACD crossover strategies.

    k = 2 / (period + 1)
    EMA = price × k + previous_EMA × (1 − k)

    Seeded with the first price. Considered valid after `period` observations
    (enough for the exponential smoothing to converge).
    """

    def __init__(self, period: int):
        self.name = f"ema_{period}"
        self.min_periods = period
        self._period = period
        self._k = 2.0 / (period + 1)
        self._ema: Optional[float] = None
        self._count = 0

    def update(self, price: float, **kwargs) -> Optional[float]:
        self._count += 1
        if self._ema is None:
            self._ema = price
        else:
            self._ema = price * self._k + self._ema * (1 - self._k)

        if self._count < self._period:
            return None
        return round(self._ema, 6)
