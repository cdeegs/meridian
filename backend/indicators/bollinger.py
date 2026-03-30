from collections import deque
from typing import Optional
import numpy as np
from backend.indicators.base import BaseIndicator


class BollingerBands(BaseIndicator):
    """
    Bollinger Bands.

    A volatility envelope around a moving average:
    - Upper band  = SMA + (num_std × σ)
    - Middle band = SMA(period)
    - Lower band  = SMA − (num_std × σ)

    When price touches the upper band → potentially overbought.
    When price touches the lower band → potentially oversold.
    Bandwidth (% of price) shows how volatile the market is right now.

    Default: 20-period SMA with 2 standard deviations (covers ~95% of moves).
    """

    def __init__(self, period: int = 20, num_std: float = 2.0):
        self.name = f"bollinger_{period}"
        self.min_periods = period
        self._period = period
        self._num_std = num_std
        self._window: deque = deque(maxlen=period)

    def update(self, price: float, **kwargs) -> Optional[dict]:
        self._window.append(price)
        if len(self._window) < self._period:
            return None

        arr = np.array(self._window)
        middle = float(np.mean(arr))
        std = float(np.std(arr, ddof=0))
        upper = middle + self._num_std * std
        lower = middle - self._num_std * std

        # Bandwidth: how wide the bands are as % of middle — measures current volatility
        bandwidth = ((upper - lower) / middle * 100) if middle != 0 else 0.0

        return {
            "upper": round(upper, 4),
            "middle": round(middle, 4),
            "lower": round(lower, 4),
            "bandwidth": round(bandwidth, 4),
        }
