from typing import Optional
from backend.indicators.base import BaseIndicator


class RSI(BaseIndicator):
    """
    Relative Strength Index (Wilder's smoothing).

    RSI measures overbought / oversold conditions:
    - RSI > 70  → overbought (price may reverse down)
    - RSI < 30  → oversold  (price may reverse up)
    - RSI = 50  → neutral

    Uses Wilder's smoothed average (factor = 1/period) rather than standard
    EMA (factor = 2/(period+1)). This is the standard in trading platforms.

    RS = avg_gain / avg_loss
    RSI = 100 − (100 / (1 + RS))
    """

    def __init__(self, period: int = 14):
        self.name = f"rsi_{period}"
        self.min_periods = period + 1  # need period changes, so period+1 prices
        self._period = period
        self._prev_price: Optional[float] = None
        self._avg_gain: Optional[float] = None
        self._avg_loss: Optional[float] = None
        self._seed_gains: list[float] = []
        self._seed_losses: list[float] = []
        self._initialized: bool = False
        self._count = 0

    def update(self, price: float, **kwargs) -> Optional[float]:
        if self._prev_price is None:
            self._prev_price = price
            return None

        change = price - self._prev_price
        self._prev_price = price
        self._count += 1

        gain = max(change, 0.0)
        loss = abs(min(change, 0.0))

        if not self._initialized:
            # Accumulate initial values for the first SMA seed
            self._seed_gains.append(gain)
            self._seed_losses.append(loss)

            if self._count < self._period:
                return None

            # Seed with simple average of first `period` changes
            self._avg_gain = sum(self._seed_gains) / self._period
            self._avg_loss = sum(self._seed_losses) / self._period
            self._initialized = True
        else:
            # Wilder's smoothed average
            self._avg_gain = (self._avg_gain * (self._period - 1) + gain) / self._period
            self._avg_loss = (self._avg_loss * (self._period - 1) + loss) / self._period

        if self._avg_loss == 0:
            return 100.0

        rs = self._avg_gain / self._avg_loss
        return round(100 - (100 / (1 + rs)), 4)
