from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from backend.indicators.base import BaseIndicator

_ET = ZoneInfo("America/New_York")


def _is_new_session(prev_time: datetime, curr_time: datetime) -> bool:
    prev_et = prev_time.astimezone(_ET)
    curr_et = curr_time.astimezone(_ET)
    prev_date = prev_et.date()
    curr_date = curr_et.date()
    if curr_date > prev_date:
        return True
    return False


class VWAP(BaseIndicator):
    """
    Volume Weighted Average Price.

    The "fair value" benchmark used by institutional traders. Calculated as:
        VWAP = Σ(price × volume) / Σ(volume)

    Resets at each new NYSE trading day so each session gets its own VWAP.
    Crypto is 24/7 — for non-equity symbols you'd want a rolling VWAP instead.

    A stock trading above VWAP → buyers are in control.
    A stock trading below VWAP → sellers are in control.
    """

    def __init__(self):
        self.name = "vwap"
        self.min_periods = 1
        self._cum_pv: float = 0.0   # cumulative price × volume
        self._cum_vol: float = 0.0  # cumulative volume
        self._last_time: Optional[datetime] = None

    def update(
        self,
        price: float,
        volume: Optional[float] = None,
        timestamp: Optional[datetime] = None,
        **kwargs,
    ) -> Optional[float]:
        if volume is None or volume <= 0:
            return None

        if timestamp is not None:
            self._maybe_reset(timestamp)

        self._cum_pv += price * volume
        self._cum_vol += volume

        if self._cum_vol == 0:
            return None

        return round(self._cum_pv / self._cum_vol, 4)

    def _maybe_reset(self, timestamp: datetime) -> None:
        if timestamp.tzinfo is None:
            return

        event_time = timestamp.astimezone(timezone.utc)
        if self._last_time is None:
            self._last_time = event_time
            return

        if _is_new_session(self._last_time, event_time):
            self._cum_pv = 0.0
            self._cum_vol = 0.0
        self._last_time = event_time
