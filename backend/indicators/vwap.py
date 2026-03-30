from datetime import datetime, timezone, time as dt_time
from typing import Optional
from backend.indicators.base import BaseIndicator

# NYSE open = 09:30 ET = 14:30 UTC (ignores daylight saving for simplicity)
_NYSE_OPEN_UTC = dt_time(14, 30, 0)


class VWAP(BaseIndicator):
    """
    Volume Weighted Average Price.

    The "fair value" benchmark used by institutional traders. Calculated as:
        VWAP = Σ(price × volume) / Σ(volume)

    Resets at NYSE open (14:30 UTC) so each trading session gets its own VWAP.
    Crypto is 24/7 — for non-equity symbols you'd want a rolling VWAP instead.

    A stock trading above VWAP → buyers are in control.
    A stock trading below VWAP → sellers are in control.
    """

    def __init__(self):
        self.name = "vwap"
        self.min_periods = 1
        self._cum_pv: float = 0.0   # cumulative price × volume
        self._cum_vol: float = 0.0  # cumulative volume
        self._session_date: Optional[datetime] = None

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
        """Reset at NYSE open (14:30 UTC) each day."""
        if timestamp.tzinfo is None:
            return

        ts_utc = timestamp.astimezone(timezone.utc)
        session_start = ts_utc.replace(
            hour=_NYSE_OPEN_UTC.hour,
            minute=_NYSE_OPEN_UTC.minute,
            second=0,
            microsecond=0,
        )

        if self._session_date is None:
            self._session_date = session_start
            return

        if ts_utc >= session_start > self._session_date:
            # New session — reset accumulators
            self._cum_pv = 0.0
            self._cum_vol = 0.0
            self._session_date = session_start
