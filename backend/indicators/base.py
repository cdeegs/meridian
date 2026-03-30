from abc import ABC, abstractmethod
from typing import Optional, Union


class BaseIndicator(ABC):
    """
    Stateful indicator — one instance per symbol.

    Each call to update() ingests one new price point and returns the
    current computed value (or None if there isn't enough data yet).
    """

    name: str        # e.g. "rsi_14", "macd", "bollinger_20"
    min_periods: int  # minimum data points before returning a real value

    @abstractmethod
    def update(
        self,
        price: float,
        volume: Optional[float] = None,
        **kwargs,
    ) -> Optional[Union[float, dict]]:
        """
        Ingest one new price. Returns:
        - float for single-value indicators (SMA, EMA, RSI, VWAP)
        - dict  for multi-value indicators (MACD, Bollinger)
        - None  if not enough data yet (warm-up period)
        """
