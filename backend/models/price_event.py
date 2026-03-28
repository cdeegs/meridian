from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class PriceEvent:
    symbol: str
    price: float
    source: str
    timestamp: datetime
    volume: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    spread: Optional[float] = None
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def latency_ms(self) -> Optional[float]:
        """Feed latency: received_at - exchange timestamp, in milliseconds."""
        if self.timestamp.tzinfo is None:
            return None
        delta = self.received_at - self.timestamp
        return round(delta.total_seconds() * 1000, 2)
