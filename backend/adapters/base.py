from abc import ABC, abstractmethod
from typing import AsyncIterator, List
from backend.models.price_event import PriceEvent


class BaseAdapter(ABC):
    """
    Abstract base for exchange WebSocket adapters.

    Each exchange gets one subclass. The ingestion engine calls these in a
    retry loop — adapters only need to handle one connection lifecycle.
    """

    source: str  # e.g. "alpaca", "coinbase"

    @abstractmethod
    async def connect(self) -> None:
        """Open WebSocket connection and authenticate."""

    @abstractmethod
    async def subscribe(self, symbols: List[str]) -> None:
        """Subscribe to price feed for the given symbols."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the WebSocket connection cleanly."""

    @abstractmethod
    async def stream(self) -> AsyncIterator[PriceEvent]:
        """
        Yield normalized PriceEvent objects until the connection closes.
        Raises on unrecoverable errors (caller handles reconnection).
        """
