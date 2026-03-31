import logging

logger = logging.getLogger(__name__)


class SchwabMarketDataClient:
    def __init__(self, *args, **kwargs):
        self._stock_symbols = [symbol.upper() for symbol in (kwargs.get("stock_symbols") or [])]

    @property
    def stock_symbols(self) -> list[str]:
        return list(self._stock_symbols)

    @property
    def ready(self) -> bool:
        return False

    def get_status(self) -> dict:
        return {"implementation": "pending", "configured": False}

    async def fetch_watchlist_snapshots(self) -> dict[str, dict]:
        logger.info("Schwab market-data client is pending and returned no watchlist snapshots")
        return {}

    async def fetch_snapshot(self, symbol: str) -> dict:
        logger.info("Schwab market-data client is pending and returned no snapshot for %s", symbol.upper())
        return {}

    async def fetch_bars(
        self,
        symbol: str,
        timeframe: str,
        start=None,
        end=None,
        limit: int = 200,
    ) -> list[dict]:
        _ = (start, end, limit)
        logger.info("Schwab market-data client is pending and returned no bars for %s (%s)", symbol.upper(), timeframe)
        return []
