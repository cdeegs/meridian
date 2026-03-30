from typing import List
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str = "postgresql+asyncpg://meridian:meridian@localhost:5432/meridian"
    redis_url: str = "redis://localhost:6379/0"

    alpaca_api_key: str = ""
    alpaca_api_secret: str = ""
    alpaca_feed: str = "iex"  # iex (free) or sip (paid)

    default_symbols: List[str] = ["SPY", "AAPL", "TSLA"]
    coinbase_enabled: bool = True
    coinbase_symbols: List[str] = ["BTC-USD", "ETH-USD", "SOL-USD"]
    batch_interval_ms: int = 100
    heartbeat_timeout_s: int = 30

    @field_validator("default_symbols", "coinbase_symbols", mode="before")
    @classmethod
    def parse_symbols(cls, v):
        if isinstance(v, str):
            return [s.strip().upper() for s in v.split(",") if s.strip()]
        return v

    @property
    def all_symbols(self) -> List[str]:
        seen = set()
        ordered: List[str] = []
        configured = list(self.default_symbols)
        if self.coinbase_enabled:
            configured.extend(self.coinbase_symbols)

        for symbol in configured:
            if symbol not in seen:
                seen.add(symbol)
                ordered.append(symbol)
        return ordered


settings = Settings()
