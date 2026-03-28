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
    batch_interval_ms: int = 100
    heartbeat_timeout_s: int = 30

    @field_validator("default_symbols", mode="before")
    @classmethod
    def parse_symbols(cls, v):
        if isinstance(v, str):
            return [s.strip().upper() for s in v.split(",") if s.strip()]
        return v


settings = Settings()
