import json
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
    schwab_client_id: str = ""
    schwab_client_secret: str = ""
    schwab_redirect_uri: str = "http://127.0.0.1:8765/schwab/callback"
    schwab_scope: str = ""
    schwab_token_path: str = ".schwab_tokens.json"
    schwab_authorize_url: str = "https://api.schwabapi.com/v1/oauth/authorize"
    schwab_token_url: str = "https://api.schwabapi.com/v1/oauth/token"
    schwab_market_data_base_url: str = "https://api.schwabapi.com/marketdata/v1"

    default_symbols: List[str] = [
        "SPY",
        "QQQ",
        "IWM",
        "DIA",
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "META",
        "TSLA",
    ]
    coinbase_enabled: bool = True
    coinbase_symbols: List[str] = ["BTC-USD", "ETH-USD", "SOL-USD"]
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    news_refresh_interval_s: int = 120
    batch_interval_ms: int = 100
    heartbeat_timeout_s: int = 30

    @field_validator("default_symbols", "coinbase_symbols", mode="before")
    @classmethod
    def parse_symbols(cls, v):
        if isinstance(v, str):
            text = v.strip()
            if text.startswith("["):
                parsed = json.loads(text)
                return [str(symbol).strip().upper() for symbol in parsed if str(symbol).strip()]
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

    @property
    def telegram_configured(self) -> bool:
        return self.telegram_enabled and bool(self.telegram_bot_token and self.telegram_chat_id)

    @property
    def schwab_configured(self) -> bool:
        return bool(self.schwab_client_id and self.schwab_client_secret and self.schwab_redirect_uri)


settings = Settings()
