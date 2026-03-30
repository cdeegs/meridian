from backend.config import Settings


def test_settings_parse_symbols_from_csv():
    settings = Settings(default_symbols="spy, aapl , tsla")
    assert settings.default_symbols == ["SPY", "AAPL", "TSLA"]


def test_settings_parse_symbols_from_json_array():
    settings = Settings(coinbase_symbols='["btc-usd", "eth-usd"]')
    assert settings.coinbase_symbols == ["BTC-USD", "ETH-USD"]
