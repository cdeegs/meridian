from datetime import timedelta


def is_equity_symbol(symbol: str) -> bool:
    """Return True if symbol is an equity (no '-' or '/' separator)."""
    return "-" not in symbol and "/" not in symbol


def asset_class(symbol: str) -> str:
    return "stock" if is_equity_symbol(symbol) else "crypto"


TIMEFRAME_SPECS = {
    "1m": {
        "bucket_width": timedelta(minutes=1),
        "window": timedelta(hours=6),
        "warmup_candles": 120,
    },
    "5m": {
        "bucket_width": timedelta(minutes=5),
        "window": timedelta(hours=24),
        "warmup_candles": 120,
    },
    "15m": {
        "bucket_width": timedelta(minutes=15),
        "window": timedelta(days=3),
        "warmup_candles": 120,
    },
    "30m": {
        "bucket_width": timedelta(minutes=30),
        "window": timedelta(days=5),
        "warmup_candles": 120,
    },
    "1h": {
        "bucket_width": timedelta(hours=1),
        "window": timedelta(days=14),
        "warmup_candles": 120,
    },
    "2h": {
        "bucket_width": timedelta(hours=2),
        "window": timedelta(days=21),
        "warmup_candles": 120,
    },
    "4h": {
        "bucket_width": timedelta(hours=4),
        "window": timedelta(days=60),
        "warmup_candles": 120,
    },
    "6h": {
        "bucket_width": timedelta(hours=6),
        "window": timedelta(days=90),
        "warmup_candles": 140,
    },
    "12h": {
        "bucket_width": timedelta(hours=12),
        "window": timedelta(days=180),
        "warmup_candles": 160,
    },
    "1d": {
        "bucket_width": timedelta(days=1),
        "window": timedelta(days=365),
        "warmup_candles": 200,
    },
    "2d": {
        "bucket_width": timedelta(days=2),
        "window": timedelta(days=730),
        "warmup_candles": 220,
    },
    "1w": {
        "bucket_width": timedelta(days=7),
        "window": timedelta(days=1825),
        "warmup_candles": 260,
    },
}
