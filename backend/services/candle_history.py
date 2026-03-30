from __future__ import annotations

from datetime import datetime, timedelta, timezone

_BUCKET_ANCHOR = datetime(2001, 1, 1, tzinfo=timezone.utc)


def expected_candle_count(
    start: datetime,
    end: datetime,
    bucket_width: timedelta,
    limit: int,
) -> int:
    if end < start:
        return 0

    bucket_seconds = max(int(bucket_width.total_seconds()), 1)
    span_seconds = max(int((end - start).total_seconds()), 0)
    raw_count = (span_seconds // bucket_seconds) + 1
    return max(1, min(limit, raw_count))


def coverage_looks_complete(
    candles: list[dict],
    start: datetime,
    end: datetime,
    bucket_width: timedelta,
    limit: int,
    tolerance_buckets: int = 1,
) -> bool:
    if not candles:
        return False

    expected = expected_candle_count(start=start, end=end, bucket_width=bucket_width, limit=limit)
    if len(candles) + tolerance_buckets < expected:
        return False

    earliest_allowed = start + (bucket_width * tolerance_buckets)
    latest_allowed = end - (bucket_width * tolerance_buckets)

    if candles[0]["time"] > earliest_allowed:
        return False
    if latest_allowed > start and candles[-1]["time"] < latest_allowed:
        return False
    return True


def merge_candles(local_candles: list[dict], provider_candles: list[dict]) -> list[dict]:
    merged: dict[datetime, dict] = {
        candle["time"]: dict(candle)
        for candle in provider_candles
        if candle.get("time") is not None
    }

    for candle in local_candles:
        timestamp = candle.get("time")
        if timestamp is None:
            continue
        merged[timestamp] = dict(candle)

    return sorted(merged.values(), key=lambda candle: candle["time"])


def aggregate_candles(candles: list[dict], bucket_width: timedelta) -> list[dict]:
    if not candles:
        return []

    bucket_seconds = max(int(bucket_width.total_seconds()), 1)
    grouped: dict[datetime, dict] = {}

    for candle in sorted(candles, key=lambda item: item["time"]):
        timestamp = candle.get("time")
        if timestamp is None:
            continue
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)

        bucket = _bucket_start(timestamp, bucket_seconds)
        current = grouped.get(bucket)
        if current is None:
            grouped[bucket] = {
                "time": bucket,
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"]),
                "volume": float(candle.get("volume", 0.0) or 0.0),
                "ticks": int(candle.get("ticks", 0) or 0),
                "source": candle.get("source"),
            }
            continue

        current["high"] = max(current["high"], float(candle["high"]))
        current["low"] = min(current["low"], float(candle["low"]))
        current["close"] = float(candle["close"])
        current["volume"] += float(candle.get("volume", 0.0) or 0.0)
        current["ticks"] += int(candle.get("ticks", 0) or 0)
        if current.get("source") is None and candle.get("source") is not None:
            current["source"] = candle.get("source")

    return sorted(grouped.values(), key=lambda candle: candle["time"])


def _bucket_start(timestamp: datetime, bucket_seconds: int) -> datetime:
    delta = timestamp - _BUCKET_ANCHOR
    total_seconds = int(delta.total_seconds())
    bucket_offset = (total_seconds // bucket_seconds) * bucket_seconds
    return _BUCKET_ANCHOR + timedelta(seconds=bucket_offset)
