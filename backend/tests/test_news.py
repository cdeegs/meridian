from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from backend.routes import news as news_routes
from backend.services.news_service import NewsService, NewsSource


def test_news_service_scores_macro_headline_as_high_impact():
    service = NewsService(tracked_symbols=["SPY", "QQQ", "BTC-USD", "ETH-USD"])
    item = service._enrich_item(
        NewsSource(
            key="federal_reserve",
            label="Federal Reserve",
            url="https://www.federalreserve.gov/feeds/press_all.xml",
            market_bucket="macro",
            base_impact=90,
        ),
        {
            "title": "Federal Reserve signals rate cuts as inflation cools",
            "summary": "Cooling inflation keeps the market focused on the path for policy easing.",
            "url": "https://example.com/fed",
            "published_at": "Mon, 30 Mar 2026 16:00:00 GMT",
        },
    )

    assert item["category"] == "macro"
    assert item["impact_level"] == "high"
    assert item["bias"] == "bullish"
    assert "SPY" in item["affected_symbols"]
    assert "BTC-USD" in item["affected_symbols"]
    assert "rate expectations" in item["why_it_matters"]


def test_news_service_tags_crypto_security_story():
    service = NewsService(tracked_symbols=["BTC-USD", "ETH-USD", "SOL-USD"])
    item = service._enrich_item(
        NewsSource(
            key="coindesk",
            label="CoinDesk",
            url="https://www.coindesk.com/arc/outboundfeeds/rss/",
            market_bucket="crypto",
            base_impact=64,
        ),
        {
            "title": "Solana exchange hack drains hot wallets after exploit",
            "summary": "Security teams are still investigating the exploit and on-chain outflows.",
            "url": "https://example.com/sol-hack",
            "published_at": "Mon, 30 Mar 2026 15:00:00 GMT",
        },
    )

    assert item["category"] == "security"
    assert item["market_bucket"] == "crypto"
    assert item["bias"] == "bearish"
    assert "SOL-USD" in item["affected_symbols"]
    assert item["horizon"] == "Intraday"


@pytest.mark.asyncio
async def test_news_service_filters_cached_items():
    service = NewsService(tracked_symbols=["BTC-USD", "SPY"])
    service._cached_items = [
        {
            "id": "1",
            "impact_level": "high",
            "impact_score": 92,
            "bias": "bullish",
            "market_bucket": "crypto",
            "category": "crypto",
            "title": "Bitcoin ETF inflows accelerate",
            "source_label": "CoinDesk",
            "watch_next": "Watch BTC-USD for follow-through.",
            "affected_symbols": ["BTC-USD"],
            "published_at": datetime.now(timezone.utc).isoformat(),
        },
        {
            "id": "2",
            "impact_level": "medium",
            "impact_score": 71,
            "bias": "bearish",
            "market_bucket": "stock",
            "category": "regulation",
            "title": "SEC opens new probe",
            "source_label": "SEC",
            "watch_next": "Watch SPY for follow-through.",
            "affected_symbols": ["SPY"],
            "published_at": datetime.now(timezone.utc).isoformat(),
        },
    ]
    service._last_refreshed_at = datetime.now(timezone.utc)

    payload = await service.list_news(symbol="BTC-USD", market_bucket="crypto", impact="high")

    assert [item["id"] for item in payload["news"]] == ["1"]
    assert payload["brief"]["conditions"][0]["label"] == "Risk Tone"
    assert payload["brief"]["drivers"][0]["id"] == "1"
    assert payload["brief"]["narratives"]
    assert payload["brief"]["what_changed"]


@pytest.mark.asyncio
async def test_news_route_validates_filters():
    with pytest.raises(HTTPException) as exc_info:
        await news_routes.list_news(market_bucket="bad")

    assert exc_info.value.status_code == 400
