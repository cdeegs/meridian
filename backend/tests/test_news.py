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


def test_news_service_classifies_geopolitics_story():
    service = NewsService(tracked_symbols=["SPY", "QQQ", "BTC-USD"])
    item = service._enrich_item(
        NewsSource(
            key="bbc_world",
            label="BBC World",
            url="https://feeds.bbci.co.uk/news/world/rss.xml",
            market_bucket="macro",
            base_impact=72,
        ),
        {
            "title": "Oil jumps after missile strikes raise fears of wider war and sanctions",
            "summary": "Markets are watching shipping lanes and energy supply after the latest escalation.",
            "url": "https://example.com/war-risk",
            "published_at": "Mon, 30 Mar 2026 14:00:00 GMT",
        },
    )

    assert item["category"] == "geopolitics"
    assert item["market_bucket"] == "macro"
    assert item["impact_level"] in {"high", "medium"}
    assert "SPY" in item["affected_symbols"]


def test_news_service_classifies_analyst_story():
    service = NewsService(tracked_symbols=["AAPL", "SPY", "QQQ"])
    item = service._enrich_item(
        NewsSource(
            key="nasdaq_stocks",
            label="Nasdaq Stocks",
            url="https://www.nasdaq.com/feed/rssoutbound?category=Stocks",
            market_bucket="stock",
            base_impact=62,
        ),
        {
            "title": "Apple shares rise after analyst upgrade and higher price target",
            "summary": "The analyst cited stronger iPhone demand and raised the firm's target on AAPL.",
            "url": "https://example.com/aapl-analyst",
            "published_at": "Mon, 30 Mar 2026 13:00:00 GMT",
        },
    )

    assert item["category"] == "analyst"
    assert item["market_bucket"] == "stock"
    assert "AAPL" in item["affected_symbols"]
    assert item["horizon"] == "Swing"


def test_news_service_does_not_misclassify_regulatory_sanctions_as_geopolitics():
    service = NewsService(tracked_symbols=["SPY", "QQQ", "BTC-USD"])
    item = service._enrich_item(
        NewsSource(
            key="cftc_enforcement",
            label="CFTC Enforcement",
            url="https://www.cftc.gov/RSS/RSSENF/rssenf.xml",
            market_bucket="macro",
            base_impact=78,
        ),
        {
            "title": "CFTC obtains sanctions and restitution in California precious metals fraud case",
            "summary": "Regulators said the court ordered restitution and civil penalties after enforcement action.",
            "url": "https://example.com/cftc-sanctions",
            "published_at": "Mon, 30 Mar 2026 12:00:00 GMT",
        },
    )

    assert item["category"] == "regulation"
    assert item["market_bucket"] == "macro"
    assert item["horizon"] == "Swing"


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
async def test_news_service_supports_sort_bias_and_horizon_filters():
    service = NewsService(tracked_symbols=["BTC-USD", "SPY", "AAPL"])
    service._cached_items = [
        {
            "id": "older-bearish",
            "impact_level": "high",
            "impact_score": 95,
            "bias": "bearish",
            "horizon": "Swing",
            "market_bucket": "stock",
            "category": "earnings",
            "title": "Older bearish item",
            "source_label": "Nasdaq Earnings",
            "watch_next": "Watch AAPL.",
            "affected_symbols": ["AAPL"],
            "published_at": "2026-03-29T10:00:00+00:00",
        },
        {
            "id": "newer-bearish",
            "impact_level": "medium",
            "impact_score": 76,
            "bias": "bearish",
            "horizon": "Swing",
            "market_bucket": "stock",
            "category": "company",
            "title": "Newer bearish item",
            "source_label": "BBC Business",
            "watch_next": "Watch SPY.",
            "affected_symbols": ["SPY"],
            "published_at": "2026-03-30T11:00:00+00:00",
        },
        {
            "id": "macro-bullish",
            "impact_level": "high",
            "impact_score": 93,
            "bias": "bullish",
            "horizon": "Regime",
            "market_bucket": "macro",
            "category": "macro",
            "title": "Macro bullish item",
            "source_label": "Federal Reserve",
            "watch_next": "Watch BTC-USD.",
            "affected_symbols": ["BTC-USD"],
            "published_at": "2026-03-30T12:00:00+00:00",
        },
    ]
    service._last_refreshed_at = datetime.now(timezone.utc)

    payload = await service.list_news(bias="bearish", horizon="swing", sort="newest")

    assert [item["id"] for item in payload["news"]] == ["newer-bearish", "older-bearish"]
    assert payload["brief"]["scope_label"].endswith("Sorted Newest")


@pytest.mark.asyncio
async def test_news_route_validates_filters():
    with pytest.raises(HTTPException) as exc_info:
        await news_routes.list_news(market_bucket="bad")

    assert exc_info.value.status_code == 400

    with pytest.raises(HTTPException) as sort_exc:
        await news_routes.list_news(sort="bad")

    assert sort_exc.value.status_code == 400
