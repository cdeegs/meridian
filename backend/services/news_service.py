import asyncio
import hashlib
import html
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Optional
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import httpx

logger = logging.getLogger(__name__)

_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class NewsSource:
    key: str
    label: str
    url: str
    market_bucket: str
    base_impact: int


_DEFAULT_SOURCES = (
    NewsSource(
        key="federal_reserve_press",
        label="Federal Reserve",
        url="https://www.federalreserve.gov/feeds/press_all.xml",
        market_bucket="macro",
        base_impact=90,
    ),
    NewsSource(
        key="federal_reserve_policy",
        label="Fed Policy",
        url="https://www.federalreserve.gov/feeds/press_monetary.xml",
        market_bucket="macro",
        base_impact=92,
    ),
    NewsSource(
        key="federal_reserve_speeches",
        label="Fed Speeches",
        url="https://www.federalreserve.gov/feeds/speeches.xml",
        market_bucket="macro",
        base_impact=80,
    ),
    NewsSource(
        key="federal_reserve_testimony",
        label="Fed Testimony",
        url="https://www.federalreserve.gov/feeds/testimony.xml",
        market_bucket="macro",
        base_impact=84,
    ),
    NewsSource(
        key="sec",
        label="SEC",
        url="https://www.sec.gov/rss/news/press.xml",
        market_bucket="macro",
        base_impact=82,
    ),
    NewsSource(
        key="cftc",
        label="CFTC",
        url="https://www.cftc.gov/RSS/RSSGP/rssgp.xml",
        market_bucket="macro",
        base_impact=78,
    ),
    NewsSource(
        key="cftc_enforcement",
        label="CFTC Enforcement",
        url="https://www.cftc.gov/RSS/RSSENF/rssenf.xml",
        market_bucket="macro",
        base_impact=81,
    ),
    NewsSource(
        key="coindesk",
        label="CoinDesk",
        url="https://www.coindesk.com/arc/outboundfeeds/rss/",
        market_bucket="crypto",
        base_impact=64,
    ),
    NewsSource(
        key="bbc_world",
        label="BBC World",
        url="https://feeds.bbci.co.uk/news/world/rss.xml",
        market_bucket="macro",
        base_impact=72,
    ),
    NewsSource(
        key="bbc_business",
        label="BBC Business",
        url="https://feeds.bbci.co.uk/news/business/rss.xml",
        market_bucket="stock",
        base_impact=66,
    ),
    NewsSource(
        key="nasdaq_markets",
        label="Nasdaq Markets",
        url="https://www.nasdaq.com/feed/rssoutbound?category=Markets",
        market_bucket="stock",
        base_impact=58,
    ),
    NewsSource(
        key="nasdaq_earnings",
        label="Nasdaq Earnings",
        url="https://www.nasdaq.com/feed/rssoutbound?category=Earnings",
        market_bucket="stock",
        base_impact=66,
    ),
    NewsSource(
        key="nasdaq_etfs",
        label="Nasdaq ETFs",
        url="https://www.nasdaq.com/feed/rssoutbound?category=ETFs",
        market_bucket="stock",
        base_impact=60,
    ),
    NewsSource(
        key="nasdaq_stocks",
        label="Nasdaq Stocks",
        url="https://www.nasdaq.com/feed/rssoutbound?category=Stocks",
        market_bucket="stock",
        base_impact=62,
    ),
    NewsSource(
        key="nasdaq_commodities",
        label="Nasdaq Commodities",
        url="https://www.nasdaq.com/feed/rssoutbound?category=Commodities",
        market_bucket="macro",
        base_impact=60,
    ),
    NewsSource(
        key="nasdaq_crypto",
        label="Nasdaq Crypto",
        url="https://www.nasdaq.com/feed/rssoutbound?category=Cryptocurrencies",
        market_bucket="crypto",
        base_impact=58,
    ),
)

_POSITIVE_TERMS = {
    "approval", "approves", "approved", "beat", "beats", "surge", "surges", "launch",
    "launches", "partner", "partnership", "record", "growth", "gains", "gain",
    "reclaim", "reclaims", "bullish", "inflow", "inflows", "adoption", "strong",
    "upgrade", "upgrades", "outperform", "expands", "expand", "wins",
    "buy rating", "overweight", "raises target", "price target raised", "raises price target",
}
_NEGATIVE_TERMS = {
    "lawsuit", "charges", "charged", "hack", "exploit", "breach", "cuts", "cut",
    "miss", "misses", "downgrade", "downgrades", "outflow", "outflows", "bankruptcy",
    "liquidation", "fraud", "penalty", "penalties", "decline", "falls", "fall",
    "slump", "slumps", "warning", "warns", "weak", "investigation", "probe",
    "sell rating", "underweight", "underperform", "cuts target", "price target cut", "lowers price target",
}
_HIGH_IMPACT_TERMS = {
    "fomc", "fed", "interest rate", "interest rates", "inflation", "cpi", "payrolls",
    "jobs report", "tariff", "treasury", "etf", "earnings", "guidance", "hack",
    "exploit", "lawsuit", "charges", "stablecoin", "listing", "bankruptcy",
    "war", "missile", "ceasefire", "sanctions", "analyst", "price target",
}
_GEOPOLITICS_STRONG_TERMS = {
    "war", "missile", "airstrike", "troops", "military", "ceasefire", "shipping lane",
    "red sea", "strait", "oil supply", "drone strike", "invasion",
}
_GEOPOLITICS_CONTEXT_TERMS = {
    "oil", "shipping", "energy", "crude", "tankers", "gaza", "iran", "israel",
    "ukraine", "russia", "china", "taiwan", "middle east", "strait", "red sea",
    "navy", "defense",
}
_CATEGORY_KEYWORDS = {
    "geopolitics": {
        *_GEOPOLITICS_STRONG_TERMS,
        "sanctions",
    },
    "macro": {"fed", "fomc", "inflation", "cpi", "pce", "jobs", "payrolls", "treasury", "tariff", "yield", "rate"},
    "regulation": {"sec", "cftc", "regulator", "regulatory", "compliance", "lawsuit", "charges", "enforcement", "policy"},
    "analyst": {
        "analyst", "price target", "raises target", "cuts target", "initiates", "reiterates",
        "upgrade", "downgrade", "overweight", "underweight", "buy rating", "sell rating",
        "outperform", "underperform",
    },
    "earnings": {"earnings", "guidance", "revenue", "profit", "quarter", "shareholder letter"},
    "company": {
        "ceo", "cfo", "executive", "layoff", "layoffs", "acquisition", "merger",
        "deal", "buyback", "dividend", "product launch", "partnership", "factory",
        "antitrust", "recall", "restructuring", "stake sale",
    },
    "crypto": {"bitcoin", "btc", "ethereum", "eth", "solana", "sol", "stablecoin", "token", "crypto", "exchange", "wallet", "coinbase"},
    "security": {"hack", "exploit", "breach", "stolen", "attack", "drain"},
}
_CATEGORY_PRIORITY = ("geopolitics", "macro", "regulation", "analyst", "earnings", "company", "security", "crypto")
_BROAD_FILTERS = {"all", "macro", "stock", "crypto"}
_CATEGORY_FILTERS = {"geopolitics", "regulation", "analyst", "earnings", "company", "security"}

_ALIAS_MAP = {
    "SPY": ("spy", "s&p 500", "s and p 500", "us equities", "u.s. equities"),
    "QQQ": ("qqq", "nasdaq 100", "nasdaq"),
    "IWM": ("iwm", "russell 2000", "small caps", "small-cap"),
    "DIA": ("dia", "dow jones", "dow"),
    "AAPL": ("aapl", "apple", "iphone"),
    "MSFT": ("msft", "microsoft", "azure"),
    "NVDA": ("nvda", "nvidia", "gpu", "ai chips"),
    "AMZN": ("amzn", "amazon", "aws"),
    "META": ("meta", "facebook", "instagram"),
    "TSLA": ("tsla", "tesla"),
    "BTC-USD": ("btc-usd", "bitcoin", "btc"),
    "ETH-USD": ("eth-usd", "ethereum", "ether", "eth"),
    "SOL-USD": ("sol-usd", "solana", "sol"),
}


class NewsService:
    def __init__(
        self,
        tracked_symbols: list[str],
        refresh_interval_s: int = 90,
        sources: tuple[NewsSource, ...] = _DEFAULT_SOURCES,
    ) -> None:
        self._tracked_symbols = list(dict.fromkeys(symbol.upper() for symbol in tracked_symbols))
        self._refresh_interval = timedelta(seconds=max(15, refresh_interval_s))
        self._sources = sources
        self._cached_items: list[dict] = []
        self._last_refreshed_at: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def list_news(
        self,
        symbol: Optional[str] = None,
        market_bucket: str = "all",
        impact: str = "all",
        limit: int = 40,
    ) -> dict:
        await self.refresh()

        items = self._cached_items
        if symbol:
            needle = symbol.upper()
            items = [item for item in items if needle in item.get("affected_symbols", [])]
        if market_bucket != "all":
            if market_bucket in _BROAD_FILTERS:
                items = [item for item in items if item.get("market_bucket") == market_bucket]
            else:
                items = [item for item in items if item.get("category") == market_bucket]
        if impact != "all":
            items = [item for item in items if item.get("impact_level") == impact]

        return {
            "news": items[:limit],
            "brief": self._build_market_brief(items, symbol=symbol, market_bucket=market_bucket, impact=impact),
            "last_refreshed_at": self._last_refreshed_at.isoformat() if self._last_refreshed_at else None,
            "sources": [
                {
                    "key": source.key,
                    "label": source.label,
                    "market_bucket": source.market_bucket,
                    "domain": urlparse(source.url).netloc,
                    "url": source.url,
                }
                for source in self._sources
            ],
        }

    def _build_market_brief(
        self,
        items: list[dict],
        symbol: Optional[str],
        market_bucket: str,
        impact: str,
    ) -> dict:
        scoped = items[:15]
        context_bits = []
        if market_bucket != "all":
            context_bits.append(market_bucket.title())
        else:
            context_bits.append("Cross-Market")
        if symbol:
            context_bits.append(symbol)
        if impact != "all":
            context_bits.append(f"{impact.title()} Impact")
        scope_label = " | ".join(context_bits)

        if not scoped:
            return {
                "scope_label": scope_label,
                "statement": "Conditions are neutral because no headlines matched the current scope yet.",
                "summary": "The desk is waiting on fresher catalyst flow before leaning risk-on or risk-off.",
                "watch_next": "Watch for a fresh macro, regulation, or crypto headline to reset the tape.",
                "confidence": {"label": "Low", "score": 24, "tone": "neutral"},
                "conditions": [
                    {
                        "label": "Risk Tone",
                        "value": "Neutral",
                        "tone": "neutral",
                        "detail": "No filtered catalyst flow yet.",
                    },
                    {
                        "label": "Catalyst Pressure",
                        "value": "Quiet",
                        "tone": "neutral",
                        "detail": "Nothing strong enough to tilt the desk read.",
                    },
                    {
                        "label": "Event Risk",
                        "value": "Contained",
                        "tone": "positive",
                        "detail": "No major macro or security driver is dominating the feed.",
                    },
                    {
                        "label": "Leadership",
                        "value": "Unclear",
                        "tone": "neutral",
                        "detail": "There is not enough signal to call a leadership bucket.",
                    },
                ],
                "drivers": [],
                "narratives": [],
                "what_changed": [
                    "No filtered narrative cluster is active yet.",
                    "Desk conditions stay neutral until a stronger catalyst arrives.",
                ],
            }

        bucket_stats = {
            "macro": {"score": 0.0, "weight": 0.0, "count": 0},
            "stock": {"score": 0.0, "weight": 0.0, "count": 0},
            "crypto": {"score": 0.0, "weight": 0.0, "count": 0},
        }
        positive_weight = 0.0
        negative_weight = 0.0
        mixed_weight = 0.0
        high_impact_count = 0
        event_risk_score = 0.0
        volatility_score = 0.0

        for item in scoped:
            weight = self._brief_weight(item)
            direction = self._direction_score(item.get("bias"))
            bucket = item.get("market_bucket", "macro")
            if bucket in bucket_stats:
                bucket_stats[bucket]["score"] += direction * weight
                bucket_stats[bucket]["weight"] += weight
                bucket_stats[bucket]["count"] += 1

            if direction > 0:
                positive_weight += weight
            elif direction < 0:
                negative_weight += weight
            else:
                mixed_weight += weight

            if item.get("impact_level") == "high":
                high_impact_count += 1
            if item.get("category") in {"macro", "regulation"}:
                event_risk_score += weight * 0.8
            if item.get("category") == "security":
                event_risk_score += weight * 1.15
                volatility_score += weight * 1.25
            elif item.get("impact_level") == "high":
                volatility_score += weight * 0.65

        total_directional = positive_weight + negative_weight
        net_bias = 0.0 if total_directional == 0 else ((positive_weight - negative_weight) / total_directional) * 100
        risk_tone = self._risk_tone(net_bias, positive_weight, negative_weight)
        pressure = self._pressure_label(high_impact_count, scoped)
        event_risk = self._event_risk_label(event_risk_score, scoped)
        leadership = self._leadership(bucket_stats)
        confidence = self._confidence_label(net_bias, bucket_stats, scoped)
        narratives = self._build_narratives(scoped)

        macro_bias = self._bucket_bias_line("macro", bucket_stats)
        stock_bias = self._bucket_bias_line("stock", bucket_stats)
        crypto_bias = self._bucket_bias_line("crypto", bucket_stats)

        statement = (
            f"Conditions are {risk_tone['value'].lower()} with {leadership['value'].lower()} leadership, "
            f"while catalyst pressure is {pressure['value'].lower()}."
        )
        summary = (
            f"Macro reads {macro_bias}, stocks read {stock_bias}, and crypto reads {crypto_bias}. "
            f"The feed is being driven by {high_impact_count} high-impact headline"
            f"{'' if high_impact_count == 1 else 's'}, so follow-through matters more than the first spike."
        )
        watch_next = self._brief_watch_next(scoped, leadership["value"], risk_tone["value"])
        what_changed = self._what_changed(
            risk_tone=risk_tone,
            leadership=leadership,
            pressure=pressure,
            event_risk=event_risk,
            narratives=narratives,
        )

        drivers = [
            {
                "id": item["id"],
                "title": item["title"],
                "impact_level": item["impact_level"],
                "bias": item["bias"],
                "source_label": item["source_label"],
                "market_bucket": item["market_bucket"],
            }
            for item in scoped[:3]
        ]

        return {
            "scope_label": scope_label,
            "statement": statement,
            "summary": summary,
            "watch_next": watch_next,
            "confidence": confidence,
            "conditions": [
                {
                    "label": "Risk Tone",
                    "value": risk_tone["value"],
                    "tone": risk_tone["tone"],
                    "detail": risk_tone["detail"],
                },
                {
                    "label": "Catalyst Pressure",
                    "value": pressure["value"],
                    "tone": pressure["tone"],
                    "detail": pressure["detail"],
                },
                {
                    "label": "Event Risk",
                    "value": event_risk["value"],
                    "tone": event_risk["tone"],
                    "detail": event_risk["detail"],
                },
                {
                    "label": "Leadership",
                    "value": leadership["value"],
                    "tone": leadership["tone"],
                    "detail": leadership["detail"],
                },
            ],
            "drivers": drivers,
            "narratives": narratives,
            "what_changed": what_changed,
        }

    async def refresh(self, force: bool = False) -> None:
        if not force and self._cached_items and self._last_refreshed_at:
            if datetime.now(timezone.utc) - self._last_refreshed_at < self._refresh_interval:
                return

        async with self._lock:
            if not force and self._cached_items and self._last_refreshed_at:
                if datetime.now(timezone.utc) - self._last_refreshed_at < self._refresh_interval:
                    return

            fetched = await self._fetch_all_sources()
            if fetched:
                self._cached_items = fetched
                self._last_refreshed_at = datetime.now(timezone.utc)

    async def _fetch_all_sources(self) -> list[dict]:
        async with httpx.AsyncClient(
            timeout=10.0,
            follow_redirects=True,
            headers={"User-Agent": "Meridian Market Desk/0.3"},
        ) as client:
            results = await asyncio.gather(
                *(self._fetch_source(client, source) for source in self._sources),
                return_exceptions=True,
            )

        items: list[dict] = []
        seen_ids: set[str] = set()
        for result in results:
            if isinstance(result, Exception):
                logger.warning("News source refresh failed: %s", result)
                continue
            for item in result:
                if item["id"] in seen_ids:
                    continue
                seen_ids.add(item["id"])
                items.append(item)

        items.sort(
            key=lambda item: (
                item.get("impact_score", 0),
                item.get("published_at") or "",
            ),
            reverse=True,
        )
        return items[:120]

    async def _fetch_source(self, client: httpx.AsyncClient, source: NewsSource) -> list[dict]:
        response = await client.get(source.url)
        response.raise_for_status()
        parsed = self._parse_feed(source, response.text)
        return [self._enrich_item(source, item) for item in parsed]

    def _parse_feed(self, source: NewsSource, xml_text: str) -> list[dict]:
        root = ET.fromstring(xml_text)
        items: list[dict] = []

        if root.tag.endswith("feed"):
            entries = root.findall("{*}entry")
            for entry in entries[:24]:
                link = ""
                link_node = entry.find("{*}link")
                if link_node is not None:
                    link = link_node.attrib.get("href", "")
                items.append(
                    {
                        "title": self._node_text(entry.find("{*}title")),
                        "summary": self._node_text(entry.find("{*}summary")) or self._node_text(entry.find("{*}content")),
                        "url": link,
                        "published_at": self._node_text(entry.find("{*}updated")) or self._node_text(entry.find("{*}published")),
                    }
                )
            return [item for item in items if item["title"] and item["url"]]

        channel = root.find("channel")
        if channel is None:
            channel = root.find("{*}channel")
        if channel is None:
            return []

        for item in channel.findall("item")[:24] + channel.findall("{*}item")[:24]:
            items.append(
                {
                    "title": self._node_text(item.find("title")) or self._node_text(item.find("{*}title")),
                    "summary": self._node_text(item.find("description")) or self._node_text(item.find("{*}description")),
                    "url": self._node_text(item.find("link")) or self._node_text(item.find("{*}link")),
                    "published_at": (
                        self._node_text(item.find("pubDate"))
                        or self._node_text(item.find("{*}pubDate"))
                        or self._node_text(item.find("{*}date"))
                    ),
                }
            )
        deduped: list[dict] = []
        seen_links: set[str] = set()
        for item in items:
            if not item["title"] or not item["url"] or item["url"] in seen_links:
                continue
            seen_links.add(item["url"])
            deduped.append(item)
        return deduped[:24]

    def _enrich_item(self, source: NewsSource, item: dict) -> dict:
        published_at = self._parse_datetime(item.get("published_at"))
        title = self._clean_text(item.get("title"))
        summary = self._clean_text(item.get("summary"))
        text = f"{title} {summary}".lower()

        category = self._detect_category(text)
        affected_symbols = self._detect_symbols(text, category)
        market_bucket = self._resolve_market_bucket(category, affected_symbols, source.market_bucket)
        bias = self._detect_bias(text, category)
        horizon = self._detect_horizon(category, text)
        impact_score = self._impact_score(source.base_impact, text, category, published_at, affected_symbols)
        impact_level = self._impact_level(impact_score)
        why_it_matters, watch_next = self._impact_explanation(category, bias, market_bucket, affected_symbols)
        tags = self._build_tags(category, bias, horizon, affected_symbols)

        stable_id = hashlib.sha1(
            f"{source.key}|{item.get('url')}|{title}|{published_at.isoformat() if published_at else ''}".encode("utf-8")
        ).hexdigest()[:16]

        return {
            "id": stable_id,
            "source_key": source.key,
            "source_label": source.label,
            "market_bucket": market_bucket,
            "category": category,
            "title": title,
            "summary": summary,
            "url": item.get("url"),
            "published_at": published_at.isoformat() if published_at else None,
            "impact_score": impact_score,
            "impact_level": impact_level,
            "bias": bias,
            "horizon": horizon,
            "affected_symbols": affected_symbols,
            "why_it_matters": why_it_matters,
            "watch_next": watch_next,
            "tags": tags,
        }

    @staticmethod
    def _node_text(node) -> str:
        if node is None:
            return ""
        return "".join(node.itertext()).strip()

    @staticmethod
    def _clean_text(value: Optional[str]) -> str:
        if not value:
            return ""
        cleaned = html.unescape(_TAG_RE.sub(" ", value))
        return _WHITESPACE_RE.sub(" ", cleaned).strip()

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _detect_category(self, text: str) -> str:
        for category in _CATEGORY_PRIORITY:
            if category == "geopolitics":
                if self._is_geopolitics_text(text):
                    return category
                continue
            if any(_contains_term(text, keyword) for keyword in _CATEGORY_KEYWORDS[category]):
                return category
        return "market"

    def _detect_symbols(self, text: str, category: str) -> list[str]:
        symbols: list[str] = []
        for symbol in self._tracked_symbols:
            aliases = _ALIAS_MAP.get(symbol, (symbol.lower(),))
            if any(_contains_term(text, alias) for alias in aliases):
                symbols.append(symbol)

        if symbols:
            return symbols

        if category == "macro":
            return [symbol for symbol in self._tracked_symbols if symbol in {"SPY", "QQQ", "IWM", "DIA", "BTC-USD", "ETH-USD"}]
        if category == "geopolitics":
            return [symbol for symbol in self._tracked_symbols if symbol in {"SPY", "QQQ", "DIA", "BTC-USD", "ETH-USD"}]
        if category in {"crypto", "security"}:
            return [symbol for symbol in self._tracked_symbols if symbol in {"BTC-USD", "ETH-USD", "SOL-USD"}]
        if category == "regulation":
            return [symbol for symbol in self._tracked_symbols if symbol in {"BTC-USD", "ETH-USD", "SOL-USD", "SPY", "QQQ"}]
        if category in {"earnings", "analyst", "company"}:
            return [symbol for symbol in self._tracked_symbols if symbol in {"QQQ", "SPY"}]
        return [symbol for symbol in self._tracked_symbols if symbol in {"SPY", "QQQ", "BTC-USD"}]

    @staticmethod
    def _resolve_market_bucket(category: str, affected_symbols: list[str], default_bucket: str) -> str:
        has_stock = any("-" not in symbol and "/" not in symbol for symbol in affected_symbols)
        has_crypto = any("-" in symbol or "/" in symbol for symbol in affected_symbols)
        if category in {"macro", "geopolitics"}:
            return "macro"
        if has_stock and has_crypto:
            return "macro"
        if has_crypto:
            return "crypto"
        if has_stock:
            return "stock"
        return default_bucket

    @staticmethod
    def _detect_bias(text: str, category: str) -> str:
        positive_hits = sum(1 for term in _POSITIVE_TERMS if _contains_term(text, term))
        negative_hits = sum(1 for term in _NEGATIVE_TERMS if _contains_term(text, term))

        if category == "macro":
            if _contains_term(text, "cooling inflation") or _contains_term(text, "rate cuts") or _contains_term(text, "eases"):
                return "bullish"
            if _contains_term(text, "hot inflation") or _contains_term(text, "higher for longer") or _contains_term(text, "tariff"):
                return "bearish"
        if positive_hits >= negative_hits + 2:
            return "bullish"
        if negative_hits >= positive_hits + 2:
            return "bearish"
        if positive_hits or negative_hits:
            return "mixed"
        return "unclear"

    @staticmethod
    def _detect_horizon(category: str, text: str) -> str:
        if category in {"macro", "geopolitics"}:
            return "Regime"
        if category in {"earnings", "regulation", "analyst", "company"}:
            return "Swing"
        if category == "security" or _contains_term(text, "breaking"):
            return "Intraday"
        return "Intraday"

    @staticmethod
    def _impact_score(
        base_impact: int,
        text: str,
        category: str,
        published_at: Optional[datetime],
        affected_symbols: list[str],
    ) -> int:
        score = base_impact
        score += sum(4 for keyword in _HIGH_IMPACT_TERMS if _contains_term(text, keyword))
        if category in {"macro", "geopolitics"}:
            score += 10
        elif category in {"regulation", "security"}:
            score += 8
        elif category in {"earnings", "analyst"}:
            score += 6
        elif category == "company":
            score += 4
        if len(affected_symbols) >= 4:
            score += 4

        if published_at:
            age = datetime.now(timezone.utc) - published_at
            if age <= timedelta(hours=2):
                score += 8
            elif age <= timedelta(hours=8):
                score += 5
            elif age <= timedelta(days=1):
                score += 2
        return max(35, min(99, score))

    @staticmethod
    def _impact_level(score: int) -> str:
        if score >= 86:
            return "high"
        if score >= 68:
            return "medium"
        return "low"

    @staticmethod
    def _impact_explanation(
        category: str,
        bias: str,
        market_bucket: str,
        affected_symbols: list[str],
    ) -> tuple[str, str]:
        symbol_line = ", ".join(affected_symbols[:5]) if affected_symbols else "the risk tape"
        if category == "macro":
            return (
                "Could reprice rate expectations and broad risk appetite across indexes and crypto.",
                f"Watch {symbol_line}, treasury yields, and whether the move sticks beyond the first reaction.",
            )
        if category == "geopolitics":
            return (
                "War, sanctions, or shipping headlines can hit oil, rates, defense, and broad risk appetite all at once.",
                f"Watch {symbol_line}, energy-sensitive names, and whether the move spills into the broader tape.",
            )
        if category == "regulation":
            return (
                "Could change policy or enforcement expectations, which usually matters more than a single headline candle.",
                f"Watch {symbol_line} for follow-through once the market digests the legal or policy angle.",
            )
        if category == "analyst":
            return (
                "Analyst actions can move individual names quickly, especially when a rating change lines up with existing price structure.",
                f"Watch {symbol_line} for acceptance beyond the first gap or price-target headline reaction.",
            )
        if category == "company":
            return (
                "Company-specific events can reset the near-term narrative even when the broader market is quiet.",
                f"Watch {symbol_line} for whether the update changes leadership, guidance expectations, or sector sympathy.",
            )
        if category == "security":
            return (
                "Security incidents can hit confidence, liquidity, and short-term positioning faster than slower fundamental news.",
                f"Watch {symbol_line} and whether spreads, volume, or liquidation-style moves start to widen.",
            )
        if category == "earnings":
            return (
                "Company-specific updates can reset expectations quickly and pull the sector or index leadership around with them.",
                f"Watch {symbol_line} for gap follow-through, reversals, and whether guidance changes the bigger trend.",
            )
        if market_bucket == "crypto":
            return (
                "Crypto headlines often hit sentiment first and structure second, so context matters more than the first spike.",
                f"Watch {symbol_line} for acceptance above key levels instead of chasing the initial move.",
            )
        direction = {
            "bullish": "Could support upside continuation if price confirms.",
            "bearish": "Could pressure the tape if the market agrees with the headline.",
            "mixed": "Could create a two-way reaction until the market picks a side.",
            "unclear": "Worth watching, but the directional read still needs confirmation from price.",
        }[bias]
        return direction, f"Watch {symbol_line} and whether volume confirms the first move."

    @staticmethod
    def _build_tags(category: str, bias: str, horizon: str, affected_symbols: list[str]) -> list[str]:
        tags = [category.title(), bias.title(), horizon]
        tags.extend(affected_symbols[:3])
        return tags

    @staticmethod
    def _brief_weight(item: dict) -> float:
        weight = float(item.get("impact_score", 60))
        published_at = item.get("published_at")
        if not published_at:
            return weight
        try:
            published = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        except ValueError:
            return weight
        age = datetime.now(timezone.utc) - published.astimezone(timezone.utc)
        if age <= timedelta(hours=2):
            return weight * 1.15
        if age <= timedelta(hours=8):
            return weight * 1.05
        if age >= timedelta(days=2):
            return weight * 0.82
        return weight

    @staticmethod
    def _direction_score(bias: Optional[str]) -> int:
        if bias == "bullish":
            return 1
        if bias == "bearish":
            return -1
        return 0

    @staticmethod
    def _risk_tone(net_bias: float, positive_weight: float, negative_weight: float) -> dict:
        if positive_weight and negative_weight and abs(net_bias) < 16:
            return {
                "value": "Mixed",
                "tone": "caution",
                "detail": "Bullish and bearish catalysts are both active, so the tape is likely to stay selective.",
            }
        if net_bias >= 55:
            return {
                "value": "Risk-On",
                "tone": "positive",
                "detail": "Positive catalysts are clearly outweighing negative ones across the active feed.",
            }
        if net_bias >= 18:
            return {
                "value": "Constructive",
                "tone": "positive",
                "detail": "The desk lean is positive, but it still needs price follow-through to broaden.",
            }
        if net_bias <= -55:
            return {
                "value": "Risk-Off",
                "tone": "negative",
                "detail": "Negative catalysts are dominating the flow and usually deserve more defensive positioning.",
            }
        if net_bias <= -18:
            return {
                "value": "Defensive",
                "tone": "negative",
                "detail": "Headline pressure is leaning bearish even if the broader tape has not fully broken yet.",
            }
        return {
            "value": "Balanced",
            "tone": "neutral",
            "detail": "Catalyst flow is not strong enough in one direction to justify a heavy directional lean.",
        }

    @staticmethod
    def _pressure_label(high_impact_count: int, items: list[dict]) -> dict:
        avg_impact = sum(item.get("impact_score", 0) for item in items[:8]) / max(1, min(len(items), 8))
        if high_impact_count >= 4 or avg_impact >= 84:
            return {
                "value": "Elevated",
                "tone": "caution",
                "detail": "There are enough high-impact catalysts in the feed to keep reaction risk elevated.",
            }
        if high_impact_count >= 2 or avg_impact >= 72:
            return {
                "value": "Active",
                "tone": "info",
                "detail": "Catalyst flow is busy enough to matter, but not every headline should reset the whole tape.",
            }
        return {
            "value": "Light",
            "tone": "positive",
            "detail": "Catalyst flow is relatively quiet, so price structure can matter more than headline volatility.",
        }

    @staticmethod
    def _event_risk_label(event_risk_score: float, items: list[dict]) -> dict:
        normalized = event_risk_score / max(len(items), 1)
        if normalized >= 58:
            return {
                "value": "High",
                "tone": "negative",
                "detail": "Macro, regulation, or security catalysts are heavy enough that sharp repricing remains possible.",
            }
        if normalized >= 34:
            return {
                "value": "Elevated",
                "tone": "caution",
                "detail": "The feed has enough policy or event pressure that breakouts need cleaner confirmation.",
            }
        return {
            "value": "Contained",
            "tone": "positive",
            "detail": "The current feed is not being dominated by the kind of catalysts that usually destabilize the whole tape.",
        }

    @staticmethod
    def _leadership(bucket_stats: dict) -> dict:
        active = {
            bucket: stats for bucket, stats in bucket_stats.items()
            if stats["count"] and stats["weight"] > 0
        }
        if not active:
            return {
                "value": "Broad Tape",
                "tone": "neutral",
                "detail": "No single market bucket is clearly dominating the catalyst flow.",
            }

        leader_key, leader_stats = max(
            active.items(),
            key=lambda item: abs(item[1]["score"] / max(item[1]["weight"], 1.0)),
        )
        leader_net = leader_stats["score"] / max(leader_stats["weight"], 1.0)
        label = {
            "macro": "Macro",
            "stock": "Equities",
            "crypto": "Crypto",
        }[leader_key]
        if leader_net >= 0.22:
            detail = f"{label} headlines are leaning positive and shaping the current desk read."
            tone = "positive"
        elif leader_net <= -0.22:
            detail = f"{label} headlines are leaning negative and carrying the heaviest pressure."
            tone = "negative"
        else:
            detail = f"{label} is leading the flow, but the directional read is still mixed."
            tone = "info"
        return {"value": label, "tone": tone, "detail": detail}

    @staticmethod
    def _confidence_label(net_bias: float, bucket_stats: dict, items: list[dict]) -> dict:
        active_scores = [
            abs(stats["score"] / max(stats["weight"], 1.0))
            for stats in bucket_stats.values()
            if stats["count"] and stats["weight"] > 0
        ]
        alignment = sum(active_scores) / max(len(active_scores), 1)
        score = int(max(24, min(92, abs(net_bias) * 0.7 + alignment * 42 + len(items[:5]) * 2)))
        if score >= 76:
            return {"label": "High", "score": score, "tone": "positive"}
        if score >= 52:
            return {"label": "Medium", "score": score, "tone": "info"}
        return {"label": "Low", "score": score, "tone": "neutral"}

    @staticmethod
    def _bucket_bias_line(bucket: str, bucket_stats: dict) -> str:
        stats = bucket_stats[bucket]
        if not stats["count"] or stats["weight"] <= 0:
            return "quiet"
        net = stats["score"] / stats["weight"]
        if net >= 0.28:
            return "constructive"
        if net >= 0.08:
            return "slightly constructive"
        if net <= -0.28:
            return "defensive"
        if net <= -0.08:
            return "slightly defensive"
        return "mixed"

    def _build_narratives(self, items: list[dict]) -> list[dict]:
        clusters: dict[str, dict] = {}
        for item in items:
            label = self._narrative_label(item)
            key = f"{item.get('market_bucket', 'macro')}|{label}"
            cluster = clusters.setdefault(
                key,
                {
                    "label": label,
                    "market_bucket": item.get("market_bucket", "macro"),
                    "items": [],
                    "weight": 0.0,
                    "score": 0.0,
                    "symbols": [],
                },
            )
            cluster["items"].append(item)
            cluster["weight"] += self._brief_weight(item)
            cluster["score"] += self._direction_score(item.get("bias")) * self._brief_weight(item)
            for symbol in item.get("affected_symbols", []):
                if symbol not in cluster["symbols"]:
                    cluster["symbols"].append(symbol)

        narratives = []
        for cluster in clusters.values():
            ordered = sorted(
                cluster["items"],
                key=lambda item: (item.get("impact_score", 0), item.get("published_at") or ""),
                reverse=True,
            )
            primary = ordered[0]
            net = cluster["score"] / max(cluster["weight"], 1.0)
            if net >= 0.18:
                tone = "positive"
            elif net <= -0.18:
                tone = "negative"
            elif primary.get("category") in {"security", "regulation"}:
                tone = "caution"
            else:
                tone = "info"

            symbols = ", ".join(cluster["symbols"][:4]) if cluster["symbols"] else "the broad tape"
            detail = (
                f"{len(cluster['items'])} related headline"
                f"{'' if len(cluster['items']) == 1 else 's'} are active across {symbols}. "
                f"{primary.get('why_it_matters')}"
            )
            narratives.append(
                {
                    "id": primary["id"],
                    "label": cluster["label"],
                    "market_bucket": cluster["market_bucket"],
                    "count": len(cluster["items"]),
                    "impact_level": primary.get("impact_level", "medium"),
                    "tone": tone,
                    "detail": detail,
                }
            )

        narratives.sort(
            key=lambda item: (
                {"high": 3, "medium": 2, "low": 1}.get(item["impact_level"], 1),
                item["count"],
            ),
            reverse=True,
        )
        return narratives[:4]

    @staticmethod
    def _narrative_label(item: dict) -> str:
        text = f"{item.get('title', '')} {item.get('summary', '')}".lower()
        category = item.get("category")
        market_bucket = item.get("market_bucket")
        if category == "geopolitics" or NewsService._is_geopolitics_text(text):
            return "War / Geopolitics"
        if any(_contains_term(text, keyword) for keyword in {"fed", "fomc", "rate", "inflation", "cpi", "pce", "yield"}):
            return "Fed / Rates"
        if any(_contains_term(text, keyword) for keyword in {"jobs", "payrolls", "tariff", "treasury"}):
            return "Macro / Economic Data"
        if any(_contains_term(text, keyword) for keyword in {"sec", "cftc", "enforcement", "lawsuit", "charges", "rule"}):
            return "Regulation / Enforcement"
        if category == "analyst" or any(_contains_term(text, keyword) for keyword in {"analyst", "price target", "upgrade", "downgrade", "overweight", "underweight"}):
            return "Analyst Actions"
        if any(_contains_term(text, keyword) for keyword in {"earnings", "guidance", "revenue", "profit"}):
            return "Earnings / Guidance"
        if category == "company" or any(_contains_term(text, keyword) for keyword in {"ceo", "cfo", "acquisition", "merger", "layoff", "buyback", "recall"}):
            return "Company Events"
        if any(_contains_term(text, keyword) for keyword in {"etf", "inflow", "outflow"}):
            return "ETF / Fund Flows"
        if category == "security" or any(_contains_term(text, keyword) for keyword in {"hack", "exploit", "breach"}):
            return "Security / Exploit"
        if market_bucket == "crypto" and any(_contains_term(text, keyword) for keyword in {"stablecoin", "listing", "adoption", "wallet", "exchange"}):
            return "Crypto Adoption / Structure"
        if market_bucket == "stock":
            return "Equity Market Commentary"
        if market_bucket == "crypto":
            return "Crypto Market Commentary"
        return "Broad Market Commentary"

    @staticmethod
    def _is_geopolitics_text(text: str) -> bool:
        if any(_contains_term(text, keyword) for keyword in _GEOPOLITICS_STRONG_TERMS):
            return True
        if _contains_term(text, "sanctions") and any(_contains_term(text, keyword) for keyword in _GEOPOLITICS_CONTEXT_TERMS):
            return True
        return False

    @staticmethod
    def _what_changed(
        risk_tone: dict,
        leadership: dict,
        pressure: dict,
        event_risk: dict,
        narratives: list[dict],
    ) -> list[str]:
        changes = [
            f"{leadership['value']} is leading the current catalyst flow and the desk tone reads {risk_tone['value'].lower()}.",
        ]
        if narratives:
            changes.append(
                f"{narratives[0]['label']} is the strongest active narrative with "
                f"{narratives[0]['count']} related headline"
                f"{'' if narratives[0]['count'] == 1 else 's'}."
            )
        if event_risk["value"] in {"High", "Elevated"}:
            changes.append(
                f"Event risk is {event_risk['value'].lower()}, so first reactions should be treated as provisional until they hold."
            )
        else:
            changes.append(
                f"Catalyst pressure is {pressure['value'].lower()}, so price structure can matter more than every single headline tick."
            )
        return changes

    @staticmethod
    def _brief_watch_next(items: list[dict], leadership_label: str, risk_tone_label: str) -> str:
        first_symbols = []
        for item in items[:5]:
            for symbol in item.get("affected_symbols", []):
                if symbol not in first_symbols:
                    first_symbols.append(symbol)
                if len(first_symbols) >= 4:
                    break
            if len(first_symbols) >= 4:
                break

        symbol_line = ", ".join(first_symbols) if first_symbols else "the broad tape"
        tone_line = risk_tone_label.lower()
        return (
            f"Watch {symbol_line} for follow-through, especially if {leadership_label.lower()} catalysts keep the market "
            f"{tone_line}. If the first move fails quickly, treat the headline as information rather than confirmation."
        )


def _contains_term(text: str, term: str) -> bool:
    pattern = re.escape(term.lower()).replace(r"\ ", r"\s+")
    return bool(re.search(rf"(?<![a-z0-9]){pattern}(?![a-z0-9])", text))
