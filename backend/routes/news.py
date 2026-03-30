from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend.services.news_service import NewsService

router = APIRouter(prefix="/api", tags=["news"])

_news_service: Optional[NewsService] = None


def set_news_service(service: NewsService) -> None:
    global _news_service
    _news_service = service


def get_news_service() -> NewsService:
    if _news_service is None:
        raise HTTPException(status_code=503, detail="News service not ready")
    return _news_service


@router.get("/news")
async def list_news(
    symbol: Optional[str] = Query(default=None),
    market_bucket: str = Query(default="all", description="all | macro | stock | crypto"),
    impact: str = Query(default="all", description="all | high | medium | low"),
    limit: int = Query(default=40, ge=1, le=100),
):
    if market_bucket not in {"all", "macro", "stock", "crypto"}:
        raise HTTPException(status_code=400, detail="market_bucket must be all, macro, stock, or crypto")
    if impact not in {"all", "high", "medium", "low"}:
        raise HTTPException(status_code=400, detail="impact must be all, high, medium, or low")

    service = get_news_service()
    return await service.list_news(
        symbol=symbol.strip().upper() if symbol else None,
        market_bucket=market_bucket,
        impact=impact,
        limit=limit,
    )
