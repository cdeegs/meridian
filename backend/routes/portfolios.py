from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.database import get_db

router = APIRouter(prefix="/api", tags=["portfolios"])

SUPPORTED_ASSET_TYPES = {
    "equity",
    "etf",
    "crypto",
    "option",
    "macro",
    "custom",
}


class PortfolioCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=80)
    strategy: Optional[str] = Field(default=None, max_length=280)
    notes: Optional[str] = Field(default=None, max_length=1000)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        return value.strip()

    @field_validator("strategy", "notes")
    @classmethod
    def normalize_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None


class PortfolioUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=80)
    strategy: Optional[str] = Field(default=None, max_length=280)
    notes: Optional[str] = Field(default=None, max_length=1000)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return value.strip()

    @field_validator("strategy", "notes")
    @classmethod
    def normalize_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None


class PortfolioAssetCreate(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=32)
    asset_type: str = Field(default="equity")
    allocation_pct: Optional[float] = Field(default=None, ge=0, le=100)
    strategy: Optional[str] = Field(default=None, max_length=280)
    notes: Optional[str] = Field(default=None, max_length=1000)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("asset_type")
    @classmethod
    def normalize_asset_type(cls, value: str) -> str:
        asset_type = value.strip().lower()
        if asset_type not in SUPPORTED_ASSET_TYPES:
            raise ValueError(f"Unsupported asset_type '{value}'")
        return asset_type

    @field_validator("strategy", "notes")
    @classmethod
    def normalize_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None


class PortfolioAssetUpdate(BaseModel):
    symbol: Optional[str] = Field(default=None, min_length=1, max_length=32)
    asset_type: Optional[str] = Field(default=None)
    allocation_pct: Optional[float] = Field(default=None, ge=0, le=100)
    strategy: Optional[str] = Field(default=None, max_length=280)
    notes: Optional[str] = Field(default=None, max_length=1000)

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return value.strip().upper()

    @field_validator("asset_type")
    @classmethod
    def normalize_asset_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        asset_type = value.strip().lower()
        if asset_type not in SUPPORTED_ASSET_TYPES:
            raise ValueError(f"Unsupported asset_type '{value}'")
        return asset_type

    @field_validator("strategy", "notes")
    @classmethod
    def normalize_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None


@router.get("/portfolios")
async def list_portfolios(db: AsyncSession = Depends(get_db)):
    portfolios = await _load_portfolios(db)
    return {"portfolios": portfolios}


@router.post("/portfolios", status_code=status.HTTP_201_CREATED)
async def create_portfolio(payload: PortfolioCreate, db: AsyncSession = Depends(get_db)):
    portfolio_id = str(uuid4())
    now = datetime.now(timezone.utc)
    await db.execute(
        text("""
            INSERT INTO portfolios (id, name, strategy, notes, created_at, updated_at)
            VALUES (:id, :name, :strategy, :notes, :created_at, :updated_at)
        """),
        {
            "id": portfolio_id,
            "name": payload.name,
            "strategy": payload.strategy,
            "notes": payload.notes,
            "created_at": now,
            "updated_at": now,
        },
    )
    await db.commit()

    return await _get_portfolio_or_404(db, portfolio_id)


@router.patch("/portfolios/{portfolio_id}")
async def update_portfolio(
    portfolio_id: str,
    payload: PortfolioUpdate,
    db: AsyncSession = Depends(get_db),
):
    existing = await _get_portfolio_or_404(db, portfolio_id)
    updates = {
        "name": payload.name if payload.name is not None else existing["name"],
        "strategy": payload.strategy if payload.strategy is not None else existing.get("strategy"),
        "notes": payload.notes if payload.notes is not None else existing.get("notes"),
        "updated_at": datetime.now(timezone.utc),
        "id": portfolio_id,
    }
    await db.execute(
        text("""
            UPDATE portfolios
            SET name = :name,
                strategy = :strategy,
                notes = :notes,
                updated_at = :updated_at
            WHERE id = :id
        """),
        updates,
    )
    await db.commit()

    return await _get_portfolio_or_404(db, portfolio_id)


@router.delete("/portfolios/{portfolio_id}")
async def delete_portfolio(portfolio_id: str, db: AsyncSession = Depends(get_db)):
    await _get_portfolio_or_404(db, portfolio_id)
    await db.execute(text("DELETE FROM portfolios WHERE id = :id"), {"id": portfolio_id})
    await db.commit()
    return {"status": "ok", "deleted_id": portfolio_id}


@router.post("/portfolios/{portfolio_id}/assets", status_code=status.HTTP_201_CREATED)
async def add_portfolio_asset(
    portfolio_id: str,
    payload: PortfolioAssetCreate,
    db: AsyncSession = Depends(get_db),
):
    await _get_portfolio_or_404(db, portfolio_id)
    asset_id = str(uuid4())

    await db.execute(
        text("""
            INSERT INTO portfolio_assets (
                id, portfolio_id, symbol, asset_type, allocation_pct, strategy, notes, created_at
            )
            VALUES (
                :id, :portfolio_id, :symbol, :asset_type, :allocation_pct, :strategy, :notes, :created_at
            )
        """),
        {
            "id": asset_id,
            "portfolio_id": portfolio_id,
            "symbol": payload.symbol,
            "asset_type": payload.asset_type,
            "allocation_pct": payload.allocation_pct,
            "strategy": payload.strategy,
            "notes": payload.notes,
            "created_at": datetime.now(timezone.utc),
        },
    )
    await db.execute(
        text("UPDATE portfolios SET updated_at = :updated_at WHERE id = :id"),
        {"updated_at": datetime.now(timezone.utc), "id": portfolio_id},
    )
    await db.commit()

    return await _get_portfolio_or_404(db, portfolio_id)


@router.patch("/portfolios/{portfolio_id}/assets/{asset_id}")
async def update_portfolio_asset(
    portfolio_id: str,
    asset_id: str,
    payload: PortfolioAssetUpdate,
    db: AsyncSession = Depends(get_db),
):
    asset = await _get_portfolio_asset_or_404(db, portfolio_id, asset_id)
    updates = {
        "id": asset_id,
        "portfolio_id": portfolio_id,
        "symbol": payload.symbol if payload.symbol is not None else asset["symbol"],
        "asset_type": payload.asset_type if payload.asset_type is not None else asset["asset_type"],
        "allocation_pct": payload.allocation_pct if payload.allocation_pct is not None else asset["allocation_pct"],
        "strategy": payload.strategy if payload.strategy is not None else asset.get("strategy"),
        "notes": payload.notes if payload.notes is not None else asset.get("notes"),
    }

    await db.execute(
        text("""
            UPDATE portfolio_assets
            SET symbol = :symbol,
                asset_type = :asset_type,
                allocation_pct = :allocation_pct,
                strategy = :strategy,
                notes = :notes
            WHERE id = :id
              AND portfolio_id = :portfolio_id
        """),
        updates,
    )
    await db.execute(
        text("UPDATE portfolios SET updated_at = :updated_at WHERE id = :id"),
        {"updated_at": datetime.now(timezone.utc), "id": portfolio_id},
    )
    await db.commit()

    return await _get_portfolio_or_404(db, portfolio_id)


@router.delete("/portfolios/{portfolio_id}/assets/{asset_id}")
async def delete_portfolio_asset(portfolio_id: str, asset_id: str, db: AsyncSession = Depends(get_db)):
    await _get_portfolio_asset_or_404(db, portfolio_id, asset_id)
    await db.execute(
        text("""
            DELETE FROM portfolio_assets
            WHERE id = :id
              AND portfolio_id = :portfolio_id
        """),
        {"id": asset_id, "portfolio_id": portfolio_id},
    )
    await db.execute(
        text("UPDATE portfolios SET updated_at = :updated_at WHERE id = :id"),
        {"updated_at": datetime.now(timezone.utc), "id": portfolio_id},
    )
    await db.commit()
    return {"status": "ok", "deleted_id": asset_id}


async def _load_portfolios(db: AsyncSession) -> list[dict]:
    portfolio_rows = (
        await db.execute(
            text("""
                SELECT id, name, strategy, notes, created_at, updated_at
                FROM portfolios
                ORDER BY updated_at DESC, created_at DESC
            """)
        )
    ).fetchall()

    asset_rows = (
        await db.execute(
            text("""
                SELECT id, portfolio_id, symbol, asset_type, allocation_pct, strategy, notes, created_at
                FROM portfolio_assets
                ORDER BY created_at ASC
            """)
        )
    ).fetchall()

    assets_by_portfolio: dict[str, list[dict]] = {}
    for row in asset_rows:
        assets_by_portfolio.setdefault(row.portfolio_id, []).append(
            {
                "id": row.id,
                "symbol": row.symbol,
                "asset_type": row.asset_type,
                "allocation_pct": row.allocation_pct,
                "strategy": row.strategy,
                "notes": row.notes,
                "created_at": row.created_at,
            }
        )

    portfolios = []
    for row in portfolio_rows:
        assets = assets_by_portfolio.get(row.id, [])
        portfolios.append(
            {
                "id": row.id,
                "name": row.name,
                "strategy": row.strategy,
                "notes": row.notes,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
                "asset_count": len(assets),
                "allocation_pct": round(
                    sum(asset.get("allocation_pct") or 0.0 for asset in assets),
                    2,
                ),
                "assets": assets,
            }
        )
    return portfolios


async def _get_portfolio_or_404(db: AsyncSession, portfolio_id: str) -> dict:
    portfolios = await _load_portfolios(db)
    for portfolio in portfolios:
        if portfolio["id"] == portfolio_id:
            return portfolio
    raise HTTPException(status_code=404, detail=f"Portfolio {portfolio_id} not found")


async def _get_portfolio_asset_or_404(db: AsyncSession, portfolio_id: str, asset_id: str) -> dict:
    result = await db.execute(
        text("""
            SELECT id, portfolio_id, symbol, asset_type, allocation_pct, strategy, notes, created_at
            FROM portfolio_assets
            WHERE id = :id
              AND portfolio_id = :portfolio_id
        """),
        {"id": asset_id, "portfolio_id": portfolio_id},
    )
    row = result.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Asset {asset_id} not found")
    return {
        "id": row.id,
        "portfolio_id": row.portfolio_id,
        "symbol": row.symbol,
        "asset_type": row.asset_type,
        "allocation_pct": row.allocation_pct,
        "strategy": row.strategy,
        "notes": row.notes,
        "created_at": row.created_at,
    }
