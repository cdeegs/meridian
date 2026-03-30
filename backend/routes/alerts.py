from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator, model_validator

from backend.services.alert_engine import (
    SUPPORTED_CONDITIONS,
    THRESHOLD_CONDITIONS,
    AlertEngine,
)

router = APIRouter(prefix="/api", tags=["alerts"])

_alert_engine: Optional[AlertEngine] = None


class AlertCreate(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=32)
    condition: str
    threshold: Optional[float] = None

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("condition")
    @classmethod
    def validate_condition(cls, value: str) -> str:
        condition = value.strip().lower()
        if condition not in SUPPORTED_CONDITIONS:
            raise ValueError(f"Unsupported condition '{value}'")
        return condition

    @model_validator(mode="after")
    def validate_threshold(self):
        if self.condition in THRESHOLD_CONDITIONS and self.threshold is None:
            raise ValueError(f"Condition '{self.condition}' requires a threshold")
        if self.condition not in THRESHOLD_CONDITIONS:
            self.threshold = None
        return self


def set_alert_engine(engine: AlertEngine) -> None:
    global _alert_engine
    _alert_engine = engine


def get_alert_engine() -> AlertEngine:
    if _alert_engine is None:
        raise HTTPException(status_code=503, detail="Alert engine not ready")
    return _alert_engine


@router.get("/alerts")
async def list_alerts(status_filter: Optional[str] = Query(default=None, alias="status")):
    if status_filter and status_filter not in {"active", "triggered", "disabled"}:
        raise HTTPException(status_code=400, detail="status must be active, triggered, or disabled")
    engine = get_alert_engine()
    return {"alerts": await engine.list_alerts(status_filter)}


@router.post("/alerts", status_code=status.HTTP_201_CREATED)
async def create_alert(payload: AlertCreate):
    engine = get_alert_engine()
    return await engine.create_alert(
        symbol=payload.symbol,
        condition=payload.condition,
        threshold=payload.threshold,
    )


@router.post("/alerts/{alert_id}/activate")
async def activate_alert(alert_id: str):
    engine = get_alert_engine()
    try:
        return await engine.activate_alert(alert_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")


@router.post("/alerts/{alert_id}/disable")
async def disable_alert(alert_id: str):
    engine = get_alert_engine()
    try:
        return await engine.disable_alert(alert_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
