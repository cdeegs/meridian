from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field, field_validator, model_validator

from backend.routes import charts as charts_routes
from backend.services.alert_engine import (
    SUPPORTED_CONDITIONS,
    THRESHOLD_CONDITIONS,
    AlertEngine,
)
from backend.services.study_profile_alerts import (
    SUPPORTED_PROFILE_KEYS,
    StudyProfileAlertService,
)

router = APIRouter(prefix="/api", tags=["alerts"])

_alert_engine: Optional[AlertEngine] = None
_study_profile_alert_service: Optional[StudyProfileAlertService] = None


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


class ProfileAlertCreate(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=32)
    timeframe: str
    profile_key: str

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()

    @field_validator("timeframe")
    @classmethod
    def validate_timeframe(cls, value: str) -> str:
        timeframe = value.strip()
        if timeframe not in charts_routes._TIMEFRAME_SPECS:
            raise ValueError(f"Unsupported timeframe '{value}'")
        return timeframe

    @field_validator("profile_key")
    @classmethod
    def validate_profile_key(cls, value: str) -> str:
        profile_key = value.strip().lower()
        if profile_key not in SUPPORTED_PROFILE_KEYS:
            raise ValueError(f"Unsupported study profile '{value}'")
        return profile_key


def set_alert_engine(engine: AlertEngine) -> None:
    global _alert_engine
    _alert_engine = engine


def set_study_profile_alert_service(service: StudyProfileAlertService) -> None:
    global _study_profile_alert_service
    _study_profile_alert_service = service


def get_alert_engine() -> AlertEngine:
    if _alert_engine is None:
        raise HTTPException(status_code=503, detail="Alert engine not ready")
    return _alert_engine


def get_study_profile_alert_service() -> StudyProfileAlertService:
    if _study_profile_alert_service is None:
        raise HTTPException(status_code=503, detail="Study profile alert service not ready")
    return _study_profile_alert_service


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


@router.get("/profile-alerts")
async def list_profile_alerts(
    symbol: Optional[str] = Query(default=None),
    timeframe: Optional[str] = Query(default=None),
):
    service = get_study_profile_alert_service()
    alerts = await service.list_alerts(symbol=symbol, timeframe=timeframe)
    return {
        "profile_alerts": alerts,
        "telegram_configured": service.telegram_configured,
    }


@router.post("/profile-alerts", status_code=status.HTTP_201_CREATED)
async def create_profile_alert(payload: ProfileAlertCreate):
    service = get_study_profile_alert_service()
    if not service.telegram_configured:
        raise HTTPException(status_code=409, detail="Telegram notifications are not configured")
    try:
        return await service.create_alert(
            symbol=payload.symbol,
            timeframe=payload.timeframe,
            profile_key=payload.profile_key,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/profile-alerts/{alert_id}/activate")
async def activate_profile_alert(alert_id: str):
    service = get_study_profile_alert_service()
    if not service.telegram_configured:
        raise HTTPException(status_code=409, detail="Telegram notifications are not configured")
    try:
        return await service.activate_alert(alert_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Profile alert {alert_id} not found")


@router.post("/profile-alerts/{alert_id}/disable")
async def disable_profile_alert(alert_id: str):
    service = get_study_profile_alert_service()
    try:
        return await service.disable_alert(alert_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Profile alert {alert_id} not found")
