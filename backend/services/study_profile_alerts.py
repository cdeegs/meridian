"""
Study profile alert subscriptions for Meridian.

These alerts watch a specific symbol + timeframe + study profile and trigger
when the profile transitions into a constructive long state.
"""
import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import DefaultDict, List, Optional
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from backend.db.database import AsyncSessionLocal
from backend.routes import charts as charts_routes

logger = logging.getLogger(__name__)

SUPPORTED_PROFILE_KEYS = {"responsive", "balanced", "trend"}
SUPPORTED_PROFILE_ALERT_CONDITION = "study_profile_ready"

_PROFILE_TITLES = {
    "responsive": "Responsive Tape",
    "balanced": "Balanced Structure",
    "trend": "Trend Continuation",
}


@dataclass
class StudyProfileAlert:
    id: str
    symbol: str
    timeframe: str
    profile_key: str
    delivery: str
    status: str
    last_signal: int
    last_evaluated_at: Optional[datetime]
    last_triggered_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    @property
    def profile_title(self) -> str:
        return _PROFILE_TITLES.get(self.profile_key, self.profile_key.replace("_", " ").title())

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "profile_key": self.profile_key,
            "profile_title": self.profile_title,
            "delivery": self.delivery,
            "status": self.status,
            "last_signal": self.last_signal,
            "last_signal_label": _signal_label(self.last_signal),
            "last_signal_tone": _signal_tone(self.last_signal),
            "last_evaluated_at": self.last_evaluated_at.isoformat() if self.last_evaluated_at else None,
            "last_triggered_at": self.last_triggered_at.isoformat() if self.last_triggered_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


class StudyProfileAlertService:
    def __init__(self, db_engine: AsyncEngine, notifier=None):
        self._db_engine = db_engine
        self._notifier = notifier
        self._active_by_symbol: DefaultDict[str, List[StudyProfileAlert]] = defaultdict(list)
        self._lock = asyncio.Lock()

    @property
    def telegram_configured(self) -> bool:
        return self._notifier is not None

    async def load_active_alerts(self) -> None:
        async with self._db_engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT
                        id,
                        symbol,
                        timeframe,
                        profile_key,
                        delivery,
                        status,
                        last_signal,
                        last_evaluated_at,
                        last_triggered_at,
                        created_at,
                        updated_at
                    FROM study_profile_alerts
                    WHERE status = 'active'
                    ORDER BY updated_at DESC, created_at DESC
                    """
                )
            )
            rows = result.fetchall()

        active_by_symbol: DefaultDict[str, List[StudyProfileAlert]] = defaultdict(list)
        for row in rows:
            alert = self._row_to_alert(row)
            active_by_symbol[alert.symbol].append(alert)

        async with self._lock:
            self._active_by_symbol = active_by_symbol

        logger.info("Loaded %d active study profile alerts", len(rows))

    async def list_alerts(
        self,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> List[dict]:
        clauses = []
        params: dict = {}
        if symbol:
            clauses.append("symbol = :symbol")
            params["symbol"] = symbol.upper()
        if timeframe:
            clauses.append("timeframe = :timeframe")
            params["timeframe"] = timeframe

        query = """
            SELECT
                id,
                symbol,
                timeframe,
                profile_key,
                delivery,
                status,
                last_signal,
                last_evaluated_at,
                last_triggered_at,
                created_at,
                updated_at
            FROM study_profile_alerts
        """
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC, created_at DESC"

        async with self._db_engine.connect() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()
        return [self._row_to_alert(row).to_dict() for row in rows]

    async def create_alert(self, symbol: str, timeframe: str, profile_key: str) -> dict:
        symbol = symbol.upper()
        self._validate(timeframe=timeframe, profile_key=profile_key)
        existing = await self._find_existing(symbol, timeframe, profile_key)
        now = datetime.now(timezone.utc)
        snapshot = await self._safe_snapshot(symbol, timeframe, profile_key)
        last_signal = int(snapshot.get("signal") or 0) if snapshot else 0
        last_evaluated_at = now if snapshot else None

        if existing is None:
            alert = StudyProfileAlert(
                id=str(uuid4()),
                symbol=symbol,
                timeframe=timeframe,
                profile_key=profile_key,
                delivery="telegram",
                status="active",
                last_signal=last_signal,
                last_evaluated_at=last_evaluated_at,
                last_triggered_at=None,
                created_at=now,
                updated_at=now,
            )
            async with self._db_engine.begin() as conn:
                await conn.execute(
                    text(
                        """
                        INSERT INTO study_profile_alerts (
                            id,
                            symbol,
                            timeframe,
                            profile_key,
                            delivery,
                            status,
                            last_signal,
                            last_evaluated_at,
                            last_triggered_at,
                            created_at,
                            updated_at
                        )
                        VALUES (
                            :id,
                            :symbol,
                            :timeframe,
                            :profile_key,
                            :delivery,
                            :status,
                            :last_signal,
                            :last_evaluated_at,
                            :last_triggered_at,
                            :created_at,
                            :updated_at
                        )
                        """
                    ),
                    {
                        "id": alert.id,
                        "symbol": alert.symbol,
                        "timeframe": alert.timeframe,
                        "profile_key": alert.profile_key,
                        "delivery": alert.delivery,
                        "status": alert.status,
                        "last_signal": alert.last_signal,
                        "last_evaluated_at": last_evaluated_at,
                        "last_triggered_at": None,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
        else:
            alert = existing
            alert.status = "active"
            alert.last_signal = last_signal
            alert.last_evaluated_at = last_evaluated_at
            alert.updated_at = now
            async with self._db_engine.begin() as conn:
                await conn.execute(
                    text(
                        """
                        UPDATE study_profile_alerts
                        SET status = 'active',
                            last_signal = :last_signal,
                            last_evaluated_at = :last_evaluated_at,
                            updated_at = :updated_at
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": alert.id,
                        "last_signal": alert.last_signal,
                        "last_evaluated_at": alert.last_evaluated_at,
                        "updated_at": alert.updated_at,
                    },
                )

        async with self._lock:
            self._remove_from_active(alert.id)
            self._active_by_symbol[alert.symbol].append(alert)

        return alert.to_dict()

    async def activate_alert(self, alert_id: str) -> dict:
        alert = await self._load_alert(alert_id)
        snapshot = await self._safe_snapshot(alert.symbol, alert.timeframe, alert.profile_key)
        now = datetime.now(timezone.utc)
        alert.status = "active"
        alert.last_signal = int(snapshot.get("signal") or 0) if snapshot else 0
        alert.last_evaluated_at = now if snapshot else alert.last_evaluated_at
        alert.updated_at = now

        async with self._db_engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    UPDATE study_profile_alerts
                    SET status = 'active',
                        last_signal = :last_signal,
                        last_evaluated_at = :last_evaluated_at,
                        updated_at = :updated_at
                    WHERE id = :id
                    """
                ),
                {
                    "id": alert.id,
                    "last_signal": alert.last_signal,
                    "last_evaluated_at": alert.last_evaluated_at,
                    "updated_at": alert.updated_at,
                },
            )

        async with self._lock:
            self._remove_from_active(alert.id)
            self._active_by_symbol[alert.symbol].append(alert)
        return alert.to_dict()

    async def disable_alert(self, alert_id: str) -> dict:
        alert = await self._load_alert(alert_id)
        alert.status = "disabled"
        alert.updated_at = datetime.now(timezone.utc)

        async with self._db_engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    UPDATE study_profile_alerts
                    SET status = 'disabled',
                        updated_at = :updated_at
                    WHERE id = :id
                    """
                ),
                {
                    "id": alert.id,
                    "updated_at": alert.updated_at,
                },
            )

        async with self._lock:
            self._remove_from_active(alert.id)
        return alert.to_dict()

    async def process_symbols(self, symbols: set[str]) -> List[dict]:
        if not symbols:
            return []

        now = datetime.now(timezone.utc)
        async with self._lock:
            candidates = [
                alert
                for symbol in {item.upper() for item in symbols}
                for alert in self._active_by_symbol.get(symbol, [])
                if self._due_for_evaluation(alert, now)
            ]

        if not candidates:
            return []

        triggered_payloads: List[dict] = []
        for alert in candidates:
            payload = await self._evaluate_subscription(alert, now)
            if payload is not None:
                triggered_payloads.append(payload)

        if triggered_payloads:
            await self._notify(triggered_payloads)

        return triggered_payloads

    async def _evaluate_subscription(self, alert: StudyProfileAlert, now: datetime) -> Optional[dict]:
        snapshot = await self._safe_snapshot(alert.symbol, alert.timeframe, alert.profile_key)
        next_signal = int(snapshot.get("signal") or 0) if snapshot else 0
        triggered = alert.last_signal != 1 and next_signal == 1 and snapshot is not None
        alert.last_signal = next_signal
        alert.last_evaluated_at = now
        alert.updated_at = now
        if triggered:
            alert.last_triggered_at = now

        await self._persist_evaluation(alert)

        if not triggered or snapshot is None:
            return None
        return self._build_payload(alert, snapshot, now)

    async def _persist_evaluation(self, alert: StudyProfileAlert) -> None:
        async with self._db_engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    UPDATE study_profile_alerts
                    SET last_signal = :last_signal,
                        last_evaluated_at = :last_evaluated_at,
                        last_triggered_at = :last_triggered_at,
                        updated_at = :updated_at
                    WHERE id = :id
                    """
                ),
                {
                    "id": alert.id,
                    "last_signal": alert.last_signal,
                    "last_evaluated_at": alert.last_evaluated_at,
                    "last_triggered_at": alert.last_triggered_at,
                    "updated_at": alert.updated_at,
                },
            )

    async def _notify(self, payloads: List[dict]) -> None:
        if self._notifier is None or not payloads:
            return

        results = await asyncio.gather(
            *(self._notifier.notify_alert(payload) for payload in payloads),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Study profile alert notification failed: %s", result)

    async def _safe_snapshot(self, symbol: str, timeframe: str, profile_key: str) -> Optional[dict]:
        try:
            async with AsyncSessionLocal() as session:
                return await charts_routes.resolve_study_profile_snapshot(
                    symbol=symbol,
                    timeframe=timeframe,
                    profile_key=profile_key,
                    db=session,
                )
        except HTTPException as exc:
            logger.debug(
                "Study profile snapshot unavailable for %s %s %s: %s",
                symbol,
                timeframe,
                profile_key,
                exc.detail,
            )
        except Exception as exc:
            logger.warning(
                "Study profile snapshot failed for %s %s %s: %s",
                symbol,
                timeframe,
                profile_key,
                exc,
            )
        return None

    async def _find_existing(self, symbol: str, timeframe: str, profile_key: str) -> Optional[StudyProfileAlert]:
        async with self._db_engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT
                        id,
                        symbol,
                        timeframe,
                        profile_key,
                        delivery,
                        status,
                        last_signal,
                        last_evaluated_at,
                        last_triggered_at,
                        created_at,
                        updated_at
                    FROM study_profile_alerts
                    WHERE symbol = :symbol
                      AND timeframe = :timeframe
                      AND profile_key = :profile_key
                      AND delivery = 'telegram'
                    LIMIT 1
                    """
                ),
                {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "profile_key": profile_key,
                },
            )
            row = result.fetchone()
        return self._row_to_alert(row) if row else None

    async def _load_alert(self, alert_id: str) -> StudyProfileAlert:
        async with self._db_engine.connect() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT
                        id,
                        symbol,
                        timeframe,
                        profile_key,
                        delivery,
                        status,
                        last_signal,
                        last_evaluated_at,
                        last_triggered_at,
                        created_at,
                        updated_at
                    FROM study_profile_alerts
                    WHERE id = :id
                    """
                ),
                {"id": alert_id},
            )
            row = result.fetchone()

        if row is None:
            raise KeyError(alert_id)
        return self._row_to_alert(row)

    @staticmethod
    def _row_to_alert(row) -> StudyProfileAlert:
        return StudyProfileAlert(
            id=row.id,
            symbol=row.symbol,
            timeframe=row.timeframe,
            profile_key=row.profile_key,
            delivery=row.delivery,
            status=row.status,
            last_signal=int(row.last_signal or 0),
            last_evaluated_at=row.last_evaluated_at,
            last_triggered_at=row.last_triggered_at,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )

    @staticmethod
    def _validate(timeframe: str, profile_key: str) -> None:
        if timeframe not in charts_routes._TIMEFRAME_SPECS:
            raise ValueError(f"Unsupported timeframe '{timeframe}'")
        if profile_key not in SUPPORTED_PROFILE_KEYS:
            raise ValueError(f"Unsupported study profile '{profile_key}'")

    @staticmethod
    def _due_for_evaluation(alert: StudyProfileAlert, now: datetime) -> bool:
        if alert.last_evaluated_at is None:
            return True
        return now - alert.last_evaluated_at >= _evaluation_interval(alert.timeframe)

    def _remove_from_active(self, alert_id: str) -> None:
        for symbol in list(self._active_by_symbol):
            remaining = [alert for alert in self._active_by_symbol[symbol] if alert.id != alert_id]
            if remaining:
                self._active_by_symbol[symbol] = remaining
            else:
                del self._active_by_symbol[symbol]

    @staticmethod
    def _build_payload(alert: StudyProfileAlert, snapshot: dict, now: datetime) -> dict:
        return {
            "id": alert.id,
            "symbol": alert.symbol,
            "condition": SUPPORTED_PROFILE_ALERT_CONDITION,
            "status": "triggered",
            "profile_key": alert.profile_key,
            "profile_title": alert.profile_title,
            "timeframe": alert.timeframe,
            "signal_label": snapshot.get("signal_label"),
            "message": (
                f"{alert.symbol} {alert.timeframe} {alert.profile_title} is now constructive. "
                f"{snapshot.get('signal_summary') or snapshot.get('timing_note') or 'Profile conditions are aligned.'}"
            ),
            "observed_value": {
                "last_close": snapshot.get("last_close"),
                "fit_score_pct": snapshot.get("fit_score_pct"),
            },
            "triggered_at": now.isoformat(),
        }


def _evaluation_interval(timeframe: str) -> timedelta:
    spec = charts_routes._TIMEFRAME_SPECS.get(timeframe)
    if spec is None:
        return timedelta(seconds=60)
    seconds = spec["bucket_width"].total_seconds()
    throttled = max(20.0, min(seconds / 6.0, 300.0))
    return timedelta(seconds=throttled)


def _signal_label(signal: int) -> str:
    if signal > 0:
        return "Constructive"
    if signal < 0:
        return "Defensive"
    return "Not Ready"


def _signal_tone(signal: int) -> str:
    if signal > 0:
        return "positive"
    if signal < 0:
        return "negative"
    return "neutral"
