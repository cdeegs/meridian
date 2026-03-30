"""
Alert engine for Meridian.

Responsibilities:
- Persist alert definitions in Postgres
- Keep active alerts in memory for low-latency evaluation
- Evaluate price- and indicator-based rules after each ingestion batch
- Emit structured alert events for WebSocket clients
"""
import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import DefaultDict, Dict, List, Optional
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from backend.models.price_event import PriceEvent

logger = logging.getLogger(__name__)

SUPPORTED_CONDITIONS = {
    "price_above",
    "price_below",
    "rsi_above",
    "rsi_below",
    "macd_cross_up",
    "macd_cross_down",
}

THRESHOLD_CONDITIONS = {
    "price_above",
    "price_below",
    "rsi_above",
    "rsi_below",
}


@dataclass
class AlertRule:
    id: str
    symbol: str
    condition: str
    threshold: Optional[float]
    status: str
    created_at: datetime
    triggered_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "symbol": self.symbol,
            "condition": self.condition,
            "threshold": self.threshold,
            "status": self.status,
            "created_at": self.created_at,
            "triggered_at": self.triggered_at,
        }


class AlertEngine:
    def __init__(self, db_engine: AsyncEngine, notifier=None):
        self._db_engine = db_engine
        self._notifier = notifier
        self._active_by_symbol: DefaultDict[str, List[AlertRule]] = defaultdict(list)
        self._last_indicator_values: DefaultDict[str, Dict[str, dict]] = defaultdict(dict)
        self._lock = asyncio.Lock()

    async def load_active_alerts(self) -> None:
        async with self._db_engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT id, symbol, condition, threshold, status, triggered_at, created_at
                    FROM alerts
                    WHERE status = 'active'
                    ORDER BY created_at DESC
                """)
            )
            rows = result.fetchall()

        active_by_symbol: DefaultDict[str, List[AlertRule]] = defaultdict(list)
        for row in rows:
            alert = self._row_to_rule(row)
            active_by_symbol[alert.symbol].append(alert)

        async with self._lock:
            self._active_by_symbol = active_by_symbol

        logger.info("Loaded %d active alerts", len(rows))

    async def list_alerts(self, status: Optional[str] = None) -> List[dict]:
        query = """
            SELECT id, symbol, condition, threshold, status, triggered_at, created_at
            FROM alerts
        """
        params: dict = {}
        if status:
            query += " WHERE status = :status"
            params["status"] = status
        query += " ORDER BY created_at DESC"

        async with self._db_engine.connect() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()
        return [self._row_to_rule(row).to_dict() for row in rows]

    async def create_alert(self, symbol: str, condition: str, threshold: Optional[float]) -> dict:
        alert = AlertRule(
            id=str(uuid4()),
            symbol=symbol.upper(),
            condition=condition,
            threshold=threshold,
            status="active",
            created_at=datetime.now(timezone.utc),
        )

        async with self._db_engine.begin() as conn:
            await conn.execute(
                text("""
                    INSERT INTO alerts (id, symbol, condition, threshold, status, created_at)
                    VALUES (:id, :symbol, :condition, :threshold, :status, :created_at)
                """),
                alert.to_dict(),
            )

        async with self._lock:
            self._active_by_symbol[alert.symbol].append(alert)

        return alert.to_dict()

    async def activate_alert(self, alert_id: str) -> dict:
        alert = await self._set_status(alert_id, "active")
        async with self._lock:
            self._remove_from_active(alert_id)
            self._active_by_symbol[alert.symbol].append(alert)
        return alert.to_dict()

    async def disable_alert(self, alert_id: str) -> dict:
        alert = await self._set_status(alert_id, "disabled")
        async with self._lock:
            self._remove_from_active(alert_id)
        return alert.to_dict()

    async def process_batch(
        self,
        batch: List[PriceEvent],
        indicator_results: List[dict],
    ) -> List[dict]:
        latest_prices: Dict[str, PriceEvent] = {event.symbol: event for event in batch}

        indicators_by_symbol: DefaultDict[str, Dict[str, dict]] = defaultdict(dict)
        for result in indicator_results:
            indicators_by_symbol[result["symbol"]][result["indicator"]] = result["value"]

        triggered_rules: List[AlertRule] = []
        alert_payloads: List[dict] = []

        async with self._lock:
            for symbol in set(latest_prices) | set(indicators_by_symbol):
                active_alerts = list(self._active_by_symbol.get(symbol, []))
                if not active_alerts:
                    if symbol in indicators_by_symbol:
                        self._last_indicator_values[symbol].update(indicators_by_symbol[symbol])
                    continue

                current_indicators = indicators_by_symbol.get(symbol, {})
                previous_indicators = dict(self._last_indicator_values.get(symbol, {}))
                price_event = latest_prices.get(symbol)

                remaining: List[AlertRule] = []
                for alert in active_alerts:
                    payload = self._evaluate_alert(
                        alert=alert,
                        price_event=price_event,
                        current_indicators=current_indicators,
                        previous_indicators=previous_indicators,
                    )
                    if payload is None:
                        remaining.append(alert)
                        continue

                    alert.status = "triggered"
                    alert.triggered_at = datetime.now(timezone.utc)
                    payload["triggered_at"] = alert.triggered_at.isoformat()
                    triggered_rules.append(alert)
                    alert_payloads.append(payload)

                if remaining:
                    self._active_by_symbol[symbol] = remaining
                elif symbol in self._active_by_symbol:
                    del self._active_by_symbol[symbol]

                if current_indicators:
                    self._last_indicator_values[symbol].update(current_indicators)

        if triggered_rules:
            await self._mark_triggered(triggered_rules)
            await self._notify(alert_payloads)

        return alert_payloads

    async def _set_status(self, alert_id: str, status: str) -> AlertRule:
        now = datetime.now(timezone.utc)
        params = {
            "id": alert_id,
            "status": status,
            "triggered_at": None if status == "active" else None,
        }

        async with self._db_engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE alerts
                    SET status = :status,
                        triggered_at = :triggered_at
                    WHERE id = :id
                """),
                params,
            )

        async with self._db_engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT id, symbol, condition, threshold, status, triggered_at, created_at
                    FROM alerts
                    WHERE id = :id
                """),
                {"id": alert_id},
            )
            row = result.fetchone()

        if row is None:
            raise KeyError(alert_id)

        alert = self._row_to_rule(row)
        if status == "active":
            alert.triggered_at = None
        logger.info("Alert %s set to %s at %s", alert.id, status, now.isoformat())
        return alert

    async def _mark_triggered(self, alerts: List[AlertRule]) -> None:
        rows = [
            {"id": alert.id, "triggered_at": alert.triggered_at}
            for alert in alerts
        ]
        async with self._db_engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE alerts
                    SET status = 'triggered',
                        triggered_at = :triggered_at
                    WHERE id = :id
                """),
                rows,
            )

    def _remove_from_active(self, alert_id: str) -> None:
        for symbol in list(self._active_by_symbol):
            remaining = [alert for alert in self._active_by_symbol[symbol] if alert.id != alert_id]
            if remaining:
                self._active_by_symbol[symbol] = remaining
            else:
                del self._active_by_symbol[symbol]

    async def _notify(self, alert_payloads: List[dict]) -> None:
        if self._notifier is None or not alert_payloads:
            return

        results = await asyncio.gather(
            *(self._notifier.notify_alert(payload) for payload in alert_payloads),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                logger.warning("Alert notification failed: %s", result)

    def _evaluate_alert(
        self,
        alert: AlertRule,
        price_event: Optional[PriceEvent],
        current_indicators: Dict[str, dict],
        previous_indicators: Dict[str, dict],
    ) -> Optional[dict]:
        condition = alert.condition

        if condition == "price_above" and price_event and alert.threshold is not None:
            if price_event.price > alert.threshold:
                return self._build_payload(
                    alert,
                    observed_value=round(price_event.price, 6),
                    message=(
                        f"{alert.symbol} price moved above {alert.threshold:.2f} "
                        f"(current {price_event.price:.2f})"
                    ),
                )

        if condition == "price_below" and price_event and alert.threshold is not None:
            if price_event.price < alert.threshold:
                return self._build_payload(
                    alert,
                    observed_value=round(price_event.price, 6),
                    message=(
                        f"{alert.symbol} price moved below {alert.threshold:.2f} "
                        f"(current {price_event.price:.2f})"
                    ),
                )

        rsi = self._indicator_scalar(current_indicators, "rsi_14")
        if condition == "rsi_above" and rsi is not None and alert.threshold is not None:
            if rsi > alert.threshold:
                return self._build_payload(
                    alert,
                    observed_value=round(rsi, 4),
                    message=f"{alert.symbol} RSI rose above {alert.threshold:.1f} (current {rsi:.2f})",
                )

        if condition == "rsi_below" and rsi is not None and alert.threshold is not None:
            if rsi < alert.threshold:
                return self._build_payload(
                    alert,
                    observed_value=round(rsi, 4),
                    message=f"{alert.symbol} RSI fell below {alert.threshold:.1f} (current {rsi:.2f})",
                )

        if condition in {"macd_cross_up", "macd_cross_down"}:
            current_macd = current_indicators.get("macd")
            previous_macd = previous_indicators.get("macd")
            if current_macd and previous_macd:
                previous_diff = float(previous_macd["macd"]) - float(previous_macd["signal"])
                current_diff = float(current_macd["macd"]) - float(current_macd["signal"])

                if condition == "macd_cross_up" and previous_diff <= 0 < current_diff:
                    return self._build_payload(
                        alert,
                        observed_value=current_macd,
                        message=f"{alert.symbol} MACD crossed above signal",
                    )

                if condition == "macd_cross_down" and previous_diff >= 0 > current_diff:
                    return self._build_payload(
                        alert,
                        observed_value=current_macd,
                        message=f"{alert.symbol} MACD crossed below signal",
                    )

        return None

    @staticmethod
    def _indicator_scalar(indicators: Dict[str, dict], indicator_name: str) -> Optional[float]:
        value = indicators.get(indicator_name)
        if not value:
            return None
        raw = value.get("v")
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _build_payload(alert: AlertRule, observed_value, message: str) -> dict:
        return {
            "id": alert.id,
            "symbol": alert.symbol,
            "condition": alert.condition,
            "threshold": alert.threshold,
            "status": "triggered",
            "observed_value": observed_value,
            "message": message,
        }

    @staticmethod
    def _row_to_rule(row) -> AlertRule:
        return AlertRule(
            id=row.id,
            symbol=row.symbol,
            condition=row.condition,
            threshold=row.threshold,
            status=row.status,
            created_at=row.created_at,
            triggered_at=row.triggered_at,
        )
