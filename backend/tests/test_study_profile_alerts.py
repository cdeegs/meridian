from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.services.study_profile_alerts import StudyProfileAlert, StudyProfileAlertService


def _subscription(**overrides) -> StudyProfileAlert:
    base = {
        "id": "profile-alert-1",
        "symbol": "BTC-USD",
        "timeframe": "1h",
        "profile_key": "balanced",
        "delivery": "telegram",
        "status": "active",
        "last_signal": 0,
        "last_evaluated_at": None,
        "last_triggered_at": None,
        "created_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    base.update(overrides)
    return StudyProfileAlert(**base)


@pytest.mark.asyncio
async def test_profile_alert_triggers_only_on_transition_to_constructive():
    notifier = MagicMock()
    notifier.notify_alert = AsyncMock()
    service = StudyProfileAlertService(MagicMock(), notifier=notifier)
    service._persist_evaluation = AsyncMock()
    service._safe_snapshot = AsyncMock(
        return_value={
            "symbol": "BTC-USD",
            "timeframe": "1h",
            "profile_key": "balanced",
            "profile_title": "Balanced Structure",
            "signal": 1,
            "signal_label": "Constructive",
            "signal_summary": "Balanced Structure is aligned for a constructive long read right now.",
            "fit_score_pct": 81.0,
            "last_close": 120345.12,
        }
    )

    alert = _subscription(last_signal=0)
    payload = await service._evaluate_subscription(alert, datetime.now(timezone.utc))

    assert payload is not None
    assert payload["condition"] == "study_profile_ready"
    assert payload["profile_key"] == "balanced"
    assert "constructive" in payload["message"].lower()

    payload = await service._evaluate_subscription(alert, datetime.now(timezone.utc))
    assert payload is None


def test_profile_alert_to_dict_includes_signal_labels():
    alert = _subscription(last_signal=1)
    payload = alert.to_dict()

    assert payload["profile_title"] == "Balanced Structure"
    assert payload["last_signal_label"] == "Constructive"
    assert payload["last_signal_tone"] == "positive"
