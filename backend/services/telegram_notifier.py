"""
Telegram notification service for triggered Meridian alerts.
"""
import logging

import httpx

logger = logging.getLogger(__name__)


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._base_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    async def notify_alert(self, alert: dict) -> None:
        message = self._format_message(alert)
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                self._base_url,
                json={
                    "chat_id": self._chat_id,
                    "text": message,
                    "disable_web_page_preview": True,
                },
            )
            response.raise_for_status()

        logger.info("Sent Telegram alert for %s (%s)", alert["symbol"], alert["condition"])

    @staticmethod
    def _format_message(alert: dict) -> str:
        if alert.get("condition") == "study_profile_ready":
            lines = [
                "Meridian Study Profile Alert",
                f"Symbol: {alert['symbol']}",
                f"Timeframe: {alert.get('timeframe', '-')}",
                f"Profile: {alert.get('profile_title') or alert.get('profile_key') or 'Study Profile'}",
                f"State: {alert.get('signal_label', 'Constructive')}",
                f"Message: {alert['message']}",
            ]
            observed = alert.get("observed_value") or {}
            last_close = observed.get("last_close")
            fit_score = observed.get("fit_score_pct")
            if last_close is not None:
                lines.append(f"Last Close: {last_close}")
            if fit_score is not None:
                lines.append(f"Fit Score: {fit_score}")
            if alert.get("triggered_at"):
                lines.append(f"Triggered At: {alert['triggered_at']}")
            return "\n".join(lines)

        lines = [
            "Meridian Alert Triggered",
            f"Symbol: {alert['symbol']}",
            f"Condition: {alert['condition']}",
            f"Message: {alert['message']}",
        ]
        if alert.get("threshold") is not None:
            lines.append(f"Threshold: {alert['threshold']}")
        if alert.get("observed_value") is not None:
            lines.append(f"Observed: {alert['observed_value']}")
        if alert.get("triggered_at"):
            lines.append(f"Triggered At: {alert['triggered_at']}")
        return "\n".join(lines)
