import base64
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from secrets import token_urlsafe
from typing import Optional
from urllib.parse import urlencode

import httpx

logger = logging.getLogger(__name__)


@dataclass
class SchwabTokenBundle:
    access_token: str
    token_type: str
    expires_at: datetime
    refresh_token: Optional[str] = None
    scope: str = ""
    issued_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "scope": self.scope,
            "issued_at": (self.issued_at or datetime.now(timezone.utc)).isoformat(),
            "expires_at": self.expires_at.isoformat(),
        }


class SchwabTokenStore:
    def __init__(self, path: str):
        self._path = Path(path).expanduser()

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> dict:
        if not self._path.exists():
            return {}
        try:
            return json.loads(self._path.read_text())
        except json.JSONDecodeError:
            logger.warning("Schwab token store at %s is not valid JSON", self._path)
            return {}

    def save(self, payload: dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    def clear(self) -> None:
        if self._path.exists():
            self._path.unlink()

    def load_tokens(self) -> Optional[dict]:
        payload = self.load()
        return payload.get("tokens")

    def save_tokens(self, tokens: dict) -> None:
        payload = self.load()
        payload["tokens"] = tokens
        payload.pop("pending_state", None)
        self.save(payload)

    def clear_tokens(self) -> None:
        payload = self.load()
        payload.pop("tokens", None)
        self.save(payload) if payload else self.clear()

    def load_pending_state(self) -> Optional[dict]:
        payload = self.load()
        return payload.get("pending_state")

    def save_pending_state(self, state: str) -> None:
        payload = self.load()
        payload["pending_state"] = {
            "value": state,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self.save(payload)

    def clear_pending_state(self) -> None:
        payload = self.load()
        payload.pop("pending_state", None)
        self.save(payload) if payload else self.clear()


class SchwabOAuthClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_uri: str,
        authorize_url: str,
        token_url: str,
        token_store: SchwabTokenStore,
        scope: str = "",
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        self._redirect_uri = redirect_uri
        self._authorize_url = authorize_url
        self._token_url = token_url
        self._token_store = token_store
        self._scope = scope

    @property
    def configured(self) -> bool:
        return bool(self._client_id and self._client_secret and self._redirect_uri)

    @property
    def redirect_uri(self) -> str:
        return self._redirect_uri

    def create_authorization(self) -> dict:
        if not self.configured:
            raise RuntimeError("Schwab OAuth is not configured")

        state = token_urlsafe(24)
        self._token_store.save_pending_state(state)

        params = {
            "response_type": "code",
            "client_id": self._client_id,
            "redirect_uri": self._redirect_uri,
            "state": state,
        }
        if self._scope:
            params["scope"] = self._scope

        return {
            "authorize_url": f"{self._authorize_url}?{urlencode(params)}",
            "state": state,
        }

    def get_status(self) -> dict:
        tokens = self._token_store.load_tokens()
        pending_state = self._token_store.load_pending_state()
        expires_at = tokens.get("expires_at") if tokens else None
        expires_at_dt = _parse_datetime(expires_at)

        return {
            "configured": self.configured,
            "redirect_uri": self._redirect_uri,
            "token_path": str(self._token_store.path),
            "authorized": bool(tokens and tokens.get("access_token")),
            "has_refresh_token": bool(tokens and tokens.get("refresh_token")),
            "pending_state": bool(pending_state),
            "expires_at": expires_at,
            "expired": bool(expires_at_dt and expires_at_dt <= datetime.now(timezone.utc)),
        }

    def get_access_token(self) -> Optional[str]:
        tokens = self._token_store.load_tokens()
        if not tokens:
            return None
        expires_at = _parse_datetime(tokens.get("expires_at"))
        if expires_at is not None and expires_at <= datetime.now(timezone.utc):
            return None
        return tokens.get("access_token")

    async def exchange_code(self, code: str, state: Optional[str]) -> dict:
        pending_state = self._token_store.load_pending_state()
        expected_state = pending_state.get("value") if pending_state else None
        if expected_state and state != expected_state:
            raise ValueError("Schwab OAuth state mismatch")

        payload = await self._request_token(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._redirect_uri,
            }
        )
        bundle = self._persist_token_payload(payload)
        self._token_store.clear_pending_state()
        logger.info("Stored Schwab OAuth tokens")
        return bundle.to_dict()

    async def refresh_access_token(self) -> dict:
        tokens = self._token_store.load_tokens()
        refresh_token = tokens.get("refresh_token") if tokens else None
        if not refresh_token:
            raise RuntimeError("No Schwab refresh token available")

        payload = await self._request_token(
            {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            }
        )
        bundle = self._persist_token_payload(payload, fallback_refresh_token=refresh_token)
        logger.info("Refreshed Schwab OAuth tokens")
        return bundle.to_dict()

    def disconnect(self) -> None:
        self._token_store.clear()
        logger.info("Cleared Schwab OAuth tokens")

    async def _request_token(self, form_data: dict) -> dict:
        basic = base64.b64encode(f"{self._client_id}:{self._client_secret}".encode()).decode()
        headers = {
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(self._token_url, data=form_data, headers=headers)
            response.raise_for_status()
            return response.json()

    def _persist_token_payload(self, payload: dict, fallback_refresh_token: Optional[str] = None) -> SchwabTokenBundle:
        now = datetime.now(timezone.utc)
        expires_in = int(payload.get("expires_in", 1800))
        refresh_token = payload.get("refresh_token") or fallback_refresh_token

        bundle = SchwabTokenBundle(
            access_token=payload["access_token"],
            refresh_token=refresh_token,
            token_type=payload.get("token_type", "Bearer"),
            scope=payload.get("scope", self._scope),
            issued_at=now,
            expires_at=now + timedelta(seconds=expires_in),
        )
        self._token_store.save_tokens(bundle.to_dict())
        return bundle


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None
