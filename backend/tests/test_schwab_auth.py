from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

from backend.services.schwab_auth import SchwabOAuthClient, SchwabTokenStore


def test_schwab_auth_url_generation_persists_state(tmp_path):
    store = SchwabTokenStore(str(tmp_path / "schwab_tokens.json"))
    client = SchwabOAuthClient(
        client_id="client-id",
        client_secret="client-secret",
        redirect_uri="http://127.0.0.1:8765/schwab/callback",
        authorize_url="https://api.schwabapi.com/v1/oauth/authorize",
        token_url="https://api.schwabapi.com/v1/oauth/token",
        token_store=store,
        scope="marketdata",
    )

    payload = client.create_authorization()
    parsed = urlparse(payload["authorize_url"])
    params = parse_qs(parsed.query)

    assert parsed.netloc == "api.schwabapi.com"
    assert params["client_id"] == ["client-id"]
    assert params["redirect_uri"] == ["http://127.0.0.1:8765/schwab/callback"]
    assert params["state"] == [payload["state"]]
    assert params["scope"] == ["marketdata"]
    assert store.load_pending_state()["value"] == payload["state"]


def test_schwab_token_store_round_trip(tmp_path):
    store = SchwabTokenStore(str(tmp_path / "schwab_tokens.json"))
    store.save_pending_state("pending-state")
    store.save_tokens(
        {
            "access_token": "token",
            "refresh_token": "refresh",
            "token_type": "Bearer",
            "scope": "marketdata",
            "issued_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
        }
    )

    tokens = store.load_tokens()
    assert tokens["access_token"] == "token"
    assert store.load_pending_state() is None

    store.clear_tokens()
    assert store.load_tokens() is None


def test_schwab_status_reports_authorized_tokens(tmp_path):
    store = SchwabTokenStore(str(tmp_path / "schwab_tokens.json"))
    store.save_tokens(
        {
            "access_token": "token",
            "refresh_token": "refresh",
            "token_type": "Bearer",
            "scope": "marketdata",
            "issued_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
        }
    )
    client = SchwabOAuthClient(
        client_id="client-id",
        client_secret="client-secret",
        redirect_uri="http://127.0.0.1:8765/schwab/callback",
        authorize_url="https://api.schwabapi.com/v1/oauth/authorize",
        token_url="https://api.schwabapi.com/v1/oauth/token",
        token_store=store,
    )

    status = client.get_status()

    assert status["configured"] is True
    assert status["authorized"] is True
    assert status["has_refresh_token"] is True
    assert status["expired"] is False
