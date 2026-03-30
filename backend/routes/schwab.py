from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse

router = APIRouter(tags=["schwab"])

_auth_client = None
_market_data_client = None


def set_schwab_services(auth_client, market_data_client) -> None:
    global _auth_client, _market_data_client
    _auth_client = auth_client
    _market_data_client = market_data_client


def _require_auth_client():
    if _auth_client is None:
        raise HTTPException(status_code=503, detail="Schwab auth is not configured")
    if not _auth_client.configured:
        raise HTTPException(status_code=503, detail="Schwab credentials are missing")
    return _auth_client


@router.get("/api/schwab/status")
async def schwab_status():
    auth_status = _auth_client.get_status() if _auth_client is not None else {"configured": False}
    market_data_status = (
        _market_data_client.get_status()
        if _market_data_client is not None
        else {"configured": False, "implementation": "not_initialized"}
    )
    return {
        "provider": "schwab",
        "auth": auth_status,
        "market_data": market_data_status,
    }


@router.get("/api/schwab/auth/url")
async def schwab_auth_url():
    auth_client = _require_auth_client()
    return auth_client.create_authorization()


@router.get("/api/schwab/auth/start", include_in_schema=False)
async def schwab_auth_start():
    auth_client = _require_auth_client()
    payload = auth_client.create_authorization()
    return RedirectResponse(payload["authorize_url"])


@router.get("/schwab/callback", include_in_schema=False)
async def schwab_callback(
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    error_description: str | None = Query(default=None),
):
    if error:
        message = error_description or error
        return HTMLResponse(
            f"<h1>Schwab authorization failed</h1><p>{message}</p>",
            status_code=400,
        )

    if not code:
        raise HTTPException(status_code=400, detail="Missing Schwab authorization code")

    auth_client = _require_auth_client()
    try:
        token_bundle = await auth_client.exchange_code(code=code, state=state)
    except ValueError as exc:
        return HTMLResponse(f"<h1>Schwab authorization failed</h1><p>{exc}</p>", status_code=400)
    except Exception as exc:
        return HTMLResponse(f"<h1>Schwab token exchange failed</h1><p>{exc}</p>", status_code=502)

    expires_at = token_bundle.get("expires_at", "unknown")
    return HTMLResponse(
        "<h1>Schwab connected</h1>"
        f"<p>Tokens stored locally. Access token expires at {expires_at}.</p>"
        "<p>You can return to the Meridian dashboard now.</p>"
    )


@router.post("/api/schwab/auth/refresh")
async def schwab_refresh():
    auth_client = _require_auth_client()
    try:
        tokens = await auth_client.refresh_access_token()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {"status": "ok", "tokens": tokens}


@router.post("/api/schwab/auth/disconnect")
async def schwab_disconnect():
    auth_client = _require_auth_client()
    auth_client.disconnect()
    return {"status": "ok", "message": "Schwab tokens cleared"}
