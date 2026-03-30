from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(tags=["dashboard"])

_DASHBOARD_PATH = Path(__file__).resolve().parent.parent / "static" / "dashboard.html"


@router.get("/dashboard", include_in_schema=False)
async def dashboard():
    return FileResponse(_DASHBOARD_PATH)
