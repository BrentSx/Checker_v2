"""Log viewer API routes."""

import os
from fastapi import APIRouter, Depends, Query
from fastapi.responses import FileResponse
from typing import Optional

from backend.models import User, UserRole
from backend.api.auth_routes import get_current_user, require_role
from backend.logging_config import get_memory_logs
from backend.config import LOG_DIR

router = APIRouter(prefix="/api/logs", tags=["logs"])


@router.get("/")
async def get_logs(
    level: Optional[str] = None,
    component: Optional[str] = None,
    job_id: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(default=200, le=5000),
    user: User = Depends(get_current_user),
):
    """Get recent log entries from memory."""
    logs = get_memory_logs()

    # Apply filters
    if level:
        level_upper = level.upper()
        logs = [l for l in logs if l["level"] == level_upper]

    if component:
        logs = [l for l in logs if l["component"].lower() == component.lower()]

    if job_id:
        logs = [l for l in logs if l.get("job_id") == job_id]

    if search:
        search_lower = search.lower()
        logs = [l for l in logs if search_lower in l["message"].lower()]

    # Return most recent entries
    return logs[-limit:]


@router.post("/clear")
async def clear_logs(
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Clear in-memory logs."""
    from backend.logging_config import _memory_logs
    _memory_logs.clear()
    return {"success": True}


@router.get("/download")
async def download_logs(
    user: User = Depends(require_role(UserRole.admin)),
):
    """Download the log file."""
    log_file = LOG_DIR / "audo_check.log"
    if not log_file.exists():
        return {"error": "No log file found"}

    return FileResponse(
        path=str(log_file),
        filename="audo_check.log",
        media_type="text/plain",
    )


@router.get("/components")
async def get_log_components(user: User = Depends(get_current_user)):
    """Get unique component names from logs."""
    logs = get_memory_logs()
    components = sorted(set(l["component"] for l in logs))
    return components
