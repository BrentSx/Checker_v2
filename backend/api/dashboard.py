"""Dashboard API routes — live system status and health."""

import psutil
from datetime import datetime, timezone
from fastapi import APIRouter, Depends

from backend.models import User, UserRole
from backend.api.auth_routes import get_current_user
from backend.services.telegram_service import telegram_service
from backend.services.discord_service import discord_service
from backend.services.cloudflare_service import cloudflare_service
from backend.services.checker_service import checker_service
from backend.workers.queue_manager import queue_manager
from backend.workers.watchdog import watchdog
from backend.websocket_hub import hub

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

_start_time = datetime.now(timezone.utc)


@router.get("/status")
async def get_dashboard_status(user: User = Depends(get_current_user)):
    """Get the full dashboard status."""
    queue = await queue_manager.get_queue()
    queued_count = sum(1 for j in queue if j["status"] == "queued")
    active_job = None
    for j in queue:
        if j["status"] not in ("queued", "completed", "failed", "cancelled"):
            active_job = j
            break

    return {
        "telegram": telegram_service.status(),
        "discord": discord_service.status(),
        "cloudflare": cloudflare_service.status(),
        "checker": checker_service.status(),
        "queue": {
            **queue_manager.status(),
            "total": len(queue),
            "queued": queued_count,
            "completed": sum(1 for j in queue if j["status"] == "completed"),
            "failed": sum(1 for j in queue if j["status"] == "failed"),
        },
        "watchdog": watchdog.status(),
        "current_job": active_job,
        "system": _system_info(),
        "websocket_clients": hub.client_count,
    }


@router.get("/health")
async def health_check():
    """Public health check endpoint (no auth required)."""
    from backend.database import async_session
    from sqlalchemy import text

    health = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": {},
    }

    # Database
    try:
        async with async_session() as session:
            await session.execute(text("SELECT 1"))
        health["components"]["database"] = {"status": "healthy"}
    except Exception as e:
        health["components"]["database"] = {"status": "unhealthy", "error": str(e)}
        health["status"] = "degraded"

    # Telegram
    tg_status = telegram_service.status()
    if tg_status["state"] == "ready":
        health["components"]["telegram"] = {"status": "healthy"}
    elif tg_status["state"] == "error":
        health["components"]["telegram"] = {"status": "unhealthy", "error": tg_status["error"]}
    else:
        health["components"]["telegram"] = {"status": tg_status["state"]}

    # Discord
    ds_status = discord_service.status()
    health["components"]["discord"] = {
        "status": "healthy" if ds_status["configured"] and not ds_status["last_error"] else
                  "unhealthy" if ds_status["last_error"] else "not_configured"
    }

    # Cloudflare
    cf_status = cloudflare_service.status()
    health["components"]["cloudflare"] = {"status": cf_status["state"]}

    # Queue
    health["components"]["queue"] = {
        "status": "healthy" if queue_manager.is_running else "stopped"
    }

    # Disk
    try:
        disk = psutil.disk_usage("/")
        disk_pct = disk.percent
        health["components"]["disk"] = {
            "status": "healthy" if disk_pct < 90 else "warning" if disk_pct < 95 else "critical",
            "usage_percent": disk_pct,
        }
    except Exception:
        health["components"]["disk"] = {"status": "unknown"}

    # System
    health["components"]["system"] = {
        "status": "healthy",
        "uptime": str(datetime.now(timezone.utc) - _start_time),
    }

    # Overall status
    if any(c["status"] == "unhealthy" for c in health["components"].values()):
        health["status"] = "degraded"

    return health


def _system_info() -> dict:
    """Get system resource information."""
    try:
        cpu_pct = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        uptime = datetime.now(timezone.utc) - _start_time

        return {
            "cpu_percent": cpu_pct,
            "memory_total": mem.total,
            "memory_used": mem.used,
            "memory_percent": mem.percent,
            "disk_total": disk.total,
            "disk_used": disk.used,
            "disk_free": disk.free,
            "disk_percent": disk.percent,
            "uptime_seconds": uptime.total_seconds(),
        }
    except Exception:
        return {}
