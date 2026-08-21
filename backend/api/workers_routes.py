"""Worker control API routes — restart, reset, emergency controls."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from backend.models import User, UserRole
from backend.api.auth_routes import get_current_user, require_role
from backend.workers.queue_manager import queue_manager
from backend.workers.watchdog import watchdog
from backend.workers.telegram_monitor import telegram_monitor
from backend.services.telegram_service import telegram_service
from backend.services.discord_service import discord_service
from backend.services.cloudflare_service import cloudflare_service
from backend.services.checker_service import checker_service
from backend.logging_config import ComponentLogger

log = ComponentLogger("Workers")
router = APIRouter(prefix="/api/workers", tags=["workers"])


class RestartRequest(BaseModel):
    worker: str  # Telegram, Downloader, Checker, Discord, Cloudflare, Queue


class TelegramStartRequest(BaseModel):
    api_id: Optional[int] = None
    api_hash: Optional[str] = None
    phone: Optional[str] = None


class TelegramCodeRequest(BaseModel):
    code: str


class Telegram2FARequest(BaseModel):
    password: str


@router.post("/restart")
async def restart_worker(
    body: RestartRequest,
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Restart a specific worker."""
    log.info(f"Worker restart requested: {body.worker} by {user.username}")

    if body.worker == "Telegram":
        await telegram_service.disconnect()
        await telegram_service.start()
        if telegram_service.is_ready:
            await telegram_monitor.start()
        return {"success": True, "message": "Telegram worker restarted"}

    elif body.worker == "Queue" or body.worker == "Downloader":
        await queue_manager.stop()
        await queue_manager.start()
        return {"success": True, "message": "Queue/Downloader restarted"}

    elif body.worker == "Checker":
        await checker_service.stop()
        return {"success": True, "message": "Checker stopped (will restart on next job)"}

    elif body.worker == "Discord":
        # Discord is stateless, just test the webhook
        ok = await discord_service.send_message("🔄 Discord connection test")
        return {"success": ok, "message": "Discord reconnected" if ok else "Discord test failed"}

    elif body.worker == "Cloudflare":
        await cloudflare_service.restart()
        return {"success": True, "message": "Cloudflare Tunnel restarted"}

    else:
        raise HTTPException(status_code=400, detail=f"Unknown worker: {body.worker}")


@router.post("/emergency-reset")
async def emergency_reset(
    user: User = Depends(require_role(UserRole.admin)),
):
    """Emergency reset — stop all workers, clean up, and restart."""
    log.critical(f"EMERGENCY RESET triggered by {user.username}")

    steps = []

    # 1. Stop workers
    try:
        await queue_manager.stop()
        steps.append("Queue stopped")
    except Exception as e:
        steps.append(f"Queue stop failed: {e}")

    try:
        await checker_service.stop()
        steps.append("Checker stopped")
    except Exception as e:
        steps.append(f"Checker stop failed: {e}")

    try:
        await telegram_monitor.stop()
        steps.append("Telegram monitor stopped")
    except Exception as e:
        steps.append(f"Telegram monitor stop failed: {e}")

    # 2. Clean temp files
    import shutil
    from backend.config import TEMP_DIR
    try:
        for item in TEMP_DIR.iterdir():
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)
        steps.append("Temp files cleaned")
    except Exception as e:
        steps.append(f"Temp cleanup failed: {e}")

    # 3. Reset stuck jobs
    from sqlalchemy import select, update
    from backend.database import async_session
    from backend.models import Job, JobStatus
    try:
        async with async_session() as session:
            result = await session.execute(
                select(Job).where(Job.status.in_([
                    JobStatus.downloading, JobStatus.extracting,
                    JobStatus.checking, JobStatus.processing_results,
                    JobStatus.sending_discord, JobStatus.cleaning_up,
                ]))
            )
            stuck_jobs = result.scalars().all()
            for job in stuck_jobs:
                job.status = JobStatus.failed
                job.error_message = "Reset by emergency reset"
            await session.commit()
            steps.append(f"Reset {len(stuck_jobs)} stuck job(s)")
    except Exception as e:
        steps.append(f"Job reset failed: {e}")

    # 4. Restart workers
    try:
        await queue_manager.start()
        steps.append("Queue restarted")
    except Exception as e:
        steps.append(f"Queue restart failed: {e}")

    if telegram_service.is_ready:
        try:
            await telegram_monitor.start()
            steps.append("Telegram monitor restarted")
        except Exception as e:
            steps.append(f"Telegram monitor restart failed: {e}")

    log.info(f"Emergency reset complete: {'; '.join(steps)}")
    return {"success": True, "steps": steps}


# ── Telegram-specific routes ────────────────────────────────────────────

@router.get("/telegram/status")
async def telegram_status(user: User = Depends(get_current_user)):
    return telegram_service.status()


@router.post("/telegram/start")
async def start_telegram(
    body: TelegramStartRequest,
    user: User = Depends(require_role(UserRole.admin)),
):
    """Start Telegram authentication."""
    await telegram_service.start(
        api_id=body.api_id or 0,
        api_hash=body.api_hash or "",
        phone=body.phone or "",
    )
    return telegram_service.status()


@router.post("/telegram/code")
async def submit_telegram_code(
    body: TelegramCodeRequest,
    user: User = Depends(require_role(UserRole.admin)),
):
    """Submit Telegram verification code."""
    telegram_service.submit_code(body.code)
    return {"success": True}


@router.post("/telegram/2fa")
async def submit_telegram_2fa(
    body: Telegram2FARequest,
    user: User = Depends(require_role(UserRole.admin)),
):
    """Submit Telegram 2FA password."""
    telegram_service.submit_2fa(body.password)
    return {"success": True}


@router.post("/telegram/disconnect")
async def disconnect_telegram(
    user: User = Depends(require_role(UserRole.admin)),
):
    """Disconnect Telegram."""
    await telegram_monitor.stop()
    await telegram_service.disconnect()
    return {"success": True}


@router.get("/telegram/channels")
async def get_telegram_channels(user: User = Depends(get_current_user)):
    """Get list of Telegram channels."""
    return await telegram_service.get_channels()


@router.get("/telegram/messages")
async def get_telegram_messages(
    channel_id: int,
    limit: int = 20,
    user: User = Depends(get_current_user),
):
    """Get messages from a Telegram channel."""
    return await telegram_service.get_messages(channel_id, limit)
