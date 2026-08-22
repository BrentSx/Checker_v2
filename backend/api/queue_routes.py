"""Queue management API routes."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from typing import Optional

from backend.database import async_session
from backend.models import User, UserRole, Setting
from backend.api.auth_routes import get_current_user, require_role
from backend.workers.queue_manager import queue_manager

router = APIRouter(prefix="/api/queue", tags=["queue"])


class AddJobRequest(BaseModel):
    filename: str
    file_size: int = 0
    telegram_channel_id: Optional[int] = None
    telegram_message_id: Optional[int] = None
    source_url: Optional[str] = None


@router.get("/")
async def get_queue(user: User = Depends(get_current_user)):
    """Get the current queue."""
    return await queue_manager.get_queue()


@router.post("/add")
async def add_job(
    body: AddJobRequest,
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Add a job to the queue."""
    job_id = await queue_manager.add_job(
        filename=body.filename,
        file_size=body.file_size,
        telegram_channel_id=body.telegram_channel_id,
        telegram_message_id=body.telegram_message_id,
        source_url=body.source_url,
    )
    return {"success": True, "job_id": job_id}


@router.post("/pause")
async def pause_queue(
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Pause queue processing."""
    queue_manager.pause()
    return {"success": True}


@router.post("/resume")
async def resume_queue(
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Resume queue processing."""
    queue_manager.resume()
    return {"success": True}


@router.post("/retry/{job_id}")
async def retry_job(
    job_id: str,
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Retry a failed job."""
    await queue_manager.retry_job(job_id)
    return {"success": True}


@router.post("/cancel/{job_id}")
async def cancel_job(
    job_id: str,
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Cancel a queued job."""
    await queue_manager.cancel_job(job_id)
    return {"success": True}


@router.post("/retest/{job_id}")
async def retest_job(
    job_id: str,
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Retest a job — skip download, run extract→check→discord on the file already on disk."""
    ok = await queue_manager.retest_job(job_id)
    if not ok:
        raise HTTPException(status_code=400, detail="No downloaded file on disk for this job")
    return {"success": True}


@router.post("/clear-completed")
async def clear_completed(
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Clear all completed/cancelled jobs."""
    await queue_manager.clear_completed()
    return {"success": True}


# ── Keep-downloads toggle ──────────────────────────────────────────────────

@router.get("/keep-downloads")
async def get_keep_downloads(user: User = Depends(get_current_user)):
    """Get the current keep-downloads setting."""
    async with async_session() as session:
        result = await session.execute(
            select(Setting).where(Setting.key == "keep_downloads")
        )
        setting = result.scalar_one_or_none()
        return {"enabled": setting.value == "true" if setting else False}


@router.post("/keep-downloads")
async def toggle_keep_downloads(
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Toggle the keep-downloads setting."""
    async with async_session() as session:
        result = await session.execute(
            select(Setting).where(Setting.key == "keep_downloads")
        )
        setting = result.scalar_one_or_none()
        if setting:
            new_val = "false" if setting.value == "true" else "true"
            setting.value = new_val
        else:
            new_val = "true"
            session.add(Setting(key="keep_downloads", value=new_val))
        await session.commit()
        return {"enabled": new_val == "true"}
