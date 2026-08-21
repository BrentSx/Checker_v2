"""Queue management API routes."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional

from backend.models import User, UserRole
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


@router.post("/clear-completed")
async def clear_completed(
    user: User = Depends(require_role(UserRole.admin, UserRole.operator)),
):
    """Clear all completed/cancelled jobs."""
    await queue_manager.clear_completed()
    return {"success": True}
