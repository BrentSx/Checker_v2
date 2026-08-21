"""Statistics API routes."""

from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone, timedelta

from backend.database import get_db
from backend.models import User, Job, JobStatus, Statistic
from backend.api.auth_routes import get_current_user

router = APIRouter(prefix="/api/stats", tags=["statistics"])


@router.get("/")
async def get_stats(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get overall statistics."""
    # Today's stats
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    result = await db.execute(select(Statistic).where(Statistic.date == today))
    today_stat = result.scalar_one_or_none()

    # All-time totals
    result = await db.execute(
        select(
            func.sum(Statistic.jobs_completed),
            func.sum(Statistic.jobs_failed),
            func.sum(Statistic.files_checked),
            func.sum(Statistic.data_downloaded_bytes),
            func.sum(Statistic.data_processed_bytes),
            func.sum(Statistic.discord_messages_sent),
            func.sum(Statistic.total_processing_seconds),
        )
    )
    totals = result.one()

    total_completed = totals[0] or 0
    total_failed = totals[1] or 0
    total_files = totals[2] or 0
    total_downloaded = totals[3] or 0
    total_processed = totals[4] or 0
    total_discord = totals[5] or 0
    total_seconds = totals[6] or 0

    avg_time = total_seconds / total_completed if total_completed > 0 else 0

    return {
        "today": {
            "jobs_completed": today_stat.jobs_completed if today_stat else 0,
            "jobs_failed": today_stat.jobs_failed if today_stat else 0,
            "files_checked": today_stat.files_checked if today_stat else 0,
            "data_downloaded": today_stat.data_downloaded_bytes if today_stat else 0,
            "discord_messages": today_stat.discord_messages_sent if today_stat else 0,
        },
        "total": {
            "jobs_completed": total_completed,
            "jobs_failed": total_failed,
            "files_checked": total_files,
            "data_downloaded": total_downloaded,
            "data_processed": total_processed,
            "discord_messages": total_discord,
            "average_processing_seconds": avg_time,
        },
    }


@router.get("/history")
async def get_stats_history(
    days: int = 30,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get daily statistics for the last N days."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    result = await db.execute(
        select(Statistic)
        .where(Statistic.date >= cutoff)
        .order_by(Statistic.date)
    )
    stats = result.scalars().all()

    return [
        {
            "date": s.date,
            "jobs_completed": s.jobs_completed,
            "jobs_failed": s.jobs_failed,
            "files_checked": s.files_checked,
            "data_downloaded": s.data_downloaded_bytes,
            "discord_messages": s.discord_messages_sent,
            "avg_processing_time": (
                s.total_processing_seconds / s.jobs_completed
                if s.jobs_completed > 0 else 0
            ),
        }
        for s in stats
    ]
