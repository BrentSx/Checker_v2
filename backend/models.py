"""SQLAlchemy ORM models."""

import enum
import time
import uuid as _uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column, String, Integer, Float, Text, Boolean, DateTime, Enum, JSON, BigInteger,
    func, Index,
)
from sqlalchemy.orm import Mapped, mapped_column

from backend.database import Base


def _utcnow():
    return datetime.now(timezone.utc)


def _uuid4():
    return str(_uuid.uuid4())


# ── Enums ───────────────────────────────────────────────────────────────────

class UserRole(str, enum.Enum):
    admin = "admin"
    operator = "operator"
    viewer = "viewer"


class JobStatus(str, enum.Enum):
    queued = "queued"
    downloading = "downloading"
    downloaded = "downloaded"
    extracting = "extracting"
    checking = "checking"
    processing_results = "processing_results"
    sending_discord = "sending_discord"
    cleaning_up = "cleaning_up"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"
    waiting = "waiting"
    paused = "paused"


# ── User ────────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    role: Mapped[UserRole] = mapped_column(Enum(UserRole), nullable=False, default=UserRole.viewer)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    last_login: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


# ── Job ─────────────────────────────────────────────────────────────────────

class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid4)
    filename: Mapped[str] = mapped_column(String(512), nullable=False)
    file_size: Mapped[int] = mapped_column(BigInteger, default=0)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus), default=JobStatus.queued, index=True)
    progress: Mapped[float] = mapped_column(Float, default=0.0)
    queue_position: Mapped[int] = mapped_column(Integer, default=0)

    # Source info
    telegram_channel_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    telegram_message_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    # Download tracking
    downloaded_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    download_speed: Mapped[int] = mapped_column(BigInteger, default=0)  # bytes/sec
    download_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)

    # Extraction
    extract_dir: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    files_extracted: Mapped[int] = mapped_column(Integer, default=0)

    # Checker results
    files_checked: Mapped[int] = mapped_column(Integer, default=0)
    files_total: Mapped[int] = mapped_column(Integer, default=0)
    results_summary: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # Timing
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Error handling
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=3)

    # Flags
    discord_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    cleanup_done: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (
        Index("ix_jobs_created", "created_at"),
        Index("ix_jobs_queue_pos", "queue_position"),
    )


# ── Setting ─────────────────────────────────────────────────────────────────

class Setting(Base):
    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False, default="")
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)


# ── LogEntry ────────────────────────────────────────────────────────────────

class LogEntry(Base):
    __tablename__ = "log_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    level: Mapped[str] = mapped_column(String(16), nullable=False, default="INFO", index=True)
    component: Mapped[str] = mapped_column(String(32), nullable=False, default="System", index=True)
    job_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    message: Mapped[str] = mapped_column(Text, nullable=False)


# ── Statistic ───────────────────────────────────────────────────────────────

class Statistic(Base):
    __tablename__ = "statistics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[str] = mapped_column(String(10), nullable=False, index=True)  # YYYY-MM-DD
    jobs_completed: Mapped[int] = mapped_column(Integer, default=0)
    jobs_failed: Mapped[int] = mapped_column(Integer, default=0)
    files_checked: Mapped[int] = mapped_column(Integer, default=0)
    data_downloaded_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    data_processed_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    discord_messages_sent: Mapped[int] = mapped_column(Integer, default=0)
    total_processing_seconds: Mapped[float] = mapped_column(Float, default=0.0)

    __table_args__ = (
        Index("ix_stats_date", "date", unique=True),
    )
