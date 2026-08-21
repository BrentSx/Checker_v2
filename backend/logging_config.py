"""Application logging with database + file + WebSocket broadcasting."""

import asyncio
import logging
import json
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from backend.config import LOG_DIR, LOG_LEVEL, LOG_MAX_SIZE_MB, LOG_BACKUP_COUNT


# In-memory log buffer for the UI (ring buffer)
_MAX_MEMORY_LOGS = 5000
_memory_logs: list[dict] = []
_log_subscribers: set[asyncio.Queue] = set()


def get_memory_logs() -> list[dict]:
    return list(_memory_logs)


def subscribe_logs() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue(maxsize=500)
    _log_subscribers.add(q)
    return q


def unsubscribe_logs(q: asyncio.Queue):
    _log_subscribers.discard(q)


def _broadcast_log(entry: dict):
    """Send a log entry to all WebSocket subscribers."""
    _memory_logs.append(entry)
    if len(_memory_logs) > _MAX_MEMORY_LOGS:
        del _memory_logs[:len(_memory_logs) - _MAX_MEMORY_LOGS]

    dead = []
    for q in _log_subscribers:
        try:
            q.put_nowait(entry)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        _log_subscribers.discard(q)


class AppLogHandler(logging.Handler):
    """Custom handler that stores logs in memory and broadcasts to WebSocket."""

    def emit(self, record: logging.LogRecord):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "component": getattr(record, "component", record.name.split(".")[-1].title()),
            "job_id": getattr(record, "job_id", None),
            "message": record.getMessage(),
        }
        _broadcast_log(entry)


def setup_logging():
    """Configure application logging."""
    root = logging.getLogger("audo_check")
    root.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # File handler
    log_file = LOG_DIR / "audo_check.log"
    fh = RotatingFileHandler(
        log_file,
        maxBytes=LOG_MAX_SIZE_MB * 1024 * 1024,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    ))
    root.addHandler(ch)

    # In-memory/WebSocket handler
    root.addHandler(AppLogHandler())

    return root


def get_logger(component: str) -> logging.Logger:
    """Get a logger for a specific component."""
    logger = logging.getLogger(f"audo_check.{component.lower()}")
    return logger


class ComponentLogger:
    """Logger wrapper that automatically includes component name and optional job_id."""

    def __init__(self, component: str, job_id: Optional[str] = None):
        self._logger = get_logger(component)
        self._component = component
        self._job_id = job_id

    def _log(self, level: int, msg: str, **kwargs):
        extra = {"component": self._component, "job_id": self._job_id}
        extra.update(kwargs)
        self._logger.log(level, msg, extra=extra)

    def debug(self, msg: str, **kw): self._log(logging.DEBUG, msg, **kw)
    def info(self, msg: str, **kw): self._log(logging.INFO, msg, **kw)
    def warning(self, msg: str, **kw): self._log(logging.WARNING, msg, **kw)
    def error(self, msg: str, **kw): self._log(logging.ERROR, msg, **kw)
    def critical(self, msg: str, **kw): self._log(logging.CRITICAL, msg, **kw)

    def with_job(self, job_id: str) -> "ComponentLogger":
        return ComponentLogger(self._component, job_id)
