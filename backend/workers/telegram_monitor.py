"""Telegram channel monitor — watches for new files and adds them to the queue."""

import asyncio
import re
from typing import Optional

from sqlalchemy import select

from backend.logging_config import ComponentLogger
from backend.config import TELEGRAM_CHANNEL_IDS
from backend.database import async_session
from backend.models import Job, JobStatus
from backend.services.telegram_service import telegram_service
from backend.workers.queue_manager import queue_manager

log = ComponentLogger("TelegramMonitor")

# ── Password extraction from message text ──────────────────────────────────

# Patterns ordered from most specific to least.
# Each captures the actual password in group 1.
# Detects multi-part RAR files that aren't the first volume.
# Only part1 should be queued; unrar will find the other parts automatically.
_MULTIPART_RAR_SKIP = re.compile(
    r"\.part(?!0*1\.rar)(\d+)\.rar$",       # .part2.rar, .part3.rar, etc. (skip; keep part1)
    re.IGNORECASE,
)

_PASSWORD_PATTERNS = [
    # "Password: xyz", "pass: xyz", "pw: xyz", "pwd: xyz" (with optional colon/equals)
    re.compile(
        r"(?:pass(?:word|wd|code)?|pw|пароль)\s*[:=\-–—]\s*(\S+)",
        re.IGNORECASE,
    ),
    # "password is xyz", "pass is xyz"
    re.compile(
        r"(?:pass(?:word)?|pw)\s+is\s+(\S+)",
        re.IGNORECASE,
    ),
    # Emoji key followed by the password: "🔑 xyz" / "🔐 xyz"
    re.compile(r"[🔑🔐🔓🗝️]\s*(\S+)"),
]


def _extract_password(text: str) -> Optional[str]:
    """Try to pull an archive password out of a Telegram message.

    Returns the password string or None if nothing matched.
    """
    if not text:
        return None

    for pattern in _PASSWORD_PATTERNS:
        m = pattern.search(text)
        if m:
            pw = m.group(1).strip().strip("`'\"")
            if pw:
                return pw
    return None


class TelegramMonitor:
    """Monitors configured Telegram channels for new file uploads."""

    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._processed_messages: set[tuple[int, int]] = set()

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self):
        """Start monitoring Telegram channels."""
        if self._running:
            return

        if not telegram_service.is_ready:
            log.warning("Cannot start monitor: Telegram not connected")
            return

        channel_ids = TELEGRAM_CHANNEL_IDS
        if not channel_ids:
            log.warning("No Telegram channel IDs configured")
            return

        self._running = True
        self._task = asyncio.create_task(self._monitor_loop(channel_ids))
        log.info(f"Telegram monitor started for {len(channel_ids)} channel(s)")

    async def stop(self):
        """Stop monitoring."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("Telegram monitor stopped")

    async def _monitor_loop(self, channel_ids: list[int]):
        """Polling-based channel monitor. Also registers event handler."""
        try:
            # On startup, grab the latest archive from each channel
            await self._startup_scan(channel_ids)

            # Register event handler for live updates
            await telegram_service.monitor_channels(
                channel_ids, self._on_new_file
            )

            # Keep alive
            while self._running:
                await asyncio.sleep(5)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(f"Monitor error: {e}")
            self._running = False

    async def _startup_scan(self, channel_ids: list[int]):
        """On startup, find the latest archive file from monitored channels and queue it.

        Checks the database first so restarts don't create duplicate jobs.
        """
        log.info("Startup scan: looking for latest archive in monitored channels...")

        ARCHIVE_EXTS = (".zip", ".rar", ".7z", ".tar", ".tar.gz", ".tgz")

        for channel_id in channel_ids:
            try:
                messages = await telegram_service.get_messages(channel_id, limit=20)
                for msg in messages:
                    if not msg.get("has_file") or not msg.get("file_name"):
                        continue
                    fname = msg["file_name"]
                    if not any(fname.lower().endswith(ext) for ext in ARCHIVE_EXTS):
                        continue

                    # Skip non-first parts of multi-part RAR archives
                    if _MULTIPART_RAR_SKIP.search(fname.lower()):
                        log.info(f"Startup scan: skipping multi-part RAR (not part1): {fname}")
                        continue

                    key = (channel_id, msg["id"])
                    if key in self._processed_messages:
                        continue

                    # Check if a job for this exact message already exists in the DB
                    already_exists = await self._job_exists(channel_id, msg["id"])
                    if already_exists:
                        self._processed_messages.add(key)
                        log.info(f"Startup scan: skipping {fname} (already in queue)")
                        break

                    self._processed_messages.add(key)

                    # Try to extract an archive password from the message text
                    password = _extract_password(msg.get("text", ""))
                    if password:
                        log.info(f"Startup scan found password in message for {fname}")

                    log.info(f"Startup scan found: {fname} ({msg.get('file_size', 0)} bytes)")
                    await queue_manager.add_job(
                        filename=fname,
                        file_size=msg.get("file_size", 0),
                        telegram_channel_id=channel_id,
                        telegram_message_id=msg["id"],
                        archive_password=password,
                    )
                    # Only queue the latest one per channel
                    break

            except Exception as e:
                log.warning(f"Startup scan failed for channel {channel_id}: {e}")

    async def _job_exists(self, channel_id: int, message_id: int) -> bool:
        """Check if a job for this Telegram message already exists (any status)."""
        async with async_session() as session:
            result = await session.execute(
                select(Job.id).where(
                    Job.telegram_channel_id == channel_id,
                    Job.telegram_message_id == message_id,
                ).limit(1)
            )
            return result.scalar_one_or_none() is not None

    async def _on_new_file(
        self, channel_id: int, message_id: int, filename: str, file_size: int,
        message_text: str = "",
    ):
        """Called when a new file is detected in a monitored channel."""
        key = (channel_id, message_id)
        if key in self._processed_messages:
            return
        self._processed_messages.add(key)

        # Only process archive files
        lower = filename.lower()
        if not any(lower.endswith(ext) for ext in (".zip", ".rar", ".7z", ".tar", ".tar.gz", ".tgz")):
            log.info(f"Skipping non-archive file: {filename}")
            return

        # Skip non-first parts of multi-part RAR archives
        if _MULTIPART_RAR_SKIP.search(lower):
            log.info(f"Skipping multi-part RAR (not part1): {filename}")
            return

        # Try to extract an archive password from the message text
        password = _extract_password(message_text)
        if password:
            log.info(f"Found password in message for {filename}")

        log.info(f"New file detected: {filename} ({file_size} bytes)")

        await queue_manager.add_job(
            filename=filename,
            file_size=file_size,
            telegram_channel_id=channel_id,
            telegram_message_id=message_id,
            archive_password=password,
        )


# Global singleton
telegram_monitor = TelegramMonitor()
