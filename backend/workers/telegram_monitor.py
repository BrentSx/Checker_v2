"""Telegram channel monitor — watches for new files and adds them to the queue."""

import asyncio
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
        """On startup, find ALL unprocessed archive files from monitored channels.

        Checks the database first so restarts don't create duplicate jobs.
        Queues every archive that doesn't already have a job record, not just
        the latest — so nothing gets missed after a restart.
        """
        log.info("Startup scan: looking for unprocessed archives in monitored channels...")

        ARCHIVE_EXTS = (".zip", ".rar", ".7z", ".tar", ".tar.gz", ".tgz")
        queued_count = 0

        for channel_id in channel_ids:
            try:
                messages = await telegram_service.get_messages(channel_id, limit=50)
                for idx, msg in enumerate(messages):
                    if not msg.get("has_file") or not msg.get("file_name"):
                        continue
                    fname = msg["file_name"]
                    if not any(fname.lower().endswith(ext) for ext in ARCHIVE_EXTS):
                        continue

                    key = (channel_id, msg["id"])
                    if key in self._processed_messages:
                        continue

                    # Check if a job for this exact message already exists in the DB
                    already_exists = await self._job_exists(channel_id, msg["id"])
                    if already_exists:
                        self._processed_messages.add(key)
                        continue

                    self._processed_messages.add(key)

                    # Try to extract an archive password from the message text.
                    # If the file message has no password, check the 2 messages
                    # before and after it (passwords are often posted separately).
                    password = _extract_password(msg.get("text", ""))
                    if not password:
                        password = self._find_nearby_password(messages, idx)
                    if password:
                        log.info(f"Startup scan found password for {fname}")

                    log.info(f"Startup scan found: {fname} ({msg.get('file_size', 0)} bytes)")
                    await queue_manager.add_job(
                        filename=fname,
                        file_size=msg.get("file_size", 0),
                        telegram_channel_id=channel_id,
                        telegram_message_id=msg["id"],
                        archive_password=password,
                    )
                    queued_count += 1

            except Exception as e:
                log.warning(f"Startup scan failed for channel {channel_id}: {e}")

        if queued_count > 0:
            log.info(f"Startup scan queued {queued_count} archive(s)")
        else:
            log.info("Startup scan: all archives already processed")

    @staticmethod
    def _find_nearby_password(messages: list[dict], file_idx: int) -> Optional[str]:
        """Search the 2 messages before and after a file message for a password.

        Passwords are often posted in a separate message adjacent to the file.
        """
        for offset in (-1, 1, -2, 2):
            neighbor_idx = file_idx + offset
            if 0 <= neighbor_idx < len(messages):
                pw = _extract_password(messages[neighbor_idx].get("text", ""))
                if pw:
                    return pw
        return None

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

        # Cap the in-memory set so it doesn't grow without bound over weeks
        if len(self._processed_messages) > 10000:
            # Keep the most recent half (arbitrary trim — DB check below is the real guard)
            excess = len(self._processed_messages) - 5000
            for _ in range(excess):
                self._processed_messages.pop()

        # Only process archive files
        lower = filename.lower()
        if not any(lower.endswith(ext) for ext in (".zip", ".rar", ".7z", ".tar", ".tar.gz", ".tgz")):
            return

        # Check DB for duplicates — the in-memory set is empty after restart,
        # so a live event for an already-queued file would slip through without this.
        if await self._job_exists(channel_id, message_id):
            log.info(f"Skipping {filename} (already in queue)")
            return

        # Try to extract an archive password from the message text.
        # If not found, check the 2 messages before (the file message is newest,
        # so the password was likely posted just before it).
        password = _extract_password(message_text)
        if not password:
            try:
                nearby = await telegram_service.get_messages(channel_id, limit=5)
                # Find the file message index in the nearby list
                file_idx = next(
                    (i for i, m in enumerate(nearby) if m["id"] == message_id), -1
                )
                if file_idx >= 0:
                    password = self._find_nearby_password(nearby, file_idx)
            except Exception:
                pass
        if password:
            log.info(f"Found password for {filename}")

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
