"""Telegram channel monitor — watches for new files and adds them to the queue."""

import asyncio
from typing import Optional

from backend.logging_config import ComponentLogger
from backend.config import TELEGRAM_CHANNEL_IDS
from backend.services.telegram_service import telegram_service
from backend.workers.queue_manager import queue_manager

log = ComponentLogger("TelegramMonitor")


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

    async def _on_new_file(
        self, channel_id: int, message_id: int, filename: str, file_size: int
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

        log.info(f"New file detected: {filename} ({file_size} bytes)")

        await queue_manager.add_job(
            filename=filename,
            file_size=file_size,
            telegram_channel_id=channel_id,
            telegram_message_id=message_id,
        )


# Global singleton
telegram_monitor = TelegramMonitor()
