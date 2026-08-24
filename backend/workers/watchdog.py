"""Watchdog service that monitors all workers and auto-restarts failed components."""

import asyncio
from datetime import datetime, timezone
from typing import Optional

from backend.logging_config import ComponentLogger
from backend.websocket_hub import hub

log = ComponentLogger("Watchdog")


class WorkerHealth:
    """Tracks the health of a single worker/service."""

    def __init__(self, name: str):
        self.name = name
        self.healthy = True
        self.last_check: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.restart_count = 0
        self.consecutive_failures = 0

    def mark_healthy(self):
        self.healthy = True
        self.last_check = datetime.now(timezone.utc)
        self.last_error = None
        self.consecutive_failures = 0

    def mark_unhealthy(self, error: str):
        self.healthy = False
        self.last_check = datetime.now(timezone.utc)
        self.last_error = error
        self.consecutive_failures += 1

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "healthy": self.healthy,
            "last_check": self.last_check.isoformat() if self.last_check else None,
            "last_error": self.last_error,
            "restart_count": self.restart_count,
            "consecutive_failures": self.consecutive_failures,
        }


class Watchdog:
    """Monitors all workers and services, auto-restarts failed components."""

    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._check_interval = 15  # seconds
        self._max_consecutive_failures = 3
        self._workers: dict[str, WorkerHealth] = {}

        # Register known workers
        for name in [
            "Telegram", "Downloader", "Queue", "Checker",
            "Discord", "Cloudflare", "Database",
        ]:
            self._workers[name] = WorkerHealth(name)

    def status(self) -> dict:
        return {
            "running": self._running,
            "workers": {k: v.to_dict() for k, v in self._workers.items()},
        }

    async def start(self):
        """Start the watchdog monitoring loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop())
        log.info("Watchdog started")

    async def stop(self):
        """Stop the watchdog."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        log.info("Watchdog stopped")

    async def _monitor_loop(self):
        """Main monitoring loop."""
        while self._running:
            try:
                await self._check_all()
                await asyncio.sleep(self._check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Watchdog error: {e}")
                await asyncio.sleep(self._check_interval)

    async def _check_all(self):
        """Check all workers."""
        from backend.services.telegram_service import telegram_service
        from backend.services.discord_service import discord_service
        from backend.services.cloudflare_service import cloudflare_service
        from backend.services.checker_service import checker_service
        from backend.workers.queue_manager import queue_manager

        # Check Telegram
        tg = self._workers["Telegram"]
        if telegram_service.state == "ready":
            tg.mark_healthy()
        elif telegram_service.state == "error":
            tg.mark_unhealthy(telegram_service.error)
        elif telegram_service.state == "idle":
            tg.healthy = True  # Not configured but not broken
            tg.last_check = datetime.now(timezone.utc)

        # Check Queue/Downloader
        q = self._workers["Queue"]
        dl = self._workers["Downloader"]
        if queue_manager.is_running:
            q.mark_healthy()
            dl.mark_healthy()
        else:
            q.mark_unhealthy("Queue manager not running")
            dl.mark_unhealthy("Queue manager not running")

        # Check Checker
        ck = self._workers["Checker"]
        ck.mark_healthy()  # Checker is healthy unless it crashes during a run

        # Check Discord
        dc = self._workers["Discord"]
        ds = discord_service.status()
        if ds["configured"]:
            last_err = ds["last_error"] or ""
            # 413 = "file too large", 429 = "rate limited" — neither is a
            # connectivity problem.  The webhook itself works fine.
            if last_err and "413" not in last_err and "429" not in last_err:
                dc.mark_unhealthy(last_err)
            else:
                dc.mark_healthy()
        else:
            dc.healthy = True
            dc.last_check = datetime.now(timezone.utc)

        # Check Cloudflare
        cf = self._workers["Cloudflare"]
        cfs = cloudflare_service.status()
        if cfs["state"] == "connected":
            cf.mark_healthy()
        elif cfs["state"] == "error":
            cf.mark_unhealthy(cfs["error"])
        elif cfs["state"] == "disabled":
            cf.healthy = True
            cf.last_check = datetime.now(timezone.utc)
        else:
            cf.last_check = datetime.now(timezone.utc)

        # Check Database
        db = self._workers["Database"]
        try:
            from backend.database import async_session
            from sqlalchemy import text
            async with async_session() as session:
                await session.execute(text("SELECT 1"))
            db.mark_healthy()
        except Exception as e:
            db.mark_unhealthy(str(e))

        # Auto-restart failed workers
        for name, health in self._workers.items():
            if not health.healthy and health.consecutive_failures >= self._max_consecutive_failures:
                await self._auto_restart(name, health)

        # Broadcast health status
        await hub.broadcast("health", {
            k: v.to_dict() for k, v in self._workers.items()
        })

    async def _auto_restart(self, name: str, health: WorkerHealth):
        """Attempt to auto-restart a failed worker.

        Caps at 5 auto-restarts per worker. After that, logs a warning once
        and stops retrying until the worker is manually restarted or a restart
        clears the counter.
        """
        MAX_AUTO_RESTARTS = 5

        if health.restart_count >= MAX_AUTO_RESTARTS:
            # Log once when we first hit the threshold, then stay silent
            if health.consecutive_failures == self._max_consecutive_failures:
                log.error(
                    f"{name} has hit {MAX_AUTO_RESTARTS} auto-restart cap. "
                    f"Not retrying — fix the issue and restart manually."
                )
            # Do NOT reset consecutive_failures — let it keep climbing so this
            # branch only logs once (when it exactly equals _max_consecutive_failures).
            return

        log.warning(f"{name} has {health.consecutive_failures} consecutive failures. Attempting restart...")

        try:
            if name == "Telegram":
                from backend.services.telegram_service import telegram_service
                from backend.workers.telegram_monitor import telegram_monitor
                await telegram_monitor.stop()
                await telegram_service.disconnect()
                await asyncio.sleep(2)
                await telegram_service.start()
                if telegram_service.is_ready:
                    await telegram_monitor.start()

            elif name == "Queue" or name == "Downloader":
                from backend.workers.queue_manager import queue_manager
                await queue_manager.stop()
                await asyncio.sleep(2)
                await queue_manager.start()

            elif name == "Cloudflare":
                from backend.services.cloudflare_service import cloudflare_service
                await cloudflare_service.restart()

            elif name == "Discord":
                # Discord is stateless — just close and recreate the session
                from backend.services.discord_service import discord_service
                await discord_service.close()
                # Session will be recreated on next send

            health.restart_count += 1
            health.consecutive_failures = 0
            log.info(f"{name} restart attempted (#{health.restart_count})")

        except Exception as e:
            log.error(f"Failed to restart {name}: {e}")

    async def restart_worker(self, name: str) -> bool:
        """Manually restart a specific worker.

        Unlike auto-restart, manual restarts always proceed and reset the
        restart counter so the worker gets a fresh set of auto-restart attempts.
        """
        if name not in self._workers:
            return False
        health = self._workers[name]
        # Reset counters so manual restart isn't blocked by the auto-restart cap
        health.restart_count = 0
        health.consecutive_failures = 0
        await self._auto_restart(name, health)
        return True


# Global singleton
watchdog = Watchdog()
