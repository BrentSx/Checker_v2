"""Cloudflare Tunnel management service."""

import asyncio
import os
import shutil
import signal
from typing import Optional

from backend.config import CLOUDFLARE_DOMAIN, CLOUDFLARE_TUNNEL_TOKEN, PORT
from backend.logging_config import ComponentLogger

log = ComponentLogger("Cloudflare")


class CloudflareService:
    """Manages the cloudflared tunnel process."""

    def __init__(self):
        self._process: Optional[asyncio.subprocess.Process] = None
        self._state = "idle"  # idle, starting, connected, error, disabled
        self._error = ""
        self._monitor_task: Optional[asyncio.Task] = None
        self._restart_count = 0
        self._max_restarts = 10

    @property
    def state(self) -> str:
        return self._state

    @property
    def is_connected(self) -> bool:
        return self._state == "connected"

    def status(self) -> dict:
        return {
            "state": self._state,
            "error": self._error,
            "domain": CLOUDFLARE_DOMAIN,
            "configured": bool(CLOUDFLARE_TUNNEL_TOKEN),
            "restart_count": self._restart_count,
        }

    async def start(self):
        """Start the Cloudflare Tunnel."""
        if not CLOUDFLARE_TUNNEL_TOKEN:
            self._state = "disabled"
            self._error = "Cloudflare Tunnel token not configured"
            log.warning(self._error)
            return

        # Check if cloudflared is installed
        cloudflared_path = shutil.which("cloudflared")
        if not cloudflared_path:
            self._state = "error"
            self._error = "cloudflared binary not found. Install it first."
            log.error(self._error)
            return

        self._state = "starting"
        self._error = ""
        log.info(f"Starting Cloudflare Tunnel for {CLOUDFLARE_DOMAIN}...")

        try:
            self._process = await asyncio.create_subprocess_exec(
                cloudflared_path,
                "tunnel", "--no-autoupdate", "run",
                "--token", CLOUDFLARE_TUNNEL_TOKEN,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            # Start monitoring task
            self._monitor_task = asyncio.create_task(self._monitor())

        except Exception as e:
            self._state = "error"
            self._error = str(e)
            log.error(f"Failed to start Cloudflare Tunnel: {e}")

    async def _monitor(self):
        """Monitor the cloudflared process."""
        if not self._process:
            return

        # Give it a moment to connect
        await asyncio.sleep(3)

        if self._process.returncode is None:
            self._state = "connected"
            log.info(f"Cloudflare Tunnel connected: https://{CLOUDFLARE_DOMAIN}")

        try:
            # Read stderr for status updates
            while self._process.returncode is None:
                if self._process.stderr:
                    line = await asyncio.wait_for(
                        self._process.stderr.readline(), timeout=30
                    )
                    if line:
                        text = line.decode("utf-8", errors="replace").strip()
                        if "registered" in text.lower() or "connected" in text.lower():
                            self._state = "connected"
                        elif "error" in text.lower():
                            log.warning(f"cloudflared: {text}")
                else:
                    await asyncio.sleep(5)

        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.error(f"Cloudflare monitor error: {e}")

        # Process exited
        if self._process.returncode is not None and self._process.returncode != 0:
            self._state = "error"
            self._error = f"cloudflared exited with code {self._process.returncode}"
            log.error(self._error)

            # Auto-restart
            if self._restart_count < self._max_restarts:
                self._restart_count += 1
                log.info(f"Attempting restart ({self._restart_count}/{self._max_restarts})...")
                await asyncio.sleep(5)
                await self.start()

    async def stop(self):
        """Stop the Cloudflare Tunnel."""
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

        if self._process and self._process.returncode is None:
            try:
                self._process.terminate()
                await asyncio.wait_for(self._process.wait(), timeout=10)
            except (asyncio.TimeoutError, ProcessLookupError):
                try:
                    self._process.kill()
                except ProcessLookupError:
                    pass

        self._state = "idle"
        self._process = None
        log.info("Cloudflare Tunnel stopped")

    async def restart(self):
        """Restart the Cloudflare Tunnel."""
        log.info("Restarting Cloudflare Tunnel...")
        await self.stop()
        await asyncio.sleep(2)
        self._restart_count = 0
        await self.start()


# Global singleton
cloudflare_service = CloudflareService()
