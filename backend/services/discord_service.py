"""Discord webhook service for sending check results."""

import asyncio
import io
import json
import math
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import ssl

import aiohttp
import certifi

from backend.config import DISCORD_WEBHOOK_URL, DISCORD_MAX_FILE_SIZE_MB
from backend.logging_config import ComponentLogger

log = ComponentLogger("Discord")

MAX_FILE_BYTES = DISCORD_MAX_FILE_SIZE_MB * 1024 * 1024


class DiscordService:
    """Sends results and notifications to Discord via webhook."""

    def __init__(self):
        self._webhook_url = DISCORD_WEBHOOK_URL
        self._last_success: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._messages_sent = 0
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # Use certifi CA bundle — CentOS 7 custom Python can't find system certs
            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_ctx)
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    def configure(self, webhook_url: str):
        self._webhook_url = webhook_url

    def status(self) -> dict:
        return {
            "configured": bool(self._webhook_url),
            "last_success": self._last_success.isoformat() if self._last_success else None,
            "last_error": self._last_error,
            "messages_sent": self._messages_sent,
        }

    async def send_message(self, content: str, embeds: list[dict] | None = None) -> bool:
        """Send a text message to Discord."""
        if not self._webhook_url:
            log.warning("Discord webhook not configured")
            return False

        try:
            session = await self._get_session()
            payload = {}
            if content:
                payload["content"] = content[:2000]  # Discord limit
            if embeds:
                payload["embeds"] = embeds[:10]  # Discord limit

            async with session.post(self._webhook_url, json=payload) as resp:
                if resp.status in (200, 204):
                    self._last_success = datetime.now(timezone.utc)
                    self._messages_sent += 1
                    self._last_error = None
                    return True
                else:
                    body = await resp.text()
                    self._last_error = f"HTTP {resp.status}: {body[:200]}"
                    log.error(f"Discord webhook failed: {self._last_error}")
                    return False

        except Exception as e:
            self._last_error = str(e)
            log.error(f"Discord send failed: {e}")
            return False

    async def send_embed(
        self,
        title: str,
        description: str = "",
        color: int = 0x3B82F6,
        fields: list[dict] | None = None,
        footer: str = "",
    ) -> bool:
        """Send an embed message."""
        embed = {
            "title": title,
            "description": description,
            "color": color,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if fields:
            embed["fields"] = fields
        if footer:
            embed["footer"] = {"text": footer}

        return await self.send_message("", embeds=[embed])

    async def send_file(self, file_data: bytes, filename: str, content: str = "") -> bool:
        """Send a file to Discord. Splits if larger than DISCORD_MAX_FILE_SIZE_MB (default 8)."""
        if not self._webhook_url:
            log.warning("Discord webhook not configured")
            return False

        if len(file_data) <= MAX_FILE_BYTES:
            return await self._upload_file(file_data, filename, content)

        # Split into parts
        num_parts = math.ceil(len(file_data) / MAX_FILE_BYTES)
        log.info(f"Large file detected ({len(file_data)} bytes). Splitting into {num_parts} parts.")

        await self.send_message(
            f"📦 **Large result detected**\n"
            f"Original: `{filename}` — {len(file_data) / (1024*1024):.1f} MB\n"
            f"Sending: {num_parts} part(s)"
        )

        for i in range(num_parts):
            start = i * MAX_FILE_BYTES
            end = min(start + MAX_FILE_BYTES, len(file_data))
            part_data = file_data[start:end]
            part_name = f"{Path(filename).stem}.part{i+1:03d}"
            part_content = f"Part {i+1}/{num_parts}"

            ok = await self._upload_file(part_data, part_name, part_content)
            if not ok:
                log.error(f"Failed to send part {i+1}/{num_parts}")
                return False

            # Rate limiting between parts
            if i < num_parts - 1:
                await asyncio.sleep(1)

        return True

    async def _upload_file(self, data: bytes, filename: str, content: str = "") -> bool:
        """Upload a single file to Discord webhook."""
        try:
            session = await self._get_session()
            form = aiohttp.FormData()
            form.add_field("file", io.BytesIO(data), filename=filename)
            if content:
                form.add_field("content", content[:2000])

            async with session.post(self._webhook_url, data=form) as resp:
                if resp.status in (200, 204):
                    self._last_success = datetime.now(timezone.utc)
                    self._messages_sent += 1
                    self._last_error = None
                    return True
                else:
                    body = await resp.text()
                    self._last_error = f"HTTP {resp.status}: {body[:200]}"
                    log.error(f"Discord file upload failed: {self._last_error}")
                    return False

        except Exception as e:
            self._last_error = str(e)
            log.error(f"Discord file upload error: {e}")
            return False

    async def send_job_result(
        self,
        filename: str,
        job_id: str,
        status: str,
        results_summary: dict,
        processing_time: str,
        results_zip_data: Optional[bytes] = None,
    ) -> bool:
        """Send a job completion notification with results."""
        color = 0x22C55E if status == "completed" else 0xEF4444  # green or red

        fields = [
            {"name": "File", "value": f"`{filename}`", "inline": True},
            {"name": "Status", "value": status.upper(), "inline": True},
            {"name": "Job ID", "value": f"`{job_id[:8]}`", "inline": True},
            {"name": "Processing Time", "value": processing_time, "inline": True},
        ]

        if results_summary:
            total = results_summary.get("total", 0)
            valid = results_summary.get("valid_mc", 0)
            fields.append({"name": "Files Checked", "value": f"{total:,}", "inline": True})
            fields.append({"name": "Valid MC", "value": f"{valid:,}", "inline": True})

            # Add ban check results if available
            for key, label in [
                ("hyp_unbanned", "Hyp Unbanned"),
                ("hyp_banned", "Hyp Banned"),
                ("don_unbanned", "Don Unbanned"),
                ("don_banned", "Don Banned"),
            ]:
                val = results_summary.get(key, 0)
                if val > 0:
                    fields.append({"name": label, "value": f"{val:,}", "inline": True})

        ok = await self.send_embed(
            title="✅ CHECK COMPLETE" if status == "completed" else "❌ CHECK FAILED",
            color=color,
            fields=fields,
            footer="Snickerdoodle Checker",
        )

        # Send results file if available
        if ok and results_zip_data:
            zip_name = f"results_{job_id[:8]}.zip"
            ok = await self.send_file(results_zip_data, zip_name)

        return ok

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# Global singleton
discord_service = DiscordService()
