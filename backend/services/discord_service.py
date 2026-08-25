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

# Discord rejects webhook uploads whose *whole request* exceeds ~8 MiB on a
# non-boosted server. Leaving headroom for multipart overhead, 7 MiB payloads
# pass reliably on the first try. Clamp the configured value to this ceiling so
# a too-high DISCORD_MAX_FILE_SIZE_MB can't cause repeated 413s.
WEBHOOK_SAFE_BYTES = 7 * 1024 * 1024
MAX_FILE_BYTES = min(DISCORD_MAX_FILE_SIZE_MB * 1024 * 1024, WEBHOOK_SAFE_BYTES)


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
            # Per-request timeout only — the session itself stays open across
            # multi-part uploads that can take minutes with rate-limit sleeps.
            # The old `total=30` killed the session mid-upload on large results.
            timeout = aiohttp.ClientTimeout(
                total=None,        # no session-wide cap
                sock_connect=15,   # 15s to establish connection
                sock_read=30,      # 30s for Discord to respond per request
            )
            self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)
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
        """Send a text message to Discord.

        Automatically retries on 429 (rate limit) and transient network errors.
        """
        if not self._webhook_url:
            log.warning("Discord webhook not configured")
            return False

        max_retries = 5
        for attempt in range(max_retries + 1):
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

                    if resp.status == 429:
                        body_text = await resp.text()
                        retry_after = 5
                        try:
                            body_json = json.loads(body_text)
                            retry_after = float(body_json.get("retry_after", 5))
                        except (json.JSONDecodeError, ValueError):
                            pass
                        retry_after = min(retry_after, 60)
                        log.warning(f"Discord rate limited (429), waiting {retry_after:.1f}s")
                        await asyncio.sleep(retry_after)
                        continue

                    if resp.status >= 500 and attempt < max_retries:
                        # Discord server error — retry after backoff
                        wait = 2 ** attempt
                        log.warning(f"Discord server error ({resp.status}), retrying in {wait}s...")
                        await asyncio.sleep(wait)
                        continue

                    body = await resp.text()
                    self._last_error = f"HTTP {resp.status}: {body[:200]}"
                    log.error(f"Discord webhook failed: {self._last_error}")
                    return False

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < max_retries:
                    wait = 2 ** attempt
                    log.warning(f"Discord network error ({e}), retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                self._last_error = str(e)
                log.error(f"Discord send failed after {max_retries + 1} attempts: {e}")
                return False
            except Exception as e:
                self._last_error = str(e)
                log.error(f"Discord send failed: {e}")
                return False

        self._last_error = "Failed after max retries"
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
        """Send a file to Discord. Splits if larger than MAX_FILE_BYTES (default 8 MB).

        If a part still gets rejected with 413, halves the chunk size and retries.
        """
        if not self._webhook_url:
            log.warning("Discord webhook not configured")
            return False

        if len(file_data) <= MAX_FILE_BYTES:
            ok = await self._upload_file(file_data, filename, content)
            if ok:
                return True
            # If the single-chunk upload got 413, fall through to splitting
            if self._last_error and "413" in self._last_error:
                log.warning("Single-file upload got 413, will split smaller...")
            else:
                return False

        # Try progressively smaller chunk sizes until it works
        chunk_size = MAX_FILE_BYTES
        MIN_CHUNK = 2 * 1024 * 1024  # 2 MB floor

        while chunk_size >= MIN_CHUNK:
            num_parts = math.ceil(len(file_data) / chunk_size)
            log.info(
                f"Splitting {len(file_data)} bytes into {num_parts} part(s) "
                f"at {chunk_size / (1024*1024):.0f} MB each"
            )

            await self.send_message(
                f"📦 **Large result detected**\n"
                f"Original: `{filename}` — {len(file_data) / (1024*1024):.1f} MB\n"
                f"Sending: {num_parts} part(s) ({chunk_size / (1024*1024):.0f} MB each)"
            )

            all_ok = True
            for i in range(num_parts):
                start = i * chunk_size
                end = min(start + chunk_size, len(file_data))
                part_data = file_data[start:end]
                part_name = f"{Path(filename).stem}.part{i+1:03d}"
                part_content = f"Part {i+1}/{num_parts}"

                ok = await self._upload_file(part_data, part_name, part_content)
                if not ok:
                    if self._last_error and "413" in self._last_error:
                        # Still too big — halve and retry from scratch
                        chunk_size //= 2
                        log.warning(
                            f"Part {i+1} still too large, reducing to "
                            f"{chunk_size / (1024*1024):.0f} MB chunks..."
                        )
                        all_ok = False
                        break
                    # Non-413 failure — give up
                    log.error(f"Failed to send part {i+1}/{num_parts}")
                    return False

                # Rate limiting between parts
                if i < num_parts - 1:
                    await asyncio.sleep(1)

            if all_ok:
                return True

        log.error(f"Cannot send {filename}: still rejected at {MIN_CHUNK/(1024*1024):.0f} MB chunks")
        return False

    async def _upload_file(self, data: bytes, filename: str, content: str = "") -> bool:
        """Upload a single file to Discord webhook.

        Automatically retries on 429 (rate limit), 5xx server errors, and
        transient network errors.
        """
        max_retries = 5

        for attempt in range(max_retries + 1):
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

                    if resp.status == 429:
                        body_text = await resp.text()
                        retry_after = 5
                        try:
                            body_json = json.loads(body_text)
                            retry_after = float(body_json.get("retry_after", 5))
                        except (json.JSONDecodeError, ValueError):
                            pass
                        retry_after = min(retry_after, 60)
                        log.warning(
                            f"Discord rate limited (429), waiting {retry_after:.1f}s "
                            f"(attempt {attempt + 1}/{max_retries + 1})"
                        )
                        await asyncio.sleep(retry_after)
                        continue

                    if resp.status >= 500 and attempt < max_retries:
                        wait = 2 ** attempt
                        log.warning(f"Discord server error ({resp.status}), retrying in {wait}s...")
                        await asyncio.sleep(wait)
                        continue

                    body = await resp.text()
                    self._last_error = f"HTTP {resp.status}: {body[:200]}"
                    log.error(f"Discord file upload failed: {self._last_error}")
                    return False

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < max_retries:
                    wait = 2 ** attempt
                    log.warning(f"Discord upload network error ({e}), retrying in {wait}s...")
                    await asyncio.sleep(wait)
                    continue
                self._last_error = str(e)
                log.error(f"Discord file upload failed after {max_retries + 1} attempts: {e}")
                return False
            except Exception as e:
                self._last_error = str(e)
                log.error(f"Discord file upload error: {e}")
                return False

        self._last_error = "Failed after max retries"
        log.error(f"Discord upload failed: {self._last_error}")
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

        # Send the results zip (summary + matched lines — should be small)
        if ok and results_zip_data:
            zip_name = f"results_{job_id[:8]}.zip"
            ok = await self.send_file(results_zip_data, zip_name)

        return ok

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()


# Global singleton
discord_service = DiscordService()
