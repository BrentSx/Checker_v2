"""Telegram client service using Telethon for monitoring channels and downloading files."""

import asyncio
import os
from pathlib import Path
from typing import Optional, Callable, Awaitable
from datetime import datetime, timezone

from backend.config import (
    TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE,
    TELEGRAM_CHANNEL_IDS, DATA_DIR, DOWNLOAD_DIR,
)
from backend.logging_config import ComponentLogger

log = ComponentLogger("Telegram")


class TelegramService:
    """Manages the Telegram client connection and file monitoring."""

    def __init__(self):
        self._client = None
        self._connected = False
        self._authenticated = False
        self._state = "idle"  # idle, connecting, wait_code, wait_2fa, ready, error
        self._error = ""
        self._user_info = None
        self._session_file = str(DATA_DIR / "telegram_session")
        self._code_future: Optional[asyncio.Future] = None
        self._2fa_future: Optional[asyncio.Future] = None
        self._on_new_file: Optional[Callable] = None
        self._monitor_task: Optional[asyncio.Task] = None

    @property
    def state(self) -> str:
        return self._state

    @property
    def error(self) -> str:
        return self._error

    @property
    def is_ready(self) -> bool:
        return self._state == "ready"

    @property
    def user_info(self) -> Optional[dict]:
        return self._user_info

    def status(self) -> dict:
        return {
            "state": self._state,
            "error": self._error,
            "user": self._user_info,
            "configured": bool(TELEGRAM_API_ID and TELEGRAM_API_HASH),
            "channels": TELEGRAM_CHANNEL_IDS,
        }

    async def start(self, api_id: int = 0, api_hash: str = "", phone: str = ""):
        """Start the Telegram client and authenticate."""
        try:
            from telethon import TelegramClient
            from telethon.errors import SessionPasswordNeededError
        except ImportError:
            self._state = "error"
            self._error = "Telethon is not installed. Run: pip install telethon"
            log.error(self._error)
            return

        aid = api_id or TELEGRAM_API_ID
        ahash = api_hash or TELEGRAM_API_HASH
        ph = phone or TELEGRAM_PHONE

        if not aid or not ahash:
            self._state = "error"
            self._error = "Telegram API ID and Hash are not configured"
            log.error(self._error)
            return

        self._state = "connecting"
        self._error = ""
        log.info("Connecting to Telegram...")

        try:
            self._client = TelegramClient(self._session_file, aid, ahash)
            await self._client.connect()

            if await self._client.is_user_authorized():
                me = await self._client.get_me()
                self._user_info = {
                    "id": me.id,
                    "first_name": me.first_name or "",
                    "last_name": me.last_name or "",
                    "username": me.username or "",
                    "phone": me.phone or "",
                }
                self._state = "ready"
                self._authenticated = True
                log.info(f"Telegram authenticated as {me.first_name} (@{me.username})")
                return

            if not ph:
                self._state = "error"
                self._error = "Phone number not configured"
                return

            # Start auth flow
            self._state = "wait_code"
            await self._client.send_code_request(ph)
            log.info("Verification code sent to Telegram")

            # Wait for code submission
            self._code_future = asyncio.get_event_loop().create_future()
            code = await self._code_future
            self._code_future = None

            try:
                await self._client.sign_in(ph, code)
            except SessionPasswordNeededError:
                self._state = "wait_2fa"
                log.info("2FA password required")
                self._2fa_future = asyncio.get_event_loop().create_future()
                password = await self._2fa_future
                self._2fa_future = None
                await self._client.sign_in(password=password)

            me = await self._client.get_me()
            self._user_info = {
                "id": me.id,
                "first_name": me.first_name or "",
                "last_name": me.last_name or "",
                "username": me.username or "",
                "phone": me.phone or "",
            }
            self._state = "ready"
            self._authenticated = True
            log.info(f"Telegram authenticated as {me.first_name}")

        except Exception as e:
            self._state = "error"
            self._error = str(e)
            log.error(f"Telegram connection failed: {e}")

    def submit_code(self, code: str):
        """Submit verification code."""
        if self._code_future and not self._code_future.done():
            self._code_future.set_result(code)

    def submit_2fa(self, password: str):
        """Submit 2FA password."""
        if self._2fa_future and not self._2fa_future.done():
            self._2fa_future.set_result(password)

    async def get_channels(self) -> list[dict]:
        """Get list of channels/groups the user is in."""
        if not self._client or not self._authenticated:
            return []

        try:
            from telethon.tl.types import Channel
            dialogs = await self._client.get_dialogs()
            channels = []
            for d in dialogs:
                if isinstance(d.entity, Channel):
                    channels.append({
                        "id": d.entity.id,
                        "title": d.entity.title,
                        "username": d.entity.username or "",
                        "members": getattr(d.entity, "participants_count", 0) or 0,
                    })
            return channels
        except Exception as e:
            log.error(f"Failed to get channels: {e}")
            return []

    async def get_messages(self, channel_id: int, limit: int = 20) -> list[dict]:
        """Get recent messages with files from a channel."""
        if not self._client or not self._authenticated:
            return []

        try:
            entity = await self._client.get_entity(channel_id)
            messages = []
            async for msg in self._client.iter_messages(entity, limit=limit):
                has_file = msg.document is not None or msg.media is not None
                file_name = ""
                file_size = 0
                if msg.document:
                    for attr in msg.document.attributes:
                        if hasattr(attr, "file_name"):
                            file_name = attr.file_name
                            break
                    file_size = msg.document.size or 0

                messages.append({
                    "id": msg.id,
                    "date": msg.date.isoformat() if msg.date else "",
                    "text": msg.text or "",
                    "has_file": has_file,
                    "file_name": file_name,
                    "file_size": file_size,
                })
            return messages
        except Exception as e:
            log.error(f"Failed to get messages: {e}")
            return []

    async def download_file(
        self,
        channel_id: int,
        message_id: int,
        dest_dir: str,
        progress_callback: Optional[Callable] = None,
    ) -> Optional[str]:
        """Download a file from a Telegram message. Returns the local file path."""
        if not self._client or not self._authenticated:
            raise RuntimeError("Telegram not connected")

        try:
            entity = await self._client.get_entity(channel_id)
            msg = await self._client.get_messages(entity, ids=message_id)
            if not msg or not msg.document:
                raise ValueError("Message has no file attachment")

            file_name = "unknown"
            for attr in msg.document.attributes:
                if hasattr(attr, "file_name"):
                    file_name = attr.file_name
                    break

            os.makedirs(dest_dir, exist_ok=True)
            dest_path = os.path.join(dest_dir, file_name)

            log.info(f"Downloading {file_name} ({msg.document.size} bytes)")

            await self._client.download_media(
                msg,
                file=dest_path,
                progress_callback=progress_callback,
            )

            log.info(f"Download complete: {file_name}")
            return dest_path

        except Exception as e:
            log.error(f"Download failed: {e}")
            raise

    async def monitor_channels(self, channel_ids: list[int], on_new_file: Callable):
        """Monitor channels for new files. Calls on_new_file(channel_id, message_id, filename, filesize)."""
        if not self._client or not self._authenticated:
            log.error("Cannot monitor: Telegram not connected")
            return

        self._on_new_file = on_new_file

        from telethon import events

        @self._client.on(events.NewMessage(chats=channel_ids))
        async def handler(event):
            msg = event.message
            if msg.document:
                file_name = "unknown"
                for attr in msg.document.attributes:
                    if hasattr(attr, "file_name"):
                        file_name = attr.file_name
                        break
                file_size = msg.document.size or 0

                log.info(f"New file detected in channel {event.chat_id}: {file_name} ({file_size} bytes)")
                await on_new_file(event.chat_id, msg.id, file_name, file_size)

        log.info(f"Monitoring {len(channel_ids)} channel(s) for new files")

    async def disconnect(self):
        """Disconnect the Telegram client."""
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                pass
        self._state = "idle"
        self._authenticated = False
        self._connected = False
        self._user_info = None
        log.info("Telegram disconnected")


# Global singleton
telegram_service = TelegramService()
