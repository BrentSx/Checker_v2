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
        self._dialogs_loaded = False  # True once get_dialogs() has run

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
        self._dialogs_loaded = False
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

                # Pre-load dialogs so entity cache is populated (like Go's GetChannels)
                await self._ensure_dialogs_loaded()
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

            # Pre-load dialogs so entity cache is populated
            await self._ensure_dialogs_loaded()

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

    async def _ensure_dialogs_loaded(self):
        """Fetch all dialogs once to populate Telethon's internal entity cache.

        This mirrors what the Go checker does: it calls GetChannels()
        (MessagesGetDialogs) on startup to cache every channel's access hash
        before trying to resolve individual channel IDs.  Without this step,
        get_entity(channel_id) fails because Telethon has never seen the entity.
        """
        if self._dialogs_loaded or not self._client or not self._authenticated:
            return
        try:
            dialogs = await self._client.get_dialogs()
            self._dialogs_loaded = True
            log.info(f"Loaded {len(dialogs)} dialogs into entity cache")
        except Exception as e:
            log.warning(f"Failed to load dialogs: {e}")

    async def get_channels(self) -> list[dict]:
        """Get list of channels/groups the user is in."""
        if not self._client or not self._authenticated:
            return []

        try:
            from telethon.tl.types import Channel

            # This also refreshes the dialog/entity cache
            await self._ensure_dialogs_loaded()

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

    async def _resolve_entity(self, channel_id: int):
        """Resolve a channel/chat ID to a Telethon entity.

        Mirrors the Go checker's approach: dialogs are loaded first (populating
        the access-hash cache), then the ID is resolved.  Handles both raw IDs
        (e.g. 1234567890) and marked IDs (e.g. -1001234567890).
        """
        # Make sure dialogs are cached first — this is the key step the old code does
        await self._ensure_dialogs_loaded()

        from telethon.tl.types import PeerChannel, PeerChat

        # Try the ID as-is — works when Telethon already has it cached
        try:
            return await self._client.get_entity(channel_id)
        except (ValueError, TypeError):
            pass

        # Strip the -100 prefix that Telegram uses for channels/supergroups
        cid = channel_id
        if cid < 0:
            cid_str = str(abs(cid))
            if cid_str.startswith("100"):
                cid = int(cid_str[3:])
            else:
                cid = abs(cid)

        # Try as PeerChannel (channel/supergroup)
        try:
            return await self._client.get_entity(PeerChannel(cid))
        except (ValueError, TypeError):
            pass

        # Try as regular chat
        try:
            return await self._client.get_entity(PeerChat(abs(channel_id)))
        except (ValueError, TypeError):
            pass

        # Last resort: try get_input_entity which uses the session cache
        try:
            input_entity = await self._client.get_input_entity(channel_id)
            return await self._client.get_entity(input_entity)
        except (ValueError, TypeError):
            pass

        raise ValueError(f"Could not resolve entity for ID {channel_id}")

    async def get_messages(self, channel_id: int, limit: int = 20) -> list[dict]:
        """Get recent messages with files from a channel."""
        if not self._client or not self._authenticated:
            return []

        try:
            entity = await self._resolve_entity(channel_id)
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

                # Build text with any embedded URLs from text-url entities
                text = msg.text or ""
                if msg.entities:
                    from telethon.tl.types import MessageEntityTextUrl
                    for ent in msg.entities:
                        if isinstance(ent, MessageEntityTextUrl) and ent.url:
                            text += " " + ent.url

                messages.append({
                    "id": msg.id,
                    "date": msg.date.isoformat() if msg.date else "",
                    "text": text,
                    "has_file": has_file,
                    "file_name": file_name,
                    "file_size": file_size,
                })
            return messages
        except Exception as e:
            log.error(f"Failed to get messages from channel {channel_id}: {e}")
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
            entity = await self._resolve_entity(channel_id)
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

        # Pre-load dialogs so the event handler can resolve entities
        await self._ensure_dialogs_loaded()

        # Resolve all channel entities up front so the event handler works
        resolved_entities = []
        for cid in channel_ids:
            try:
                entity = await self._resolve_entity(cid)
                resolved_entities.append(entity)
                log.info(f"Resolved channel {cid} -> {getattr(entity, 'title', cid)}")
            except Exception as e:
                log.warning(f"Could not resolve channel {cid}: {e} — skipping")

        if not resolved_entities:
            log.error("No channels could be resolved for monitoring")
            return

        from telethon import events

        @self._client.on(events.NewMessage(chats=resolved_entities))
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

        log.info(f"Monitoring {len(resolved_entities)} channel(s) for new files")

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
        self._dialogs_loaded = False
        log.info("Telegram disconnected")


# Global singleton
telegram_service = TelegramService()
