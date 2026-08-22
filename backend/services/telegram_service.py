"""Telegram client service using Telethon for monitoring channels and downloading files."""

import asyncio
import os
import time
from pathlib import Path
from typing import Optional, Callable, Awaitable
from datetime import datetime, timezone

from backend.config import (
    TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE,
    TELEGRAM_CHANNEL_IDS, DATA_DIR, DOWNLOAD_DIR,
)
from backend.logging_config import ComponentLogger

log = ComponentLogger("Telegram")

# ── Parallel download constants (mirrors Go checker) ─────────────────────────

CHUNK_SIZE = 1024 * 1024  # 1 MB — MTProto maximum
MAX_RETRIES = 8
RETRY_DELAYS = [0, 0.25, 0.5, 1, 2, 3, 5, 10]  # seconds


def _best_workers(file_size: int) -> int:
    """Return parallel worker count scaled by file size (matches Go checker)."""
    if file_size < 1 << 20:       # < 1 MB
        return 1
    if file_size < 5 << 20:       # < 5 MB
        return 2
    if file_size < 20 << 20:      # < 20 MB
        return 4
    if file_size < 50 << 20:      # < 50 MB
        return 8
    if file_size < 500 << 20:     # < 500 MB
        return 16
    return 32


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
        self._dialogs_loaded = False
        self.fast_download = True  # Default to fast mode

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
            "fast_download": self.fast_download,
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
                await self._ensure_dialogs_loaded()
                return

            if not ph:
                self._state = "error"
                self._error = "Phone number not configured"
                return

            self._state = "wait_code"
            await self._client.send_code_request(ph)
            log.info("Verification code sent to Telegram")

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
            await self._ensure_dialogs_loaded()

        except Exception as e:
            self._state = "error"
            self._error = str(e)
            log.error(f"Telegram connection failed: {e}")

    def submit_code(self, code: str):
        if self._code_future and not self._code_future.done():
            self._code_future.set_result(code)

    def submit_2fa(self, password: str):
        if self._2fa_future and not self._2fa_future.done():
            self._2fa_future.set_result(password)

    async def _ensure_dialogs_loaded(self):
        """Fetch dialogs once to populate Telethon's entity cache (mirrors Go GetChannels)."""
        if self._dialogs_loaded or not self._client or not self._authenticated:
            return
        try:
            dialogs = await self._client.get_dialogs()
            self._dialogs_loaded = True
            log.info(f"Loaded {len(dialogs)} dialogs into entity cache")
        except Exception as e:
            log.warning(f"Failed to load dialogs: {e}")

    async def get_channels(self) -> list[dict]:
        if not self._client or not self._authenticated:
            return []
        try:
            from telethon.tl.types import Channel
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
        """Resolve a channel ID to a Telethon entity. Loads dialogs first."""
        await self._ensure_dialogs_loaded()
        from telethon.tl.types import PeerChannel, PeerChat

        try:
            return await self._client.get_entity(channel_id)
        except (ValueError, TypeError):
            pass

        cid = channel_id
        if cid < 0:
            cid_str = str(abs(cid))
            if cid_str.startswith("100"):
                cid = int(cid_str[3:])
            else:
                cid = abs(cid)

        try:
            return await self._client.get_entity(PeerChannel(cid))
        except (ValueError, TypeError):
            pass

        try:
            return await self._client.get_entity(PeerChat(abs(channel_id)))
        except (ValueError, TypeError):
            pass

        try:
            inp = await self._client.get_input_entity(channel_id)
            return await self._client.get_entity(inp)
        except (ValueError, TypeError):
            pass

        raise ValueError(f"Could not resolve entity for ID {channel_id}")

    async def get_messages(self, channel_id: int, limit: int = 20) -> list[dict]:
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

    # ── Download methods ─────────────────────────────────────────────────────

    async def download_file(
        self,
        channel_id: int,
        message_id: int,
        dest_dir: str,
        progress_callback: Optional[Callable] = None,
    ) -> Optional[str]:
        """Download a file from a Telegram message. Uses fast or slow mode."""
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
            file_size = msg.document.size or 0

            if self.fast_download and file_size > 0:
                log.info(f"⚡ Fast downloading {file_name} ({_format_size(file_size)}) "
                         f"with {_best_workers(file_size)} workers")
                await self._download_parallel(
                    msg.document, dest_path, file_size, progress_callback
                )
            else:
                log.info(f"Downloading {file_name} ({_format_size(file_size)})")
                await self._client.download_media(
                    msg, file=dest_path, progress_callback=progress_callback,
                )

            log.info(f"Download complete: {file_name}")
            return dest_path

        except Exception as e:
            log.error(f"Download failed: {e}")
            raise

    async def _download_parallel(
        self,
        document,
        dest_path: str,
        file_size: int,
        progress_callback: Optional[Callable] = None,
    ):
        """Parallel chunk download — mirrors Go checker's approach.

        Splits the file into 1 MB chunks, downloads them concurrently with
        worker count scaled by file size, retries transient failures.
        """
        from telethon.tl.functions.upload import GetFileRequest
        from telethon.tl.types import InputDocumentFileLocation

        location = InputDocumentFileLocation(
            id=document.id,
            access_hash=document.access_hash,
            file_reference=document.file_reference,
            thumb_size="",
        )

        num_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        workers = _best_workers(file_size)

        # Pre-allocate the file
        with open(dest_path, "wb") as f:
            f.truncate(file_size)

        downloaded = 0
        download_lock = asyncio.Lock()
        start_time = time.monotonic()
        first_error = None
        error_lock = asyncio.Lock()

        async def fetch_chunk(chunk_idx: int):
            nonlocal downloaded, first_error
            offset = chunk_idx * CHUNK_SIZE

            for attempt in range(MAX_RETRIES):
                # Bail if another chunk already failed
                async with error_lock:
                    if first_error is not None:
                        return

                if attempt > 0 and attempt < len(RETRY_DELAYS):
                    await asyncio.sleep(RETRY_DELAYS[attempt])

                try:
                    result = await self._client(GetFileRequest(
                        location=location,
                        offset=offset,
                        limit=CHUNK_SIZE,
                        precise=True,
                        cdn_supported=False,
                    ))

                    data = result.bytes
                    if not data:
                        return

                    # Write at the correct offset (file I/O in executor to not block)
                    def _write():
                        with open(dest_path, "r+b") as f:
                            f.seek(offset)
                            f.write(data)

                    await asyncio.get_event_loop().run_in_executor(None, _write)

                    async with download_lock:
                        downloaded += len(data)
                        if progress_callback:
                            try:
                                await progress_callback(downloaded, file_size)
                            except Exception:
                                pass
                    return

                except Exception as e:
                    if attempt == MAX_RETRIES - 1:
                        async with error_lock:
                            if first_error is None:
                                first_error = e
                        return

        # Run chunks with bounded concurrency
        semaphore = asyncio.Semaphore(workers)

        async def bounded_fetch(idx: int):
            async with semaphore:
                await fetch_chunk(idx)

        tasks = [bounded_fetch(i) for i in range(num_chunks)]
        await asyncio.gather(*tasks)

        if first_error is not None:
            raise first_error

        elapsed = time.monotonic() - start_time
        speed = file_size / elapsed if elapsed > 0 else 0
        log.info(f"Parallel download finished: {_format_size(file_size)} in "
                 f"{elapsed:.1f}s ({_format_size(int(speed))}/s)")

    # ── Channel monitoring ───────────────────────────────────────────────────

    async def monitor_channels(self, channel_ids: list[int], on_new_file: Callable):
        if not self._client or not self._authenticated:
            log.error("Cannot monitor: Telegram not connected")
            return

        self._on_new_file = on_new_file
        await self._ensure_dialogs_loaded()

        resolved = []
        for cid in channel_ids:
            try:
                entity = await self._resolve_entity(cid)
                resolved.append(entity)
                log.info(f"Resolved channel {cid} -> {getattr(entity, 'title', cid)}")
            except Exception as e:
                log.warning(f"Could not resolve channel {cid}: {e}")

        if not resolved:
            log.error("No channels could be resolved for monitoring")
            return

        from telethon import events

        @self._client.on(events.NewMessage(chats=resolved))
        async def handler(event):
            msg = event.message
            if msg.document:
                file_name = "unknown"
                for attr in msg.document.attributes:
                    if hasattr(attr, "file_name"):
                        file_name = attr.file_name
                        break
                file_size = msg.document.size or 0
                log.info(f"New file in channel {event.chat_id}: {file_name} ({file_size} bytes)")
                await on_new_file(event.chat_id, msg.id, file_name, file_size)

        log.info(f"Monitoring {len(resolved)} channel(s) for new files")

    async def disconnect(self):
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


def _format_size(n: int) -> str:
    """Human-readable file size."""
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    return f"{n / (1024 * 1024 * 1024):.2f} GB"


# Global singleton
telegram_service = TelegramService()
