"""Centralized configuration loaded from .env and environment variables."""

import os
import secrets
from pathlib import Path
from dotenv import load_dotenv

# Project root is the parent of the backend/ directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(ENV_FILE)


def _get(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def _int(key: str, default: int = 0) -> int:
    v = _get(key)
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _bool(key: str, default: bool = False) -> bool:
    v = _get(key).lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off", ""):
        return default
    return default


def _list(key: str) -> list[str]:
    raw = _get(key)
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


# ── Server ──────────────────────────────────────────────────────────────────

PORT = _int("PORT", 7777)

# Auto-generate a secret key if not provided
_secret = _get("SECRET_KEY")
if not _secret:
    _secret = secrets.token_hex(32)
SECRET_KEY = _secret

# ── Telegram ────────────────────────────────────────────────────────────────

TELEGRAM_API_ID = _int("TELEGRAM_API_ID")
TELEGRAM_API_HASH = _get("TELEGRAM_API_HASH")
TELEGRAM_PHONE = _get("TELEGRAM_PHONE")
TELEGRAM_CHANNEL_IDS = [int(x) for x in _list("TELEGRAM_CHANNEL_IDS") if x.isdigit() or (x.startswith("-") and x[1:].isdigit())]

# ── Discord ─────────────────────────────────────────────────────────────────

DISCORD_WEBHOOK_URL = _get("DISCORD_WEBHOOK_URL")
DISCORD_MAX_FILE_SIZE_MB = _int("DISCORD_MAX_FILE_SIZE_MB", 8)

# ── Cloudflare ──────────────────────────────────────────────────────────────

CLOUDFLARE_DOMAIN = _get("CLOUDFLARE_DOMAIN", "checker.ravealts.com")
CLOUDFLARE_TUNNEL_TOKEN = _get("CLOUDFLARE_TUNNEL_TOKEN")

# ── Checker ─────────────────────────────────────────────────────────────────

CHECKER_COMMAND = _get("CHECKER_COMMAND")
CHECKER_WORKERS = _int("CHECKER_WORKERS", 20)
PROXY_FILE = _get("PROXY_FILE")

# ── Directories ─────────────────────────────────────────────────────────────

DOWNLOAD_DIR = Path(_get("DOWNLOAD_DIR", str(PROJECT_ROOT / "downloads")))
TEMP_DIR = Path(_get("TEMP_DIR", str(PROJECT_ROOT / "temp")))
RESULTS_DIR = Path(_get("RESULTS_DIR", str(PROJECT_ROOT / "results")))
LOG_DIR = Path(_get("LOG_DIR", str(PROJECT_ROOT / "logs")))
DATA_DIR = Path(_get("DATA_DIR", str(PROJECT_ROOT / "data")))

# Ensure directories exist
for d in (DOWNLOAD_DIR, TEMP_DIR, RESULTS_DIR, LOG_DIR, DATA_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ── Database ────────────────────────────────────────────────────────────────

DATABASE_URL = f"sqlite+aiosqlite:///{DATA_DIR / 'audo_check.db'}"

# ── Queue ───────────────────────────────────────────────────────────────────

MAX_RETRIES = _int("MAX_RETRIES", 3)
RETRY_DELAY = _int("RETRY_DELAY", 30)

# ── Logging ─────────────────────────────────────────────────────────────────

LOG_LEVEL = _get("LOG_LEVEL", "INFO").upper()
LOG_MAX_SIZE_MB = _int("LOG_MAX_SIZE_MB", 50)
LOG_BACKUP_COUNT = _int("LOG_BACKUP_COUNT", 5)

# ── Initial Admin ───────────────────────────────────────────────────────────

INITIAL_ADMIN_PASSWORD = "ship2026"
