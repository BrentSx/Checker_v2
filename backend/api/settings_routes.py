"""Settings API routes."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from backend.database import get_db
from backend.models import User, UserRole, Setting
from backend.api.auth_routes import get_current_user, require_role
from backend.logging_config import ComponentLogger

log = ComponentLogger("Settings")
router = APIRouter(prefix="/api/settings", tags=["settings"])

# Settings that should be masked in the frontend
MASKED_KEYS = {
    "discord_webhook_url", "cloudflare_tunnel_token",
    "telegram_api_hash",
}


class SettingUpdate(BaseModel):
    key: str
    value: str


class SettingsBatch(BaseModel):
    settings: dict[str, str]


@router.get("/")
async def get_settings(
    user: User = Depends(require_role(UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Get all settings."""
    result = await db.execute(select(Setting))
    settings = result.scalars().all()

    return {
        s.key: _mask_value(s.key, s.value) if user.role != UserRole.admin else s.value
        for s in settings
    }


@router.post("/")
async def update_settings(
    body: SettingsBatch,
    user: User = Depends(require_role(UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Update multiple settings at once."""
    for key, value in body.settings.items():
        result = await db.execute(select(Setting).where(Setting.key == key))
        setting = result.scalar_one_or_none()
        if setting:
            setting.value = value
        else:
            db.add(Setting(key=key, value=value))

    await db.commit()
    log.info(f"Settings updated by {user.username}: {list(body.settings.keys())}")
    return {"success": True}


@router.put("/{key}")
async def update_setting(
    key: str,
    body: SettingUpdate,
    user: User = Depends(require_role(UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Update a single setting."""
    result = await db.execute(select(Setting).where(Setting.key == key))
    setting = result.scalar_one_or_none()
    if setting:
        setting.value = body.value
    else:
        db.add(Setting(key=key, value=body.value))

    await db.commit()
    log.info(f"Setting '{key}' updated by {user.username}")
    return {"success": True}


@router.get("/{key}")
async def get_setting(
    key: str,
    user: User = Depends(require_role(UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Get a single setting."""
    result = await db.execute(select(Setting).where(Setting.key == key))
    setting = result.scalar_one_or_none()
    if not setting:
        raise HTTPException(status_code=404, detail="Setting not found")

    return {"key": setting.key, "value": setting.value}


def _mask_value(key: str, value: str) -> str:
    """Mask sensitive setting values."""
    if key in MASKED_KEYS and value:
        if len(value) > 8:
            return value[:4] + "****" + value[-4:]
        return "********"
    return value
