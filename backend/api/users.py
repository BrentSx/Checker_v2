"""User management API routes (admin only)."""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from backend.database import get_db
from backend.models import User, UserRole
from backend.api.auth_routes import get_current_user, require_role
from backend.auth.password import hash_password
from backend.logging_config import ComponentLogger

log = ComponentLogger("Users")
router = APIRouter(prefix="/api/users", tags=["users"])


class CreateUserRequest(BaseModel):
    username: str
    password: str
    role: str = "viewer"


class UpdateUserRequest(BaseModel):
    role: Optional[str] = None
    is_active: Optional[bool] = None
    new_password: Optional[str] = None


@router.get("/")
async def list_users(
    user: User = Depends(require_role(UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """List all users."""
    result = await db.execute(select(User).order_by(User.id))
    users = result.scalars().all()
    return [
        {
            "id": u.id,
            "username": u.username,
            "role": u.role.value,
            "is_active": u.is_active,
            "created_at": u.created_at.isoformat() if u.created_at else None,
            "last_login": u.last_login.isoformat() if u.last_login else None,
        }
        for u in users
    ]


@router.post("/")
async def create_user(
    body: CreateUserRequest,
    user: User = Depends(require_role(UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Create a new user."""
    # Validate role
    try:
        role = UserRole(body.role)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid role: {body.role}")

    # Check username uniqueness
    result = await db.execute(select(User).where(User.username == body.username))
    if result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Username already exists")

    if len(body.password) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")

    new_user = User(
        username=body.username,
        password_hash=hash_password(body.password),
        role=role,
        is_active=True,
    )
    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    log.info(f"User created: {body.username} ({role.value}) by {user.username}")
    return {"success": True, "id": new_user.id}


@router.put("/{user_id}")
async def update_user(
    user_id: int,
    body: UpdateUserRequest,
    user: User = Depends(require_role(UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Update a user's role, status, or password."""
    result = await db.execute(select(User).where(User.id == user_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    # Prevent admin from disabling themselves
    if target.id == user.id and body.is_active is False:
        raise HTTPException(status_code=400, detail="Cannot disable your own account")

    if body.role is not None:
        try:
            target.role = UserRole(body.role)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid role: {body.role}")

    if body.is_active is not None:
        target.is_active = body.is_active

    if body.new_password is not None:
        if len(body.new_password) < 4:
            raise HTTPException(status_code=400, detail="Password must be at least 4 characters")
        target.password_hash = hash_password(body.new_password)

    await db.commit()
    log.info(f"User {target.username} updated by {user.username}")
    return {"success": True}


@router.delete("/{user_id}")
async def delete_user(
    user_id: int,
    user: User = Depends(require_role(UserRole.admin)),
    db: AsyncSession = Depends(get_db),
):
    """Delete a user."""
    if user_id == user.id:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")

    result = await db.execute(select(User).where(User.id == user_id))
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status_code=404, detail="User not found")

    await db.delete(target)
    await db.commit()
    log.info(f"User {target.username} deleted by {user.username}")
    return {"success": True}
