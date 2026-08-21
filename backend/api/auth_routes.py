"""Authentication API routes."""

from datetime import datetime, timezone
from fastapi import APIRouter, Request, Response, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database import get_db
from backend.models import User, UserRole
from backend.auth.password import hash_password, verify_password
from backend.auth.session import create_session_token, verify_session_token
from backend.logging_config import ComponentLogger

log = ComponentLogger("Auth")
router = APIRouter(prefix="/api/auth", tags=["auth"])

SESSION_COOKIE = "_ac_session"
MAX_LOGIN_ATTEMPTS = 5
LOGIN_WINDOW = 300  # 5 minutes

# Simple in-memory rate limiter
_login_attempts: dict[str, list[float]] = {}


class LoginRequest(BaseModel):
    password: str


class LoginResponse(BaseModel):
    success: bool
    role: str = ""
    message: str = ""


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


def _check_rate_limit(ip: str) -> bool:
    """Returns True if the IP is rate-limited."""
    import time
    now = time.time()
    attempts = _login_attempts.get(ip, [])
    # Remove old attempts
    attempts = [t for t in attempts if now - t < LOGIN_WINDOW]
    _login_attempts[ip] = attempts
    return len(attempts) >= MAX_LOGIN_ATTEMPTS


def _record_attempt(ip: str):
    import time
    if ip not in _login_attempts:
        _login_attempts[ip] = []
    _login_attempts[ip].append(time.time())


async def get_current_user(request: Request, db: AsyncSession = Depends(get_db)) -> User:
    """Dependency that returns the current authenticated user or raises 401."""
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")

    data = verify_session_token(token)
    if not data:
        raise HTTPException(status_code=401, detail="Session expired")

    result = await db.execute(select(User).where(User.id == data["uid"]))
    user = result.scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or disabled")

    return user


def require_role(*roles: UserRole):
    """Dependency factory that requires the user to have one of the specified roles."""
    async def check(user: User = Depends(get_current_user)):
        if user.role not in roles:
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user
    return check


@router.post("/login", response_model=LoginResponse)
async def login(
    request: Request,
    response: Response,
    body: LoginRequest,
    db: AsyncSession = Depends(get_db),
):
    """Authenticate with password."""
    ip = request.client.host if request.client else "unknown"

    if _check_rate_limit(ip):
        log.warning(f"Rate limited login attempt from {ip}")
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")

    _record_attempt(ip)

    # Try all active users
    result = await db.execute(select(User).where(User.is_active == True))
    users = result.scalars().all()

    for user in users:
        if verify_password(body.password, user.password_hash):
            # Successful login
            token = create_session_token(user.id, user.username, user.role.value)
            response.set_cookie(
                key=SESSION_COOKIE,
                value=token,
                httponly=True,
                samesite="strict",
                max_age=7 * 24 * 3600,
                path="/",
            )
            user.last_login = datetime.now(timezone.utc)
            await db.commit()

            # Clear rate limit on success
            _login_attempts.pop(ip, None)

            log.info(f"Login successful: {user.username} ({user.role.value})")
            return LoginResponse(success=True, role=user.role.value)

    log.warning(f"Failed login attempt from {ip}")
    return LoginResponse(success=False, message="Invalid password")


@router.post("/logout")
async def logout(response: Response):
    """Log out by clearing the session cookie."""
    response.delete_cookie(SESSION_COOKIE, path="/")
    return {"success": True}


@router.get("/me")
async def get_me(user: User = Depends(get_current_user)):
    """Get current user info."""
    return {
        "id": user.id,
        "username": user.username,
        "role": user.role.value,
        "is_active": user.is_active,
        "last_login": user.last_login.isoformat() if user.last_login else None,
    }


@router.post("/change-password")
async def change_password(
    body: ChangePasswordRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Change the current user's password."""
    if not verify_password(body.current_password, user.password_hash):
        raise HTTPException(status_code=400, detail="Current password is incorrect")

    if len(body.new_password) < 4:
        raise HTTPException(status_code=400, detail="Password must be at least 4 characters")

    result = await db.execute(select(User).where(User.id == user.id))
    db_user = result.scalar_one()
    db_user.password_hash = hash_password(body.new_password)
    await db.commit()

    log.info(f"Password changed for {user.username}")
    return {"success": True}
