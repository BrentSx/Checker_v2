"""Main FastAPI application."""

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select

from backend.config import PORT, INITIAL_ADMIN_PASSWORD, CLOUDFLARE_DOMAIN
from backend.database import init_db, async_session
from backend.models import User, UserRole
from backend.auth.password import hash_password
from backend.auth.session import verify_session_token
from backend.logging_config import setup_logging, ComponentLogger

# API routers
from backend.api.auth_routes import router as auth_router, SESSION_COOKIE
from backend.api.dashboard import router as dashboard_router
from backend.api.queue_routes import router as queue_router
from backend.api.users import router as users_router
from backend.api.settings_routes import router as settings_router
from backend.api.workers_routes import router as workers_router
from backend.api.logs_routes import router as logs_router
from backend.api.stats_routes import router as stats_router
from backend.api.websocket_routes import router as ws_router

log = ComponentLogger("App")

# Resolve paths
BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
TEMPLATES_DIR = FRONTEND_DIR / "templates"
STATIC_DIR = FRONTEND_DIR / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown."""
    # Setup logging
    setup_logging()
    log.info("Starting Snickerdoodle Checker...")

    # Initialize database
    await init_db()
    log.info("Database initialized")

    # Create admin user if not exists
    await _ensure_admin()

    # Start workers
    from backend.workers.queue_manager import queue_manager
    from backend.workers.watchdog import watchdog
    from backend.workers.telegram_monitor import telegram_monitor
    from backend.services.telegram_service import telegram_service
    from backend.services.cloudflare_service import cloudflare_service

    # Load Discord webhook from DB settings (overrides .env if set)
    from backend.services.discord_service import discord_service
    try:
        from backend.models import Setting
        from sqlalchemy import select as sa_select
        async with async_session() as session:
            result = await session.execute(
                sa_select(Setting).where(Setting.key == "discord_webhook_url")
            )
            setting = result.scalar_one_or_none()
            if setting and setting.value:
                discord_service.configure(setting.value)
                log.info("Discord webhook loaded from DB settings")
    except Exception as e:
        log.warning(f"Failed to load Discord webhook from DB: {e}")

    # Clean up old results directories (older than 7 days)
    await _cleanup_old_results()

    await queue_manager.start()
    await watchdog.start()

    # Start Telegram if configured
    try:
        await telegram_service.start()
        if telegram_service.is_ready:
            await telegram_monitor.start()
    except Exception as e:
        log.warning(f"Telegram auto-start failed: {e}")

    # Start Cloudflare Tunnel if configured
    try:
        await cloudflare_service.start()
    except Exception as e:
        log.warning(f"Cloudflare Tunnel start failed: {e}")

    log.info(f"Server ready on port {PORT}")
    if CLOUDFLARE_DOMAIN:
        log.info(f"Public URL: https://{CLOUDFLARE_DOMAIN}")

    yield

    # Shutdown
    log.info("Shutting down...")
    from backend.workers.queue_manager import queue_manager
    from backend.workers.watchdog import watchdog
    from backend.workers.telegram_monitor import telegram_monitor
    from backend.services.telegram_service import telegram_service
    from backend.services.cloudflare_service import cloudflare_service
    from backend.services.discord_service import discord_service

    await telegram_monitor.stop()
    await queue_manager.stop()
    await watchdog.stop()
    await telegram_service.disconnect()
    await cloudflare_service.stop()
    await discord_service.close()
    log.info("Shutdown complete")


async def _ensure_admin():
    """Create the admin user on first startup."""
    async with async_session() as session:
        result = await session.execute(
            select(User).where(User.role == UserRole.admin)
        )
        if result.scalar_one_or_none() is None:
            admin = User(
                username="admin",
                password_hash=hash_password(INITIAL_ADMIN_PASSWORD),
                role=UserRole.admin,
                is_active=True,
            )
            session.add(admin)
            await session.commit()
            log.info("Admin account created with initial password")


async def _cleanup_old_results():
    """Remove results directories older than 7 days to prevent disk bloat.

    Results directories are named ``YYYY-MM-DD_HH-MM-SS`` under RESULTS_DIR.
    """
    from backend.config import RESULTS_DIR
    import shutil
    from datetime import timedelta

    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    cleaned = 0

    try:
        for entry in RESULTS_DIR.iterdir():
            if not entry.is_dir():
                continue
            # Parse the directory name as a timestamp
            try:
                dir_time = datetime.strptime(entry.name, "%Y-%m-%d_%H-%M-%S")
                dir_time = dir_time.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            if dir_time < cutoff:
                shutil.rmtree(entry, ignore_errors=True)
                cleaned += 1
    except Exception as e:
        log.warning(f"Results cleanup error: {e}")

    if cleaned > 0:
        log.info(f"Cleaned up {cleaned} old results director{'y' if cleaned == 1 else 'ies'}")


# Create FastAPI app
app = FastAPI(
    title="Snickerdoodle Checker",
    version="2.0.0",
    lifespan=lifespan,
)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Include API routers
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(queue_router)
app.include_router(users_router)
app.include_router(settings_router)
app.include_router(workers_router)
app.include_router(logs_router)
app.include_router(stats_router)
app.include_router(ws_router)

# Health check (no auth)
from backend.api.dashboard import health_check
app.get("/health")(health_check)

# Templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _is_authenticated(request: Request) -> dict | None:
    """Check if the request has a valid session. Returns session data or None."""
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        return None
    return verify_session_token(token)


# ── Page routes ─────────────────────────────────────────────────────────────

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    session = _is_authenticated(request)
    if session:
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    session = _is_authenticated(request)
    if not session:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("dashboard.html", {
        "request": request, "user": session, "page": "dashboard",
    })


@app.get("/queue", response_class=HTMLResponse)
async def queue_page(request: Request):
    session = _is_authenticated(request)
    if not session:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("queue.html", {
        "request": request, "user": session, "page": "queue",
    })


@app.get("/files", response_class=HTMLResponse)
async def files_page(request: Request):
    session = _is_authenticated(request)
    if not session:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("files.html", {
        "request": request, "user": session, "page": "files",
    })


@app.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request):
    session = _is_authenticated(request)
    if not session:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("logs.html", {
        "request": request, "user": session, "page": "logs",
    })


@app.get("/system", response_class=HTMLResponse)
async def system_page(request: Request):
    session = _is_authenticated(request)
    if not session:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("system.html", {
        "request": request, "user": session, "page": "system",
    })


@app.get("/statistics", response_class=HTMLResponse)
async def stats_page(request: Request):
    session = _is_authenticated(request)
    if not session:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse("statistics.html", {
        "request": request, "user": session, "page": "statistics",
    })


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    session = _is_authenticated(request)
    if not session:
        return RedirectResponse(url="/login", status_code=302)
    if session.get("role") != "admin":
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("settings.html", {
        "request": request, "user": session, "page": "settings",
    })


@app.get("/users", response_class=HTMLResponse)
async def users_page(request: Request):
    session = _is_authenticated(request)
    if not session:
        return RedirectResponse(url="/login", status_code=302)
    if session.get("role") != "admin":
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse("users.html", {
        "request": request, "user": session, "page": "users",
    })
