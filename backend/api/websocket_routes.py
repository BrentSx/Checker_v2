"""WebSocket endpoint for live updates."""

import asyncio
import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from backend.websocket_hub import hub
from backend.auth.session import verify_session_token
from backend.logging_config import subscribe_logs, unsubscribe_logs

router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """WebSocket endpoint for live dashboard updates."""
    # Authenticate via query param (cookies aren't sent for WS in all browsers)
    token = ws.query_params.get("token", "")
    if not token:
        # Try cookie
        token = ws.cookies.get("_ac_session", "")

    session_data = verify_session_token(token)
    if not session_data:
        await ws.close(code=4001, reason="Unauthorized")
        return

    await hub.connect(ws)
    log_queue = subscribe_logs()

    try:
        # Start a task to forward log entries
        async def forward_logs():
            try:
                while True:
                    entry = await log_queue.get()
                    await hub.send_to(ws, "log", entry)
            except asyncio.CancelledError:
                pass

        log_task = asyncio.create_task(forward_logs())

        # Keep connection alive, handle incoming messages
        while True:
            try:
                data = await ws.receive_text()
                msg = json.loads(data)

                # Handle ping/pong
                if msg.get("type") == "ping":
                    await hub.send_to(ws, "pong", {})

            except WebSocketDisconnect:
                break
            except json.JSONDecodeError:
                pass

    finally:
        log_task.cancel()
        unsubscribe_logs(log_queue)
        await hub.disconnect(ws)
