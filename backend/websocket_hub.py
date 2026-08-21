"""WebSocket hub for broadcasting live updates to all connected clients."""

import asyncio
import json
from typing import Any
from fastapi import WebSocket


class WebSocketHub:
    """Manages WebSocket connections and broadcasts events."""

    def __init__(self):
        self._connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        async with self._lock:
            self._connections.add(ws)

    async def disconnect(self, ws: WebSocket):
        async with self._lock:
            self._connections.discard(ws)

    async def broadcast(self, event_type: str, data: Any):
        """Broadcast an event to all connected clients."""
        message = json.dumps({"type": event_type, "data": data})
        async with self._lock:
            dead = []
            for ws in self._connections:
                try:
                    await ws.send_text(message)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self._connections.discard(ws)

    async def send_to(self, ws: WebSocket, event_type: str, data: Any):
        """Send an event to a specific client."""
        try:
            await ws.send_text(json.dumps({"type": event_type, "data": data}))
        except Exception:
            pass

    @property
    def client_count(self) -> int:
        return len(self._connections)


# Global hub instance
hub = WebSocketHub()
