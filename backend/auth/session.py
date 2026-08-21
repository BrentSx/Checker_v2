"""Session management with signed cookies."""

import time
import secrets
from typing import Optional

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from backend.config import SECRET_KEY

SESSION_TTL = 7 * 24 * 3600  # 7 days in seconds
_serializer = URLSafeTimedSerializer(SECRET_KEY)


def create_session_token(user_id: int, username: str, role: str) -> str:
    """Create a signed session token."""
    return _serializer.dumps({
        "uid": user_id,
        "user": username,
        "role": role,
        "nonce": secrets.token_hex(8),
    })


def verify_session_token(token: str) -> Optional[dict]:
    """
    Verify and decode a session token.
    Returns the payload dict or None if invalid/expired.
    """
    try:
        data = _serializer.loads(token, max_age=SESSION_TTL)
        return data
    except (BadSignature, SignatureExpired):
        return None
