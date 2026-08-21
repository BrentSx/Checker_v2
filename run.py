#!/usr/bin/env python3
"""Direct entry point for the Snickerdoodle Checker application."""

import uvicorn
from backend.config import PORT

if __name__ == "__main__":
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=PORT,
        log_level="warning",
        reload=False,
    )
