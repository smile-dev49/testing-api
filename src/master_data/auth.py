"""
Standalone auth for Master Data: no JWT (optional API key later via env).
Grant backend used get_current_user; here we return a fixed service identity.
"""
from __future__ import annotations

import os

from fastapi import Header, HTTPException, status


async def get_current_user(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> dict:
    expected = os.getenv("MASTER_DATA_API_KEY", "").strip()
    if expected:
        if not x_api_key or x_api_key != expected:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing X-API-Key",
            )
    return {"id": 0, "username": "db_apis_master_data"}
