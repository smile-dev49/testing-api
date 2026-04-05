"""
PostgreSQL engine for Master Data API (standalone; no Grant backend).
Render.com: set DATABASE_URL=postgresql://...
"""
from __future__ import annotations

import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

_engine = None
_SessionLocal = None


def _build_url() -> str:
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url:
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://") :]
        return url
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    user = os.getenv("PG_USER", "postgres")
    password = os.getenv("PG_PASSWORD", "")
    database = os.getenv("PG_DATABASE", "postgres")
    if password:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    return f"postgresql://{user}@{host}:{port}/{database}"


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(
            _build_url(),
            pool_pre_ping=True,
            pool_recycle=300,
            pool_size=int(os.getenv("PG_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("PG_MAX_OVERFLOW", "10")),
            echo=False,
        )
    return _engine


class _LazyEngine:
    """Proxy so `engine.connect()` works without eager connect at import time."""

    def connect(self, *a, **kw):
        return get_engine().connect(*a, **kw)

    def begin(self, *a, **kw):
        return get_engine().begin(*a, **kw)

    def dispose(self, *a, **kw):
        return get_engine().dispose(*a, **kw)


engine = _LazyEngine()


def _session_local():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    return _SessionLocal


def get_db() -> Generator[Session, None, None]:
    SessionLocal = _session_local()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def dispose_engine():
    global _engine, _SessionLocal
    if _engine is not None:
        try:
            _engine.dispose()
        except Exception:
            pass
        _engine = None
    _SessionLocal = None
