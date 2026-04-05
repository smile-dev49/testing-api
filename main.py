"""
DB_APIS — two independent API groups:
  - /api/v1/view/*   MongoDB (parsed extraction data)
  - /api/v1/master-data/*  PostgreSQL (master data; ported from Grant master_data.py)

Run: python main.py
Render: set MONGODB_URL, DATABASE_URL (or PG_*), optional MASTER_DATA_API_KEY.
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from src.api.view import router as view_router
from src.api.api_entity import router as entity_router
from src.api.api_smo import router as smo_router
from src.api.api_shareholders import router as shareholders_router
from src.api.api_liquidators import router as liquidators_router
from src.api.api_auditors import router as auditors_router
from src.api.api_depositaries import router as depositaries_router
from src.api.api_regimes import router as regimes_router
from src.access.mongodb import close_mongodb_connection

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SERVICE_NAME = "DB_APIS"
SERVICE_VERSION = "1.0.0"


def _cors_origins() -> list[str]:
    raw = os.getenv("CORS_ORIGINS", "*").strip()
    if raw == "*":
        return ["*"]
    return [o.strip() for o in raw.split(",") if o.strip()]


def _openapi_enabled() -> bool:
    return os.getenv("ENABLE_OPENAPI_DOCS", "true").lower() in ("1", "true", "yes")


def _master_data_enabled() -> bool:
    return os.getenv("ENABLE_MASTER_DATA_APIS", "true").lower() in ("1", "true", "yes")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting %s v%s", SERVICE_NAME, SERVICE_VERSION)
    yield
    try:
        close_mongodb_connection()
    except Exception as e:
        logger.warning("Error closing MongoDB: %s", e)
    if _master_data_enabled():
        try:
            from src.master_data.database import dispose_engine as dispose_master_data_engine

            dispose_master_data_engine()
        except Exception as e:
            logger.warning("Error disposing PostgreSQL pool: %s", e)
    logger.info("Shutdown %s", SERVICE_NAME)


_docs = "/docs" if _openapi_enabled() else None
_redoc = "/redoc" if _openapi_enabled() else None

app = FastAPI(
    title=SERVICE_NAME,
    version=SERVICE_VERSION,
    description="MongoDB view API + optional PostgreSQL master-data API (standalone).",
    lifespan=lifespan,
    docs_url=_docs,
    redoc_url=_redoc,
    openapi_url="/openapi.json" if _openapi_enabled() else None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(view_router)
app.include_router(entity_router)
app.include_router(smo_router)
app.include_router(shareholders_router)
app.include_router(liquidators_router)
app.include_router(auditors_router)
app.include_router(depositaries_router)
app.include_router(regimes_router)

if _master_data_enabled():
    from src.master_data import router as master_data_router

    app.include_router(master_data_router, prefix="/api/v1")


@app.get("/api/v1/health", tags=["Health"])
async def health():
    return {
        "status": "healthy",
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "view_api_mongodb": True,
        "master_data_postgresql": _master_data_enabled(),
    }


if __name__ == "__main__":
    port = int(os.getenv("VIEW_API_PORT", "8093"))
    host = os.getenv("VIEW_API_HOST", "0.0.0.0")
    uvicorn.run(app, host=host, port=port, reload=False, log_level="info")
