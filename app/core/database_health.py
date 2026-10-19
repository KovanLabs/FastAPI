"""DuckDB readiness checking with protected structured events."""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from app.core.exceptions import DatabaseUnavailableError


logger = logging.getLogger(__name__)


def check_database_health(database_path: Path) -> None:
    """Verify that DuckDB can open the import database and execute a query."""

    try:
        target = str(database_path) if database_path.exists() else ":memory:"
        with duckdb.connect(target) as connection:
            result = connection.execute("SELECT 1").fetchone()
        if result != (1,):
            raise RuntimeError("DuckDB health query returned an invalid result")
    except Exception as exc:
        logger.exception(
            "Database health check failed",
            extra={
                "event": "database.health_check_failed",
                "database_status": "unavailable",
            },
        )
        raise DatabaseUnavailableError(
            "Import database is unavailable"
        ) from exc

    logger.debug(
        "Database health check passed",
        extra={
            "event": "database.health_check_passed",
            "database_status": "healthy",
        },
    )
