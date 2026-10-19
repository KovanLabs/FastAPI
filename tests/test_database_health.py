import json
import logging
from collections.abc import Iterator
from io import StringIO
from pathlib import Path

import duckdb
import pytest

from app.core.database_health import check_database_health
from app.core.exceptions import DatabaseUnavailableError
from app.core.log_context import bind_log_context
from app.core.logging_config import configure_logging


@pytest.fixture(autouse=True)
def isolate_application_logger() -> Iterator[None]:
    application_logger = logging.getLogger("app")
    for handler in tuple(application_logger.handlers):
        application_logger.removeHandler(handler)
        handler.close()
    application_logger.setLevel(logging.NOTSET)
    application_logger.propagate = True
    yield
    for handler in tuple(application_logger.handlers):
        application_logger.removeHandler(handler)
        handler.close()


def test_database_health_success_emits_correlated_debug_event(
    tmp_path: Path,
) -> None:
    output = StringIO()
    configure_logging("DEBUG", stream=output)

    database_path = tmp_path / "imports.duckdb"
    with bind_log_context(request_id="req-health-101"):
        check_database_health(database_path)

    event = json.loads(output.getvalue())
    assert event["event"] == "database.health_check_passed"
    assert event["level"] == "DEBUG"
    assert event["request_id"] == "req-health-101"
    assert event["database_status"] == "healthy"
    assert not database_path.exists()


def test_database_health_failure_emits_protected_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = StringIO()
    configure_logging("DEBUG", stream=output)

    def fail_to_connect(*args: object, **kwargs: object) -> None:
        raise duckdb.IOException(f"secret database path: {tmp_path}")

    monkeypatch.setattr(duckdb, "connect", fail_to_connect)
    with bind_log_context(request_id="req-health-102"):
        with pytest.raises(DatabaseUnavailableError):
            check_database_health(tmp_path / "imports.duckdb")

    event = json.loads(output.getvalue())
    assert event["event"] == "database.health_check_failed"
    assert event["level"] == "ERROR"
    assert event["request_id"] == "req-health-102"
    assert event["database_status"] == "unavailable"
    assert event["exception"]["type"] == "IOException"
    assert str(tmp_path) not in output.getvalue()
