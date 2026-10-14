import asyncio
import json
import logging
import re
from collections.abc import Iterator
from io import StringIO
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import main as main_module
from app.core.batch_loading import BatchLoadError, DuckDBBatchLoader
from app.core.config import Settings
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
    application_logger.setLevel(logging.NOTSET)
    application_logger.propagate = True


def test_configured_application_logger_emits_one_json_event() -> None:
    output = StringIO()
    configure_logging("INFO", stream=output)

    logging.getLogger("app.tests").info(
        "Application started",
        extra={
            "event": "application.started",
            "environment": "test",
            "configured_log_level": "INFO",
        },
    )

    event = json.loads(output.getvalue())
    assert event["level"] == "INFO"
    assert event["logger"] == "app.tests"
    assert event["event"] == "application.started"
    assert event["message"] == "Application started"
    assert event["environment"] == "test"
    assert event["configured_log_level"] == "INFO"
    assert event["timestamp"].endswith("+05:30")


def test_bound_correlation_fields_are_added_and_then_reset() -> None:
    output = StringIO()
    configure_logging("INFO", stream=output)
    logger = logging.getLogger("app.tests.correlation")

    with bind_log_context(
        request_id="req-101",
        import_id="imp-501",
        engine="polars",
        batch_number=27,
        attempt=2,
    ):
        logger.info("Inside import", extra={"event": "test.correlated"})
    logger.info("Outside import", extra={"event": "test.uncorrelated"})

    correlated, uncorrelated = [
        json.loads(line) for line in output.getvalue().splitlines()
    ]
    assert correlated["request_id"] == "req-101"
    assert correlated["import_id"] == "imp-501"
    assert correlated["engine"] == "polars"
    assert correlated["batch_number"] == 27
    assert correlated["attempt"] == 2
    assert "request_id" not in uncorrelated
    assert "import_id" not in uncorrelated


def test_concurrent_tasks_keep_their_correlation_context_isolated() -> None:
    output = StringIO()
    configure_logging("INFO", stream=output)
    logger = logging.getLogger("app.tests.concurrent_correlation")

    async def log_import(import_id: str) -> None:
        with bind_log_context(import_id=import_id):
            await asyncio.sleep(0)
            logger.info("Import event", extra={"event": "test.import"})

    async def run_concurrently() -> None:
        await asyncio.gather(log_import("imp-A"), log_import("imp-B"))

    asyncio.run(run_concurrently())

    events = [json.loads(line) for line in output.getvalue().splitlines()]
    assert {event["import_id"] for event in events} == {"imp-A", "imp-B"}


def test_correlation_context_resets_when_the_bound_scope_raises() -> None:
    output = StringIO()
    configure_logging("INFO", stream=output)
    logger = logging.getLogger("app.tests.exception_reset")

    with pytest.raises(RuntimeError):
        with bind_log_context(request_id="req-failed"):
            raise RuntimeError("simulated request failure")
    logger.info("After failure", extra={"event": "test.after_failure"})

    event = json.loads(output.getvalue())
    assert event["event"] == "test.after_failure"
    assert "request_id" not in event


def test_configured_application_logger_writes_daily_rotating_json_file(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "logs" / "application.log"
    configure_logging(
        "INFO",
        stream=StringIO(),
        file_path=log_path,
        file_backup_count=30,
    )

    logging.getLogger("app.tests.file").info(
        "Stored in the daily log",
        extra={"event": "logging.file_test"},
    )

    event = json.loads(log_path.read_text(encoding="utf-8"))
    assert event["event"] == "logging.file_test"
    assert event["message"] == "Stored in the daily log"
    file_handlers = [
        handler
        for handler in logging.getLogger("app").handlers
        if isinstance(handler, TimedRotatingFileHandler)
    ]
    assert len(file_handlers) == 1
    assert file_handlers[0].when == "MIDNIGHT"
    assert file_handlers[0].backupCount == 30
    assert file_handlers[0].utc is False
    assert file_handlers[0].suffix == "%Y-%m-%d"

    file_handlers[0].doRollover()
    logging.getLogger("app.tests.file").info(
        "Stored after rollover",
        extra={"event": "logging.after_rollover"},
    )

    archives = list(log_path.parent.glob("application.log.*"))
    assert len(archives) == 1
    assert re.fullmatch(
        r"application\.log\.\d{4}-\d{2}-\d{2}",
        archives[0].name,
    )
    current_event = json.loads(log_path.read_text(encoding="utf-8"))
    assert current_event["event"] == "logging.after_rollover"


def test_json_logging_supports_all_five_standard_levels() -> None:
    output = StringIO()
    configure_logging("DEBUG", stream=output)
    logger = logging.getLogger("app.tests.levels")

    for level in (
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL,
    ):
        logger.log(
            level,
            "Level example",
            extra={"event": "logging.level_example"},
        )

    events = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [event["level"] for event in events] == [
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    ]


def test_configured_level_filters_less_severe_events() -> None:
    output = StringIO()
    configure_logging("WARNING", stream=output)
    logger = logging.getLogger("app.tests.threshold")

    logger.info("Hidden", extra={"event": "test.hidden"})
    logger.warning("Visible warning", extra={"event": "test.warning"})
    logger.error("Visible error", extra={"event": "test.error"})

    events = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [event["event"] for event in events] == [
        "test.warning",
        "test.error",
    ]


def test_repeated_configuration_does_not_duplicate_events() -> None:
    output = StringIO()
    configure_logging("INFO", stream=output)
    configure_logging("INFO", stream=output)

    logging.getLogger("app.tests.idempotence").info(
        "Configured once",
        extra={"event": "logging.configured"},
    )

    assert len(output.getvalue().splitlines()) == 1


def test_repeated_configuration_does_not_duplicate_file_events(
    tmp_path: Path,
) -> None:
    log_path = tmp_path / "application.log"
    output = StringIO()
    configure_logging("INFO", stream=output, file_path=log_path)
    configure_logging("INFO", stream=output, file_path=log_path)

    logging.getLogger("app.tests.file_idempotence").info(
        "Stored once",
        extra={"event": "logging.file_configured"},
    )

    assert len(log_path.read_text(encoding="utf-8").splitlines()) == 1


def test_configuration_replaces_a_preexisting_console_handler() -> None:
    previous_output = StringIO()
    previous_handler = logging.StreamHandler(previous_output)
    application_logger = logging.getLogger("app")
    application_logger.addHandler(previous_handler)
    output = StringIO()

    configure_logging("INFO", stream=output)
    logging.getLogger("app.tests.handler_replacement").info(
        "Configured centrally",
        extra={"event": "logging.configured"},
    )

    assert previous_output.getvalue() == ""
    assert len(output.getvalue().splitlines()) == 1


def test_import_failure_event_keeps_its_safe_identifier() -> None:
    output = StringIO()
    configure_logging("INFO", stream=output)

    logging.getLogger("app.core.import_coordinator").error(
        "Import processing failed",
        extra={"event": "import.failed", "import_id": "import-123"},
    )

    event = json.loads(output.getvalue())
    assert event["event"] == "import.failed"
    assert event["import_id"] == "import-123"


def test_exception_event_preserves_protected_stack_frames() -> None:
    output = StringIO()
    configure_logging("INFO", stream=output)

    try:
        raise RuntimeError("buyer@example.com must not be serialized")
    except RuntimeError:
        logging.getLogger("app.core.import_coordinator").exception(
            "Import processing failed",
            extra={"event": "import.failed", "import_id": "import-123"},
        )

    event = json.loads(output.getvalue())
    assert event["exception"]["type"] == "RuntimeError"
    assert event["exception"]["frames"][-1]["file"] == "test_logging_config.py"
    assert "buyer@example.com" not in output.getvalue()
    assert str(Path(__file__).parent) not in output.getvalue()


def test_json_logging_redacts_sensitive_message_data() -> None:
    output = StringIO()
    configure_logging("INFO", stream=output)

    logging.getLogger("app.tests.redaction").error(
        "customer=buyer@example.com "
        "database_url=postgresql://admin:secret@db/orders token=abc123 "
        '\"password\":\"secret 123\" Authorization: Basic YWRtaW46c2VjcmV0 '
        'api_key=\"multi word key\"',
        extra={
            "event": "test.redaction",
            "customer_email": "second@example.com",
        },
    )

    raw_event = output.getvalue()
    event = json.loads(raw_event)
    assert "buyer@example.com" not in raw_event
    assert "second@example.com" not in raw_event
    assert "admin:secret" not in raw_event
    assert "abc123" not in raw_event
    assert "secret 123" not in raw_event
    assert "YWRtaW46c2VjcmV0" not in raw_event
    assert "multi word key" not in raw_event
    assert "customer_email" not in event
    assert "[REDACTED" in event["message"]


def test_application_startup_emits_the_catalogued_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = StringIO()
    log_file_path = tmp_path / "logs" / "application.log"
    settings = Settings(
        _env_file=None,
        environment="test",
        upload_directory=tmp_path / "uploads",
        output_directory=tmp_path / "output",
        log_file_path=log_file_path,
    )
    monkeypatch.setattr(main_module, "get_settings", lambda: settings)
    monkeypatch.setattr(
        main_module,
        "configure_logging",
        lambda level, **kwargs: configure_logging(
            level,
            stream=output,
            **kwargs,
        ),
    )

    async def run_lifespan() -> None:
        async with main_module.lifespan(main_module.app):
            pass

    asyncio.run(run_lifespan())

    event = json.loads(output.getvalue())
    assert event["event"] == "application.started"
    assert event["environment"] == "test"
    assert event["configured_log_level"] == "INFO"
    file_event = json.loads(log_file_path.read_text(encoding="utf-8"))
    assert file_event == event


def test_request_events_share_request_id_and_reset_after_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        _env_file=None,
        environment="test",
        upload_directory=tmp_path / "uploads",
        output_directory=tmp_path / "output",
        log_file_path=tmp_path / "logs" / "application.log",
    )
    monkeypatch.setattr(main_module, "get_settings", lambda: settings)

    with TestClient(main_module.app) as client:
        output = StringIO()
        configure_logging("INFO", stream=output)
        response = client.get(
            "/health/live",
            headers={"X-Request-ID": "req-test-101"},
        )
        logging.getLogger("app.tests.after_request").info(
            "After request",
            extra={"event": "test.after_request"},
        )

    assert response.status_code == 200
    events = [json.loads(line) for line in output.getvalue().splitlines()]
    request_events = [
        event for event in events if event["event"].startswith("request.")
    ]
    assert [event["event"] for event in request_events] == [
        "request.started",
        "request.completed",
    ]
    assert all(
        event["request_id"] == "req-test-101" for event in request_events
    )
    assert request_events[-1]["status_code"] == 200
    assert "request_id" not in events[-1]


def test_sensitive_client_request_id_is_replaced_before_logging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        _env_file=None,
        environment="test",
        upload_directory=tmp_path / "uploads",
        output_directory=tmp_path / "output",
        log_file_path=tmp_path / "logs" / "application.log",
    )
    monkeypatch.setattr(main_module, "get_settings", lambda: settings)

    with TestClient(main_module.app) as client:
        output = StringIO()
        configure_logging("INFO", stream=output)
        response = client.get(
            "/health/live",
            headers={"X-Request-ID": "buyer@example.com"},
        )

    assert response.status_code == 200
    assert response.headers["X-Request-ID"].startswith("req_")
    assert "buyer@example.com" not in output.getvalue()


def test_background_import_logs_request_job_engine_and_batch_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(
        _env_file=None,
        environment="test",
        upload_directory=tmp_path / "uploads",
        output_directory=tmp_path / "output",
        log_file_path=tmp_path / "logs" / "application.log",
    )
    monkeypatch.setattr(main_module, "get_settings", lambda: settings)
    canonical_record = {
        "record_number": 1,
        "order": {
            "order_id": "ORD-1",
            "customer_email": "buyer@example.com",
            "amount": "10.00",
            "currency": "INR",
            "ordered_at": "2026-10-05T04:30:00+00:00",
            "status": "pending",
            "items": [{"sku": "SKU-1", "quantity": 2}],
        },
    }

    with TestClient(main_module.app) as client:
        output = StringIO()
        configure_logging("INFO", stream=output)
        response = client.post(
            "/imports",
            headers={"X-Request-ID": "req-import-101"},
            files={
                "file": (
                    "orders.ndjson",
                    f"{json.dumps(canonical_record)}\n".encode(),
                    "application/x-ndjson",
                )
            },
        )

    assert response.status_code == 202
    import_id = response.json()["import_id"]
    events = [json.loads(line) for line in output.getvalue().splitlines()]
    by_name = {event["event"]: event for event in events}
    assert {
        "import.queued",
        "import.started",
        "engine.started",
        "engine.completed",
        "batch.loading_completed",
        "import.completed",
    } <= by_name.keys()
    for event_name in (
        "import.queued",
        "import.started",
        "engine.started",
        "engine.completed",
        "import.completed",
    ):
        assert by_name[event_name]["request_id"] == "req-import-101"
        assert by_name[event_name]["import_id"] == import_id
    assert by_name["engine.started"]["engine"] == "python"
    assert by_name["batch.loading_completed"]["engine"] == "duckdb"


def test_batch_progress_events_are_interval_bounded_and_correlated(
    tmp_path: Path,
) -> None:
    source = tmp_path / "processed.ndjson"
    records = []
    for record_number in range(1, 6):
        records.append(
            {
                "record_number": record_number,
                "item_count": 1,
                "total_quantity": 2,
                "order": {
                    "order_id": f"ORD-{record_number}",
                    "customer_email": "buyer@example.com",
                    "amount": "10.00",
                    "currency": "INR",
                    "ordered_at": "2026-10-05T04:30:00+00:00",
                    "status": "pending",
                    "items": [{"sku": "SKU-1", "quantity": 2}],
                },
            }
        )
    source.write_text(
        "".join(f"{json.dumps(record)}\n" for record in records),
        encoding="utf-8",
    )
    output = StringIO()
    configure_logging("INFO", stream=output)

    loader = DuckDBBatchLoader(
        tmp_path / "imports.duckdb",
        batch_size=1,
    )
    # Exercise threshold crossings with a compact fixture after separately
    # validating the public constructor's production minimum.
    loader.progress_log_interval = 2
    loader.load(source, import_id="imp-progress")

    events = [json.loads(line) for line in output.getvalue().splitlines()]
    progress = [event for event in events if event["event"] == "batch.progress"]
    assert [event["records_processed"] for event in progress] == [2, 4]
    assert [event["batch_number"] for event in progress] == [2, 4]
    assert all(event["import_id"] == "imp-progress" for event in progress)
    assert all(event["engine"] == "duckdb" for event in progress)
    assert all(event["attempt"] == 1 for event in progress)


def test_failed_and_retried_batch_timeline_is_reconstructable(
    tmp_path: Path,
) -> None:
    valid = {
        "record_number": 1,
        "item_count": 1,
        "total_quantity": 2,
        "order": {
            "order_id": "ORD-1",
            "customer_email": "buyer@example.com",
            "amount": "10.00",
            "currency": "INR",
            "ordered_at": "2026-10-05T04:30:00+00:00",
            "status": "pending",
            "items": [{"sku": "SKU-1", "quantity": 2}],
        },
    }
    invalid = json.loads(json.dumps(valid))
    invalid["record_number"] = 2
    invalid["total_quantity"] = 99
    source = tmp_path / "invalid.ndjson"
    source.write_text(
        f"{json.dumps(valid)}\n{json.dumps(invalid)}\n",
        encoding="utf-8",
    )
    output = StringIO()
    configure_logging("INFO", stream=output)
    loader = DuckDBBatchLoader(
        tmp_path / "imports.duckdb",
        batch_size=1,
    )

    with pytest.raises(BatchLoadError):
        loader.load(source, import_id="imp-failed", attempt=1)

    corrected = tmp_path / "corrected.ndjson"
    invalid["total_quantity"] = 2
    corrected.write_text(
        f"{json.dumps(valid)}\n{json.dumps(invalid)}\n",
        encoding="utf-8",
    )
    loader.load(corrected, import_id="imp-failed", attempt=2)

    events = [json.loads(line) for line in output.getvalue().splitlines()]
    failed = next(event for event in events if event["event"] == "batch.failed")
    assert failed["import_id"] == "imp-failed"
    assert failed["engine"] == "duckdb"
    assert failed["batch_number"] == 2
    assert failed["attempt"] == 1
    assert failed["exception"]["type"] == "PermanentBatchLoadError"
    completed = [
        event for event in events if event["event"] == "batch.loading_completed"
    ][-1]
    assert completed["attempt"] == 2
    assert completed["batches_skipped"] == 1
    assert completed["batches_committed"] == 1
