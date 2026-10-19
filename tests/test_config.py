from pathlib import Path

import pytest
from pydantic import ValidationError

from app.core.config import LogLevel, ProcessingEngine, Settings


def test_processing_engine_defaults_to_low_memory_reference() -> None:
    settings = Settings(_env_file=None)

    assert settings.processing_engine == "python"


def test_processing_engine_can_be_selected_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_PROCESSING_ENGINE", "polars")

    settings = Settings(_env_file=None)

    assert settings.processing_engine is ProcessingEngine.polars


def test_log_level_defaults_to_info_and_can_be_selected_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert Settings(_env_file=None).log_level is LogLevel.info

    monkeypatch.setenv("APP_LOG_LEVEL", "DEBUG")

    assert Settings(_env_file=None).log_level is LogLevel.debug


def test_log_file_defaults_and_retention_can_be_configured_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = Settings(_env_file=None)
    assert settings.log_file_path == Path("logs/application.log")
    assert settings.log_retention_days == 30

    monkeypatch.setenv("APP_LOG_FILE_PATH", "var/log/orders.jsonl")
    monkeypatch.setenv("APP_LOG_RETENTION_DAYS", "14")

    configured = Settings(_env_file=None)
    assert configured.log_file_path == Path("var/log/orders.jsonl")
    assert configured.log_retention_days == 14


def test_import_batches_have_record_byte_and_item_limits() -> None:
    settings = Settings(_env_file=None)

    assert settings.import_batch_size == 5_000
    assert settings.import_batch_bytes == 16 * 1024 * 1024
    assert settings.maximum_record_bytes == 1024 * 1024
    assert settings.maximum_items_per_order == 1_000
    assert settings.batch_max_attempts == 2


def test_batch_retry_attempts_are_bounded() -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, batch_max_attempts=0)
    with pytest.raises(ValidationError):
        Settings(_env_file=None, batch_max_attempts=11)


def test_progress_logging_is_bounded_to_100_events_per_10_million_records() -> None:
    settings = Settings(_env_file=None)

    assert settings.progress_log_interval == 100_000
    assert 10_000_000 // settings.progress_log_interval == 100

    with pytest.raises(ValidationError):
        Settings(_env_file=None, progress_log_interval=99_999)


@pytest.mark.parametrize(
    "field",
    [
        "maximum_upload_bytes",
        "upload_chunk_bytes",
        "maximum_record_bytes",
        "import_batch_size",
        "import_batch_bytes",
        "maximum_items_per_order",
        "log_retention_days",
    ],
)
def test_size_and_batch_limits_must_be_positive(field: str) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: 0})


def test_batch_byte_budget_must_fit_one_maximum_sized_record() -> None:
    with pytest.raises(
        ValidationError,
        match="import_batch_bytes must be at least maximum_record_bytes plus 2",
    ):
        Settings(
            _env_file=None,
            maximum_record_bytes=100,
            import_batch_bytes=101,
        )
