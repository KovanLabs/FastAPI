import pytest
from pydantic import ValidationError

from app.core.config import ProcessingEngine, Settings


def test_processing_engine_defaults_to_low_memory_reference() -> None:
    settings = Settings(_env_file=None)

    assert settings.processing_engine == "python"


def test_processing_engine_can_be_selected_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("APP_PROCESSING_ENGINE", "polars")

    settings = Settings(_env_file=None)

    assert settings.processing_engine is ProcessingEngine.polars


def test_import_batches_have_record_byte_and_item_limits() -> None:
    settings = Settings(_env_file=None)

    assert settings.import_batch_size == 5_000
    assert settings.import_batch_bytes == 16 * 1024 * 1024
    assert settings.maximum_record_bytes == 1024 * 1024
    assert settings.maximum_items_per_order == 1_000


@pytest.mark.parametrize(
    "field",
    [
        "maximum_upload_bytes",
        "upload_chunk_bytes",
        "maximum_record_bytes",
        "import_batch_size",
        "import_batch_bytes",
        "maximum_items_per_order",
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
