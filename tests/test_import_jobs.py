import pytest

from app.core.import_jobs import (
    IllegalImportTransition,
    ImportJobNotFound,
    ImportState,
    InMemoryImportJobRepository,
)


def test_import_job_follows_the_successful_lifecycle() -> None:
    repository = InMemoryImportJobRepository()

    uploaded = repository.create(
        import_id="import-1",
        stored_filename="import-1.json",
        checksum="abc123",
        bytes_received=42,
    )
    queued = repository.transition("import-1", ImportState.queued)
    processing = repository.transition("import-1", ImportState.processing)
    completed = repository.transition(
        "import-1",
        ImportState.completed,
        total_records=2,
        accepted_records=2,
        rejected_records=0,
    )

    assert uploaded.state is ImportState.uploaded
    assert queued.state is ImportState.queued
    assert processing.state is ImportState.processing
    assert completed.state is ImportState.completed
    assert completed.total_records == 2
    assert completed.accepted_records == 2
    assert completed.rejected_records == 0


def test_import_job_rejects_an_illegal_transition_without_changing_state() -> None:
    repository = InMemoryImportJobRepository()
    repository.create(
        import_id="import-1",
        stored_filename="import-1.json",
        checksum="abc123",
        bytes_received=42,
    )
    repository.transition("import-1", ImportState.queued)

    with pytest.raises(IllegalImportTransition):
        repository.transition("import-1", ImportState.completed)

    assert repository.get("import-1").state is ImportState.queued


def test_in_memory_job_state_is_lost_when_the_process_repository_is_recreated() -> None:
    repository_before_restart = InMemoryImportJobRepository()
    repository_before_restart.create(
        import_id="import-1",
        stored_filename="import-1.json",
        checksum="abc123",
        bytes_received=42,
    )

    repository_after_restart = InMemoryImportJobRepository()

    with pytest.raises(ImportJobNotFound):
        repository_after_restart.get("import-1")
