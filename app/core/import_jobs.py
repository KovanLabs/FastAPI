"""Import-job state and repository seam for deferred processing."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from threading import Lock
from typing import Protocol


class ImportState(StrEnum):
    uploaded = "uploaded"
    queued = "queued"
    processing = "processing"
    completed = "completed"
    completed_with_errors = "completed_with_errors"
    failed = "failed"


TERMINAL_IMPORT_STATES = frozenset(
    {
        ImportState.completed,
        ImportState.completed_with_errors,
        ImportState.failed,
    }
)

_ALLOWED_TRANSITIONS = {
    ImportState.uploaded: frozenset({ImportState.queued}),
    ImportState.queued: frozenset({ImportState.processing}),
    ImportState.processing: TERMINAL_IMPORT_STATES,
    ImportState.completed: frozenset(),
    ImportState.completed_with_errors: frozenset(),
    ImportState.failed: frozenset(),
}


@dataclass(frozen=True)
class ImportJob:
    import_id: str
    stored_filename: str
    checksum: str
    bytes_received: int
    state: ImportState
    created_at: datetime
    updated_at: datetime
    total_records: int = 0
    accepted_records: int = 0
    rejected_records: int = 0
    failure_code: str | None = None
    failure_message: str | None = None


class ImportJobError(RuntimeError):
    """Base class for import-job repository failures."""


class ImportJobAlreadyExists(ImportJobError):
    """Raised when an import identifier is created more than once."""


class ImportJobNotFound(ImportJobError):
    """Raised when an import identifier has no job record."""


class IllegalImportTransition(ImportJobError):
    """Raised when a job attempts an undefined state transition."""


class ImportJobRepository(Protocol):
    def create(
        self,
        *,
        import_id: str,
        stored_filename: str,
        checksum: str,
        bytes_received: int,
    ) -> ImportJob: ...

    def get(self, import_id: str) -> ImportJob: ...

    def transition(
        self,
        import_id: str,
        target: ImportState,
        *,
        total_records: int | None = None,
        accepted_records: int | None = None,
        rejected_records: int | None = None,
        failure_code: str | None = None,
        failure_message: str | None = None,
    ) -> ImportJob: ...


class InMemoryImportJobRepository:
    """Thread-safe course repository; state is lost when the process exits."""

    def __init__(self) -> None:
        self._jobs: dict[str, ImportJob] = {}
        self._lock = Lock()

    def create(
        self,
        *,
        import_id: str,
        stored_filename: str,
        checksum: str,
        bytes_received: int,
    ) -> ImportJob:
        now = datetime.now(UTC)
        job = ImportJob(
            import_id=import_id,
            stored_filename=stored_filename,
            checksum=checksum,
            bytes_received=bytes_received,
            state=ImportState.uploaded,
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            if import_id in self._jobs:
                raise ImportJobAlreadyExists(import_id)
            self._jobs[import_id] = job
        return job

    def get(self, import_id: str) -> ImportJob:
        with self._lock:
            job = self._jobs.get(import_id)
        if job is None:
            raise ImportJobNotFound(import_id)
        return job

    def transition(
        self,
        import_id: str,
        target: ImportState,
        *,
        total_records: int | None = None,
        accepted_records: int | None = None,
        rejected_records: int | None = None,
        failure_code: str | None = None,
        failure_message: str | None = None,
    ) -> ImportJob:
        with self._lock:
            current = self._jobs.get(import_id)
            if current is None:
                raise ImportJobNotFound(import_id)
            if target not in _ALLOWED_TRANSITIONS[current.state]:
                raise IllegalImportTransition(
                    f"cannot transition import from {current.state} to {target}"
                )
            updated = replace(
                current,
                state=target,
                updated_at=datetime.now(UTC),
                total_records=(
                    current.total_records
                    if total_records is None
                    else total_records
                ),
                accepted_records=(
                    current.accepted_records
                    if accepted_records is None
                    else accepted_records
                ),
                rejected_records=(
                    current.rejected_records
                    if rejected_records is None
                    else rejected_records
                ),
                failure_code=failure_code,
                failure_message=failure_message,
            )
            self._jobs[import_id] = updated
        return updated
