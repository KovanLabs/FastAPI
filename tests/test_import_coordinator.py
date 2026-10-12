import json
from pathlib import Path

import duckdb
import pytest

import app.core.import_coordinator as import_coordinator
from app.core.batch_loading import DuckDBBatchLoader
from app.core.import_coordinator import ImportCoordinator
from app.core.import_jobs import (
    IllegalImportTransition,
    ImportState,
    InMemoryImportJobRepository,
)
from app.core.processing_adapters import StandardPythonAdapter


GOLDEN_ACCEPTED = Path(__file__).parent / "golden" / "day5" / "accepted.ndjson"


def test_import_coordinator_processes_and_loads_a_queued_json_import(
    tmp_path: Path,
) -> None:
    source = tmp_path / "import-1.json"
    source.write_text(
        json.dumps(
            [
                {
                    "order_id": " ord-1 ",
                    "customer_email": "USER@EXAMPLE.COM",
                    "amount": "10.005",
                    "currency": " inr ",
                    "ordered_at": "2026-10-05T04:30:00+00:00",
                    "status": " PENDING ",
                    "items": [{"sku": " sku-1 ", "quantity": 2}],
                }
            ]
        ),
        encoding="utf-8",
    )
    repository = InMemoryImportJobRepository()
    repository.create(
        import_id="import-1",
        stored_filename=source.name,
        checksum="abc123",
        bytes_received=source.stat().st_size,
    )
    repository.transition("import-1", ImportState.queued)
    output_directory = tmp_path / "output"
    coordinator = ImportCoordinator(
        repository=repository,
        adapter=StandardPythonAdapter(),
        batch_loader=DuckDBBatchLoader(
            tmp_path / "imports.duckdb",
            batch_size=1,
        ),
        output_directory=output_directory,
        supported_currencies=frozenset({"INR", "USD", "EUR"}),
    )

    completed = coordinator.run("import-1", source)

    assert completed.state is ImportState.completed
    assert completed.total_records == 1
    assert completed.accepted_records == 1
    assert completed.rejected_records == 0
    assert (output_directory / "import-1" / "accepted.ndjson").exists()
    assert (output_directory / "import-1" / "processed.ndjson").exists()
    assert (output_directory / "import-1" / "summary.json").exists()

    with pytest.raises(IllegalImportTransition):
        coordinator.run("import-1", source)
    assert repository.get("import-1").state is ImportState.completed


def test_import_coordinator_marks_malformed_input_failed_and_cleans_partials(
    tmp_path: Path,
) -> None:
    source = tmp_path / "import-1.json"
    source.write_text('[{"order_id":"unfinished"}', encoding="utf-8")
    repository = InMemoryImportJobRepository()
    repository.create(
        import_id="import-1",
        stored_filename=source.name,
        checksum="abc123",
        bytes_received=source.stat().st_size,
    )
    repository.transition("import-1", ImportState.queued)
    output_directory = tmp_path / "output"
    coordinator = ImportCoordinator(
        repository=repository,
        adapter=StandardPythonAdapter(),
        batch_loader=DuckDBBatchLoader(tmp_path / "imports.duckdb"),
        output_directory=output_directory,
        supported_currencies=frozenset({"INR", "USD", "EUR"}),
    )

    failed = coordinator.run("import-1", source)

    assert failed.state is ImportState.failed
    assert failed.failure_code == "processing_failed"
    assert failed.failure_message == "Import processing failed."
    assert not list(output_directory.rglob("*.part"))
    assert not (output_directory / "import-1" / "summary.json").exists()


def test_import_coordinator_completes_with_errors_when_records_are_rejected(
    tmp_path: Path,
) -> None:
    source = tmp_path / "import-1.json"
    source.write_text(
        json.dumps(
            [
                {
                    "order_id": "ORD-1",
                    "customer_email": "user@example.com",
                    "amount": "10.00",
                    "currency": "INR",
                    "ordered_at": "2026-10-05T04:30:00+00:00",
                    "status": "pending",
                    "items": [{"sku": "SKU-1", "quantity": 1}],
                },
                {"order_id": "missing-required-fields"},
            ]
        ),
        encoding="utf-8",
    )
    repository = InMemoryImportJobRepository()
    repository.create(
        import_id="import-1",
        stored_filename=source.name,
        checksum="abc123",
        bytes_received=source.stat().st_size,
    )
    repository.transition("import-1", ImportState.queued)
    output_directory = tmp_path / "output"
    coordinator = ImportCoordinator(
        repository=repository,
        adapter=StandardPythonAdapter(),
        batch_loader=DuckDBBatchLoader(tmp_path / "imports.duckdb"),
        output_directory=output_directory,
        supported_currencies=frozenset({"INR", "USD", "EUR"}),
    )

    completed = coordinator.run("import-1", source)

    assert completed.state is ImportState.completed_with_errors
    assert completed.total_records == 2
    assert completed.accepted_records == 1
    assert completed.rejected_records == 1


def test_import_coordinator_removes_processed_output_when_loading_fails(
    tmp_path: Path,
) -> None:
    source = tmp_path / "import-1.json"
    source.write_text(
        json.dumps(
            [
                {
                    "order_id": "ORD-1",
                    "customer_email": "user@example.com",
                    "amount": "10.00",
                    "currency": "INR",
                    "ordered_at": "2026-10-05T04:30:00+00:00",
                    "status": "pending",
                    "items": [{"sku": "SKU-1", "quantity": 1}],
                }
            ]
        ),
        encoding="utf-8",
    )
    repository = InMemoryImportJobRepository()
    repository.create(
        import_id="import-1",
        stored_filename=source.name,
        checksum="abc123",
        bytes_received=source.stat().st_size,
    )
    repository.transition("import-1", ImportState.queued)

    class FailingBatchLoader:
        def load(self, input_path: Path, *, import_id: str) -> None:
            raise RuntimeError("simulated load failure")

        def discard_import(self, import_id: str) -> None:
            pass

    output_directory = tmp_path / "output"
    coordinator = ImportCoordinator(
        repository=repository,
        adapter=StandardPythonAdapter(),
        batch_loader=FailingBatchLoader(),
        output_directory=output_directory,
        supported_currencies=frozenset({"INR", "USD", "EUR"}),
    )

    failed = coordinator.run("import-1", source)

    assert failed.state is ImportState.failed
    assert not (output_directory / "import-1" / "processed.ndjson").exists()


def test_import_coordinator_processes_queued_canonical_ndjson(
    tmp_path: Path,
) -> None:
    source = tmp_path / "import-1.ndjson"
    source.write_bytes(GOLDEN_ACCEPTED.read_bytes())
    repository = InMemoryImportJobRepository()
    repository.create(
        import_id="import-1",
        stored_filename=source.name,
        checksum="abc123",
        bytes_received=source.stat().st_size,
    )
    repository.transition("import-1", ImportState.queued)
    output_directory = tmp_path / "output"
    coordinator = ImportCoordinator(
        repository=repository,
        adapter=StandardPythonAdapter(),
        batch_loader=DuckDBBatchLoader(tmp_path / "imports.duckdb"),
        output_directory=output_directory,
        supported_currencies=frozenset({"INR", "USD", "EUR"}),
    )

    completed = coordinator.run_canonical_ndjson("import-1", source)

    assert completed.state is ImportState.completed
    assert completed.total_records == 1
    assert completed.accepted_records == 1
    assert completed.rejected_records == 0
    assert (
        output_directory / "import-1" / "processed.ndjson"
    ).read_bytes() == source.read_bytes()
    assert (output_directory / "import-1" / "rejected.ndjson").read_bytes() == b""


def test_import_coordinator_compensates_database_when_summary_publish_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "import-1.ndjson"
    source.write_bytes(GOLDEN_ACCEPTED.read_bytes())
    repository = InMemoryImportJobRepository()
    repository.create(
        import_id="import-1",
        stored_filename=source.name,
        checksum="abc123",
        bytes_received=source.stat().st_size,
    )
    repository.transition("import-1", ImportState.queued)
    database_path = tmp_path / "imports.duckdb"
    coordinator = ImportCoordinator(
        repository=repository,
        adapter=StandardPythonAdapter(),
        batch_loader=DuckDBBatchLoader(database_path),
        output_directory=tmp_path / "output",
        supported_currencies=frozenset({"INR", "USD", "EUR"}),
    )

    def fail_summary(*args: object, **kwargs: object) -> None:
        raise OSError("simulated summary publication failure")

    monkeypatch.setattr(import_coordinator.json, "dump", fail_summary)

    failed = coordinator.run_canonical_ndjson("import-1", source)

    assert failed.state is ImportState.failed
    with duckdb.connect(str(database_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM accepted_orders WHERE import_id = 'import-1'"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM completed_imports WHERE import_id = 'import-1'"
        ).fetchone() == (0,)
