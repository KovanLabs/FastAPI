from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Lock
from time import sleep

import pytest

import app.api.orders as orders_api
from app.core.config import Settings


def test_background_imports_serialize_process_local_duckdb_writers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    start_together = Barrier(2)
    counter_lock = Lock()
    writer_lock = Lock()
    active = 0
    maximum_active = 0

    class RecordingCoordinator:
        def __init__(self, **kwargs: object) -> None:
            pass

        def run_canonical_ndjson(self, import_id: str, input_path: Path) -> None:
            nonlocal active, maximum_active
            with counter_lock:
                active += 1
                maximum_active = max(maximum_active, active)
            sleep(0.05)
            with counter_lock:
                active -= 1

    monkeypatch.setattr(orders_api, "ImportCoordinator", RecordingCoordinator)
    settings = Settings(
        _env_file=None,
        upload_directory=tmp_path / "uploads",
        output_directory=tmp_path / "output",
    )

    def run(import_id: str) -> None:
        start_together.wait(timeout=5)
        orders_api._run_import(
            object(),
            settings,
            import_id,
            tmp_path / f"{import_id}.ndjson",
            ".ndjson",
            writer_lock,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run, f"import-{number}") for number in (1, 2)]
        for future in futures:
            future.result(timeout=5)

    assert maximum_active == 1
