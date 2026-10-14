from pathlib import Path

import duckdb
import pytest

from app.core.batch_loading import (
    BatchLoadError,
    BatchLoadResult,
    BatchReplayConflict,
    DuckDBBatchLoader,
    PermanentBatchLoadError,
    RetryableBatchLoadError,
)


def test_batch_loader_rejects_progress_interval_below_volume_bound(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ValueError,
        match="progress_log_interval must be at least 100000",
    ):
        DuckDBBatchLoader(
            tmp_path / "imports.duckdb",
            progress_log_interval=99_999,
        )


GOLDEN_PROCESSED = Path(__file__).parent / "golden" / "day6" / "processed.ndjson"


def test_duckdb_batch_loader_commits_once_and_skips_an_identical_replay(
    tmp_path: Path,
) -> None:
    loader = DuckDBBatchLoader(tmp_path / "imports.duckdb", batch_size=1)

    first = loader.load(GOLDEN_PROCESSED, import_id="import-1")
    replay = loader.load(GOLDEN_PROCESSED, import_id="import-1")

    assert first == BatchLoadResult(
        import_id="import-1",
        records_seen=1,
        records_inserted=1,
        batches_committed=1,
        batches_skipped=0,
    )
    assert replay == BatchLoadResult(
        import_id="import-1",
        records_seen=1,
        records_inserted=0,
        batches_committed=0,
        batches_skipped=1,
    )


def test_duckdb_batch_loader_stores_derived_order_metrics(tmp_path: Path) -> None:
    database_path = tmp_path / "imports.duckdb"
    loader = DuckDBBatchLoader(database_path)

    loader.load(GOLDEN_PROCESSED, import_id="import-1")

    with duckdb.connect(str(database_path), read_only=True) as connection:
        stored_metrics = connection.execute(
            """
            SELECT item_count, total_quantity
            FROM accepted_orders
            WHERE import_id = 'import-1'
            """
        ).fetchone()

    assert stored_metrics == (1, 2)


def test_duckdb_batch_loader_upgrades_the_day5_order_table(tmp_path: Path) -> None:
    database_path = tmp_path / "imports.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE accepted_orders (
                import_id VARCHAR NOT NULL,
                record_number BIGINT NOT NULL,
                order_id VARCHAR NOT NULL,
                customer_email VARCHAR NOT NULL,
                amount VARCHAR NOT NULL,
                currency VARCHAR NOT NULL,
                ordered_at VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                PRIMARY KEY (import_id, record_number)
            )
            """
        )

    loader = DuckDBBatchLoader(database_path)
    loader.load(GOLDEN_PROCESSED, import_id="import-1")

    with duckdb.connect(str(database_path), read_only=True) as connection:
        stored_metrics = connection.execute(
            """
            SELECT item_count, total_quantity
            FROM accepted_orders
            WHERE import_id = 'import-1'
            """
        ).fetchone()

    assert stored_metrics == (1, 2)


def test_duckdb_batch_loader_rejects_inconsistent_derived_metrics(
    tmp_path: Path,
) -> None:
    source = tmp_path / "wrong-metrics.ndjson"
    source.write_text(
        GOLDEN_PROCESSED.read_text(encoding="utf-8").replace(
            '"total_quantity":2',
            '"total_quantity":99',
        ),
        encoding="utf-8",
    )
    loader = DuckDBBatchLoader(tmp_path / "imports.duckdb")

    with pytest.raises(PermanentBatchLoadError, match="derived metrics"):
        loader.load(source, import_id="import-1")


def test_duckdb_batch_loader_splits_batches_at_the_byte_budget(
    tmp_path: Path,
) -> None:
    template = GOLDEN_PROCESSED.read_text(encoding="utf-8").strip()
    first = template.replace('"record_number":3', '"record_number":1')
    second = template.replace('"record_number":3', '"record_number":2')
    source = tmp_path / "two-records.ndjson"
    source.write_text(f"{first}\n{second}\n", encoding="utf-8")
    one_line_bytes = len(f"{first}\n".encode())
    loader = DuckDBBatchLoader(
        tmp_path / "imports.duckdb",
        batch_size=10,
        maximum_batch_bytes=one_line_bytes,
    )

    result = loader.load(source, import_id="import-1")

    assert result.records_inserted == 2
    assert result.batches_committed == 2


def test_duckdb_batch_loader_rejects_a_record_over_the_byte_limit(
    tmp_path: Path,
) -> None:
    loader = DuckDBBatchLoader(
        tmp_path / "imports.duckdb",
        maximum_record_bytes=100,
    )

    with pytest.raises(BatchLoadError, match="record byte limit"):
        loader.load(GOLDEN_PROCESSED, import_id="import-1")


def test_duckdb_batch_loader_rejects_too_many_items_in_one_order(
    tmp_path: Path,
) -> None:
    import json

    envelope = json.loads(GOLDEN_PROCESSED.read_text(encoding="utf-8"))
    envelope["order"]["items"].append({"sku": "SECOND", "quantity": 1})
    source = tmp_path / "too-many-items.ndjson"
    source.write_text(json.dumps(envelope) + "\n", encoding="utf-8")
    loader = DuckDBBatchLoader(
        tmp_path / "imports.duckdb",
        maximum_items_per_order=1,
    )

    with pytest.raises(PermanentBatchLoadError, match="item limit") as captured:
        loader.load(source, import_id="import-1")

    assert captured.value.retryable is False


def test_duckdb_batch_loader_classifies_transient_database_io_as_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_to_connect(*args: object, **kwargs: object) -> None:
        raise duckdb.IOException("simulated temporary storage exhaustion")

    monkeypatch.setattr(duckdb, "connect", fail_to_connect)
    loader = DuckDBBatchLoader(tmp_path / "imports.duckdb")

    with pytest.raises(RetryableBatchLoadError) as captured:
        loader.load(GOLDEN_PROCESSED, import_id="import-1")

    assert captured.value.retryable is True


def test_duckdb_batch_loader_rolls_back_a_failed_batch_before_retry(
    tmp_path: Path,
) -> None:
    loader = DuckDBBatchLoader(tmp_path / "imports.duckdb", batch_size=2)
    invalid = tmp_path / "invalid.ndjson"
    corrected = tmp_path / "corrected.ndjson"
    template = GOLDEN_PROCESSED.read_text(encoding="utf-8").strip()
    record_one = template.replace('"record_number":3', '"record_number":1')
    record_two = template.replace('"record_number":3', '"record_number":2')
    invalid.write_text(f"{record_one}\n{record_one}\n", encoding="utf-8")
    corrected.write_text(f"{record_one}\n{record_two}\n", encoding="utf-8")

    with pytest.raises(BatchLoadError):
        loader.load(invalid, import_id="import-1")

    retry = loader.load(corrected, import_id="import-1")

    assert retry == BatchLoadResult(
        import_id="import-1",
        records_seen=2,
        records_inserted=2,
        batches_committed=1,
        batches_skipped=0,
    )


def test_duckdb_batch_loader_rejects_changed_content_for_a_committed_key(
    tmp_path: Path,
) -> None:
    loader = DuckDBBatchLoader(tmp_path / "imports.duckdb", batch_size=1)
    changed = tmp_path / "changed.ndjson"
    changed.write_text(
        GOLDEN_PROCESSED.read_text(encoding="utf-8").replace(
            '"amount":"2.35"',
            '"amount":"9.99"',
        ),
        encoding="utf-8",
    )
    loader.load(GOLDEN_PROCESSED, import_id="import-1")

    with pytest.raises(BatchReplayConflict):
        loader.load(changed, import_id="import-1")


@pytest.mark.parametrize("replay_kind", ["extended", "truncated"])
def test_duckdb_batch_loader_rejects_a_different_completed_import_shape(
    tmp_path: Path,
    replay_kind: str,
) -> None:
    loader = DuckDBBatchLoader(tmp_path / "imports.duckdb", batch_size=1)
    template = GOLDEN_PROCESSED.read_text(encoding="utf-8").strip()
    first_record = template.replace('"record_number":3', '"record_number":1')
    second_record = template.replace('"record_number":3', '"record_number":2')
    one_record = tmp_path / "one.ndjson"
    two_records = tmp_path / "two.ndjson"
    one_record.write_text(f"{first_record}\n", encoding="utf-8")
    two_records.write_text(
        f"{first_record}\n{second_record}\n",
        encoding="utf-8",
    )
    initial, replay = (
        (one_record, two_records)
        if replay_kind == "extended"
        else (two_records, one_record)
    )
    loader.load(initial, import_id="import-1")

    with pytest.raises(BatchReplayConflict):
        loader.load(replay, import_id="import-1")


def test_duckdb_batch_loader_rejects_truncated_retry_after_partial_commits(
    tmp_path: Path,
) -> None:
    loader = DuckDBBatchLoader(tmp_path / "imports.duckdb", batch_size=1)
    template = GOLDEN_PROCESSED.read_text(encoding="utf-8").strip()
    first = template.replace('"record_number":3', '"record_number":1')
    second = template.replace('"record_number":3', '"record_number":2')
    partial_failure = tmp_path / "partial-failure.ndjson"
    truncated_retry = tmp_path / "truncated-retry.ndjson"
    partial_failure.write_text(
        f"{first}\n{second}\n{first}\n",
        encoding="utf-8",
    )
    truncated_retry.write_text(f"{first}\n", encoding="utf-8")
    with pytest.raises(BatchLoadError):
        loader.load(partial_failure, import_id="import-1")

    with pytest.raises(BatchReplayConflict):
        loader.load(truncated_retry, import_id="import-1")
