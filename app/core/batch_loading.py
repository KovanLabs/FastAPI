"""Transactional, idempotent loading of canonical accepted order batches."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class BatchLoadResult:
    import_id: str
    records_seen: int
    records_inserted: int
    batches_committed: int
    batches_skipped: int


class BatchLoadError(RuntimeError):
    """Raised when a canonical batch cannot be loaded atomcally."""

    retryable = False


class PermanentBatchLoadError(BatchLoadError):
    """A retry with unchanged input and configuration cannot succeed."""


class RetryableBatchLoadError(BatchLoadError):
    """An infrastructure condition may allow a later retry to succeed."""

    retryable = True


class BatchReplayConflict(PermanentBatchLoadError):
    """Raised when an idempotency key is replayed with different content."""


_RETRYABLE_DUCKDB_EXCEPTIONS = (
    duckdb.ConnectionException,
    duckdb.IOException,
    duckdb.InterruptException,
    duckdb.OutOfMemoryException,
    duckdb.SerializationException,
    duckdb.TransactionException,
)


class DuckDBBatchLoader:
    """Load canonical NDJSON through row- and byte-bounded transactions."""

    def __init__(
        self,
        database_path: Path,
        *,
        batch_size: int = 5_000,
        maximum_batch_bytes: int = 16 * 1024 * 1024,
        maximum_record_bytes: int = 1024 * 1024,
        maximum_items_per_order: int = 1_000,
        memory_limit: str = "512MB",
        threads: int = 2,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if maximum_batch_bytes <= 0:
            raise ValueError("maximum_batch_bytes must be positive")
        if maximum_record_bytes <= 0:
            raise ValueError("maximum_record_bytes must be positive")
        if maximum_items_per_order <= 0:
            raise ValueError("maximum_items_per_order must be positive")
        if threads <= 0:
            raise ValueError("threads must be positive")
        self.database_path = database_path
        self.batch_size = batch_size
        self.maximum_batch_bytes = maximum_batch_bytes
        self.maximum_record_bytes = maximum_record_bytes
        self.maximum_items_per_order = maximum_items_per_order
        self.memory_limit = memory_limit
        self.threads = threads

    def load(self, input_path: Path, *, import_id: str) -> BatchLoadResult:
        if not import_id:
            raise ValueError("import_id must not be empty")

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        temp_directory = self.database_path.parent / f".{self.database_path.name}.tmp"
        records_seen = 0
        records_inserted = 0
        batches_committed = 0
        batches_skipped = 0
        input_digest = hashlib.sha256()
        total_batches = 0

        try:
            with duckdb.connect(str(self.database_path)) as connection:
                connection.execute("SET memory_limit = ?", [self.memory_limit])
                connection.execute("SET threads = ?", [self.threads])
                connection.execute("SET temp_directory = ?", [str(temp_directory)])
                _create_tables(connection)

                completed = connection.execute(
                    """
                    SELECT input_checksum, total_batches, record_count
                    FROM completed_imports
                    WHERE import_id = ?
                    """,
                    [import_id],
                ).fetchone()
                if completed is not None:
                    checksum, batch_count, record_count = _input_manifest(
                        input_path,
                        self.batch_size,
                        self.maximum_batch_bytes,
                        self.maximum_record_bytes,
                    )
                    if completed != (checksum, batch_count, record_count):
                        raise BatchReplayConflict(
                            "completed import was replayed with different content"
                        )
                    return BatchLoadResult(
                        import_id=import_id,
                        records_seen=record_count,
                        records_inserted=0,
                        batches_committed=0,
                        batches_skipped=batch_count,
                    )

                for batch_number, batch in enumerate(
                    _iter_line_batches(
                        input_path,
                        self.batch_size,
                        self.maximum_batch_bytes,
                        self.maximum_record_bytes,
                    ),
                    start=1,
                ):
                    total_batches = batch_number
                    records_seen += len(batch)
                    for line in batch:
                        input_digest.update(line.encode("utf-8"))
                    checksum = _batch_checksum(batch)
                    existing = connection.execute(
                        """
                        SELECT batch_checksum, record_count
                        FROM import_batches
                        WHERE import_id = ? AND batch_number = ?
                        """,
                        [import_id, batch_number],
                    ).fetchone()
                    if existing is not None:
                        if existing != (checksum, len(batch)):
                            raise BatchReplayConflict(
                                "batch idempotency key was replayed with different content"
                            )
                        batches_skipped += 1
                        continue

                    connection.execute("BEGIN TRANSACTION")
                    try:
                        _insert_batch(
                            connection,
                            import_id=import_id,
                            batch=batch,
                            maximum_items_per_order=self.maximum_items_per_order,
                        )
                        connection.execute(
                            """
                            INSERT INTO import_batches(
                                import_id,
                                batch_number,
                                batch_checksum,
                                record_count
                            ) VALUES (?, ?, ?, ?)
                            """,
                            [import_id, batch_number, checksum, len(batch)],
                        )
                        connection.execute("COMMIT")
                    except BaseException:
                        connection.execute("ROLLBACK")
                        raise

                    records_inserted += len(batch)
                    batches_committed += 1

                ledger_shape = connection.execute(
                    """
                    SELECT
                        count(*),
                        coalesce(max(batch_number), 0),
                        coalesce(sum(record_count), 0)
                    FROM import_batches
                    WHERE import_id = ?
                    """,
                    [import_id],
                ).fetchone()
                if ledger_shape != (
                    total_batches,
                    total_batches,
                    records_seen,
                ):
                    raise BatchReplayConflict(
                        "retry does not cover every previously committed batch"
                    )

                connection.execute("BEGIN TRANSACTION")
                try:
                    connection.execute(
                        """
                        INSERT INTO completed_imports(
                            import_id,
                            input_checksum,
                            total_batches,
                            record_count
                        ) VALUES (?, ?, ?, ?)
                        """,
                        [
                            import_id,
                            input_digest.hexdigest(),
                            total_batches,
                            records_seen,
                        ],
                    )
                    connection.execute("COMMIT")
                except BaseException:
                    connection.execute("ROLLBACK")
                    raise
        except BatchLoadError:
            raise
        except _RETRYABLE_DUCKDB_EXCEPTIONS as exc:
            raise RetryableBatchLoadError(
                "DuckDB batch loading hit a retryable infrastructure failure"
            ) from exc
        except Exception as exc:
            raise PermanentBatchLoadError("DuckDB batch loading failed") from exc

        return BatchLoadResult(
            import_id=import_id,
            records_seen=records_seen,
            records_inserted=records_inserted,
            batches_committed=batches_committed,
            batches_skipped=batches_skipped,
        )

    def discard_import(self, import_id: str) -> None:
        """Compensate a failed coordinator step by deleting this import."""

        if not self.database_path.exists():
            return
        with duckdb.connect(str(self.database_path)) as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute(
                    "DELETE FROM accepted_items WHERE import_id = ?",
                    [import_id],
                )
                connection.execute(
                    "DELETE FROM accepted_orders WHERE import_id = ?",
                    [import_id],
                )
                connection.execute(
                    "DELETE FROM import_batches WHERE import_id = ?",
                    [import_id],
                )
                connection.execute(
                    "DELETE FROM completed_imports WHERE import_id = ?",
                    [import_id],
                )
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise


def _create_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS import_batches (
            import_id VARCHAR NOT NULL,
            batch_number BIGINT NOT NULL,
            batch_checksum VARCHAR NOT NULL,
            record_count BIGINT NOT NULL,
            PRIMARY KEY (import_id, batch_number)
        );

        CREATE TABLE IF NOT EXISTS completed_imports (
            import_id VARCHAR PRIMARY KEY,
            input_checksum VARCHAR NOT NULL,
            total_batches BIGINT NOT NULL,
            record_count BIGINT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS accepted_orders (
            import_id VARCHAR NOT NULL,
            record_number BIGINT NOT NULL,
            order_id VARCHAR NOT NULL,
            customer_email VARCHAR NOT NULL,
            amount VARCHAR NOT NULL,
            currency VARCHAR NOT NULL,
            ordered_at VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            PRIMARY KEY (import_id, record_number)
        );

        CREATE TABLE IF NOT EXISTS accepted_items (
            import_id VARCHAR NOT NULL,
            record_number BIGINT NOT NULL,
            item_position BIGINT NOT NULL,
            sku VARCHAR NOT NULL,
            quantity BIGINT NOT NULL,
            PRIMARY KEY (import_id, record_number, item_position)
        );
        """
    )


def _iter_line_batches(
    path: Path,
    batch_size: int,
    maximum_batch_bytes: int,
    maximum_record_bytes: int,
) -> Iterator[list[str]]:
    batch: list[str] = []
    batch_bytes = 0
    with path.open("rb") as source:
        while raw_line := source.readline(maximum_record_bytes + 3):
            newline_bytes = 0
            if raw_line.endswith(b"\n"):
                newline_bytes = 1
                if raw_line.endswith(b"\r\n"):
                    newline_bytes = 2
            if len(raw_line) - newline_bytes > maximum_record_bytes:
                raise PermanentBatchLoadError(
                    "canonical record exceeds the configured record byte limit"
                )
            line = raw_line.decode("utf-8")
            line_bytes = len(raw_line)
            if line_bytes > maximum_batch_bytes:
                raise PermanentBatchLoadError(
                    "canonical record exceeds the configured batch byte budget"
                )
            if batch and batch_bytes + line_bytes > maximum_batch_bytes:
                yield batch
                batch = []
                batch_bytes = 0
            batch.append(line)
            batch_bytes += line_bytes
            if len(batch) == batch_size or batch_bytes == maximum_batch_bytes:
                yield batch
                batch = []
                batch_bytes = 0
    if batch:
        yield batch


def _batch_checksum(batch: list[str]) -> str:
    digest = hashlib.sha256()
    for line in batch:
        digest.update(line.encode("utf-8"))
    return digest.hexdigest()


def _input_manifest(
    path: Path,
    batch_size: int,
    maximum_batch_bytes: int,
    maximum_record_bytes: int,
) -> tuple[str, int, int]:
    digest = hashlib.sha256()
    record_count = 0
    batch_count = 0
    for batch_count, batch in enumerate(
        _iter_line_batches(
            path,
            batch_size,
            maximum_batch_bytes,
            maximum_record_bytes,
        ),
        start=1,
    ):
        record_count += len(batch)
        for line in batch:
            digest.update(line.encode("utf-8"))
    return digest.hexdigest(), batch_count, record_count


def _insert_batch(
    connection: duckdb.DuckDBPyConnection,
    *,
    import_id: str,
    batch: list[str],
    maximum_items_per_order: int,
) -> None:
    order_rows: list[tuple[object, ...]] = []
    item_rows: list[tuple[object, ...]] = []

    for line in batch:
        envelope = json.loads(line)
        record_number = envelope["record_number"]
        order = envelope["order"]
        items = order["items"]
        if len(items) > maximum_items_per_order:
            raise PermanentBatchLoadError(
                "canonical order exceeds the configured item limit"
            )
        order_rows.append(
            (
                import_id,
                record_number,
                order["order_id"],
                order["customer_email"],
                order["amount"],
                order["currency"],
                order["ordered_at"],
                order["status"],
            )
        )
        item_rows.extend(
            (
                import_id,
                record_number,
                position,
                item["sku"],
                item["quantity"],
            )
            for position, item in enumerate(items, start=1)
        )

    connection.executemany(
        """
        INSERT INTO accepted_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        order_rows,
    )
    if item_rows:
        connection.executemany(
            """
            INSERT INTO accepted_items VALUES (?, ?, ?, ?, ?)
            """,
            item_rows,
        )
