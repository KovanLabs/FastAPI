"""Transactional, idempotent loading of canonical accepted order batches."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import duckdb

from app.core.log_context import bind_log_context
from app.core.progress_logging import ProgressEventGate


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchLoadResult:
    import_id: str
    records_seen: int
    records_inserted: int
    batches_committed: int
    batches_skipped: int


class BatchLoadError(RuntimeError):
    """Raised when a canonical batch cannot be loaded atomically."""

    retryable = False

    def __init__(
        self,
        message: str,
        *,
        batch_number: int | None = None,
    ) -> None:
        super().__init__(message)
        self.batch_number = batch_number


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
        progress_log_interval: int = 100_000,
        maximum_attempts: int = 2,
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
        if progress_log_interval < 100_000:
            raise ValueError(
                "progress_log_interval must be at least 100000"
            )
        if maximum_attempts <= 0:
            raise ValueError("maximum_attempts must be positive")
        if threads <= 0:
            raise ValueError("threads must be positive")
        self.database_path = database_path
        self.batch_size = batch_size
        self.maximum_batch_bytes = maximum_batch_bytes
        self.maximum_record_bytes = maximum_record_bytes
        self.maximum_items_per_order = maximum_items_per_order
        self.progress_log_interval = progress_log_interval
        self.maximum_attempts = maximum_attempts
        self.memory_limit = memory_limit
        self.threads = threads

    def load(
        self,
        input_path: Path,
        *,
        import_id: str,
        attempt: int = 1,
    ) -> BatchLoadResult:
        if not import_id:
            raise ValueError("import_id must not be empty")
        if attempt <= 0:
            raise ValueError("attempt must be positive")

        final_attempt = attempt + self.maximum_attempts - 1
        for current_attempt in range(attempt, final_attempt + 1):
            try:
                return self._load_attempt(
                    input_path,
                    import_id=import_id,
                    attempt=current_attempt,
                )
            except RetryableBatchLoadError as exc:
                if current_attempt == final_attempt:
                    raise
                with bind_log_context(
                    import_id=import_id,
                    engine="duckdb",
                    batch_number=exc.batch_number,
                    attempt=current_attempt,
                ):
                    logger.warning(
                        "Batch retry scheduled",
                        extra={
                            "event": "batch.retry_scheduled",
                            "next_attempt": current_attempt + 1,
                            "retryable": True,
                        },
                    )

        raise AssertionError("batch retry loop did not return or raise")

    def _load_attempt(
        self,
        input_path: Path,
        *,
        import_id: str,
        attempt: int,
    ) -> BatchLoadResult:
        with bind_log_context(
            import_id=import_id,
            engine="duckdb",
            attempt=attempt,
        ):
            logger.info(
                "Batch loading started",
                extra={"event": "batch.loading_started"},
            )
            try:
                result = self._load(input_path, import_id=import_id)
            except Exception as exc:
                failure_fields: dict[str, object] = {
                    "event": "batch.loading_failed",
                    "retryable": getattr(exc, "retryable", False),
                }
                failed_batch_number = getattr(exc, "batch_number", None)
                if failed_batch_number is not None:
                    failure_fields["batch_number"] = failed_batch_number
                logger.exception(
                    "Batch loading failed",
                    extra=failure_fields,
                )
                raise
            logger.info(
                "Batch loading completed",
                extra={
                    "event": "batch.loading_completed",
                    "records_processed": result.records_seen,
                    "batches_committed": result.batches_committed,
                    "batches_skipped": result.batches_skipped,
                },
            )
            return result

    def _load(self, input_path: Path, *, import_id: str) -> BatchLoadResult:

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        temp_directory = self.database_path.parent / f".{self.database_path.name}.tmp"
        records_seen = 0
        records_inserted = 0
        batches_committed = 0
        batches_skipped = 0
        input_digest = hashlib.sha256()
        total_batches = 0
        active_batch_number: int | None = None
        progress_events = ProgressEventGate(self.progress_log_interval)

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
                    with bind_log_context(batch_number=batch_number):
                        active_batch_number = batch_number
                        try:
                            committed = self._load_one_batch(
                                connection,
                                import_id=import_id,
                                batch_number=batch_number,
                                batch=batch,
                            )
                        except Exception:
                            logger.exception(
                                "Batch failed",
                                extra={"event": "batch.failed"},
                            )
                            raise

                        if committed:
                            records_inserted += len(batch)
                            batches_committed += 1
                        else:
                            batches_skipped += 1
                        if progress_events.crossed(records_seen):
                            logger.info(
                                "Batch loading progress",
                                extra={
                                    "event": "batch.progress",
                                    "records_processed": records_seen,
                                },
                            )
                    active_batch_number = None

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
                "DuckDB batch loading hit a retryable infrastructure failure",
                batch_number=active_batch_number,
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

    def _load_one_batch(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        import_id: str,
        batch_number: int,
        batch: list[str],
    ) -> bool:
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
            return False

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
        return True

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
            item_count BIGINT NOT NULL,
            total_quantity BIGINT NOT NULL,
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
    # Day 05 databases predate the derived metrics. Keep existing local data
    # usable while allowing all newly processed rows to store the Day 06 shape.
    connection.execute(
        "ALTER TABLE accepted_orders ADD COLUMN IF NOT EXISTS item_count BIGINT"
    )
    connection.execute(
        "ALTER TABLE accepted_orders ADD COLUMN IF NOT EXISTS total_quantity BIGINT"
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
        item_count = envelope.get("item_count")
        total_quantity = envelope.get("total_quantity")
        expected_total_quantity = sum(item["quantity"] for item in items)
        if (
            type(item_count) is not int
            or type(total_quantity) is not int
            or item_count != len(items)
            or total_quantity != expected_total_quantity
        ):
            raise PermanentBatchLoadError(
                "processed order contains inconsistent derived metrics"
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
                item_count,
                total_quantity,
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
        INSERT INTO accepted_orders(
            import_id,
            record_number,
            order_id,
            customer_email,
            amount,
            currency,
            ordered_at,
            status,
            item_count,
            total_quantity
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
