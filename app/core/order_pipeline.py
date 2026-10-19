"""Bounded cleaning and validation for orders in a top-level JSON array."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import BinaryIO, TextIO, TypeVar

import ijson
import polars as pl
from pydantic import TypeAdapter, ValidationError

from app.core.order_cleaning import CleaningIssue, clean_order
from app.core.parsers import InvalidJsonError
from app.core.progress_logging import ProgressEventGate
from app.schemas.orders import OrderCreate


T = TypeVar("T")
logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class ValidationIssue:
    location: tuple[str | int, ...]
    message: str
    error_type: str


@dataclass(frozen=True)
class OrderProcessingOutcome:
    record_number: int
    order: OrderCreate | None = None
    error_code: str | None = None
    issues: tuple[ValidationIssue, ...] = ()

    @property
    def accepted(self) -> bool:
        return self.order is not None


@dataclass(frozen=True)
class CleanedOrder:
    record_number: int
    value: dict[str, object]
    duplicate_order_id: bool


@dataclass(frozen=True)
class OrderPipelineSummary:
    total_records: int
    accepted_records: int
    rejected_records: int
    failures_by_code: dict[str, int]
    failure_samples: list[dict[str, object]]


@dataclass(frozen=True)
class OrderImportSummary:
    """Auditable counts produced by a completed two-pass order import."""

    total_records: int
    accepted_records: int
    rejected_records: int
    failures_by_code: dict[str, int]
    failure_samples: list[dict[str, object]]
    duplicate_index_backend: str
    sqlite_working_store_bytes: int

    def __post_init__(self) -> None:
        if self.total_records != self.accepted_records + self.rejected_records:
            raise ValueError("Order import counts do not reconcile")


def validate_order_business(
    order: OrderCreate,
    supported_currencies: Iterable[str],
) -> tuple[ValidationIssue, ...]:
    """Apply project business policy after Pydantic schema validation."""
    supported = frozenset(currency.upper() for currency in supported_currencies)
    return _business_issues(order, supported)


def _business_issues(
    order: OrderCreate,
    supported_currencies: frozenset[str],
) -> tuple[ValidationIssue, ...]:
    if order.currency in supported_currencies:
        return ()
    return (
        ValidationIssue(
            location=("currency",),
            message=f"currency {order.currency!r} is not supported",
            error_type="unsupported_currency",
        ),
    )


def process_order_file(
    input_path: Path,
    *,
    accepted_path: Path,
    rejected_path: Path,
    supported_currencies: Iterable[str],
    maximum_failure_samples: int = 20,
    commit_interval: int = 5_000,
    progress_log_interval: int = 100_000,
    maximum_record_bytes: int = 1024 * 1024,
) -> OrderImportSummary:
    """Validate JSON orders and publish two NDJSON files with rollback.

    Pass one incrementally parses, cleans, validates, and records decisions in a
    temporary SQLite database. The winners table is updated only by valid
    records, so the last valid occurrence of each canonical order ID wins.
    Pass two walks the recorded decisions in input order and writes bounded,
    deterministic accepted and rejected NDJSON streams.
    """
    if maximum_failure_samples < 0:
        raise ValueError("maximum_failure_samples must not be negative")
    if commit_interval <= 0:
        raise ValueError("commit_interval must be positive")
    if progress_log_interval < 100_000:
        raise ValueError("progress_log_interval must be at least 100000")
    if maximum_record_bytes <= 0:
        raise ValueError("maximum_record_bytes must be positive")
    if accepted_path == rejected_path:
        raise ValueError("accepted_path and rejected_path must be different")

    accepted_path.parent.mkdir(parents=True, exist_ok=True)
    rejected_path.parent.mkdir(parents=True, exist_ok=True)
    supported = frozenset(currency.upper() for currency in supported_currencies)
    adapter = TypeAdapter(OrderCreate)

    database_handle, database_name = tempfile.mkstemp(
        prefix=".order-import-",
        suffix=".sqlite3",
        dir=accepted_path.parent,
    )
    os.close(database_handle)
    database_path = Path(database_name)

    try:
        with sqlite3.connect(database_path) as connection:
            _create_import_tables(connection)
            logger.info(
                "Streaming order parsing started",
                extra={"event": "parsing.started"},
            )
            try:
                total_records = _stage_order_records(
                    connection,
                    _iter_source_orders(
                        input_path,
                        maximum_record_bytes=maximum_record_bytes,
                    ),
                    adapter=adapter,
                    supported_currencies=supported,
                    commit_interval=commit_interval,
                    progress_log_interval=progress_log_interval,
                )
            except Exception:
                logger.exception(
                    "Streaming order parsing failed",
                    extra={"event": "parsing.failed"},
                )
                raise
            logger.info(
                "Streaming order parsing completed",
                extra={
                    "event": "parsing.completed",
                    "records_processed": total_records,
                },
            )
            sqlite_working_store_bytes = _sqlite_size_bytes(connection)
            (
                accepted_records,
                rejected_records,
                failures_by_code,
                failure_samples,
            ) = _publish_order_outputs(
                connection,
                accepted_path=accepted_path,
                rejected_path=rejected_path,
                maximum_failure_samples=maximum_failure_samples,
            )
            logger.info(
                "Order validation completed",
                extra={
                    "event": "validation.completed",
                    "total_records": total_records,
                    "accepted_records": accepted_records,
                    "rejected_records": rejected_records,
                },
            )
    finally:
        database_path.unlink(missing_ok=True)

    return OrderImportSummary(
        total_records=total_records,
        accepted_records=accepted_records,
        rejected_records=rejected_records,
        failures_by_code=failures_by_code,
        failure_samples=failure_samples,
        duplicate_index_backend="sqlite",
        sqlite_working_store_bytes=sqlite_working_store_bytes,
    )


def _create_import_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE records (
            record_number INTEGER PRIMARY KEY,
            order_id TEXT,
            accepted_json TEXT,
            rejected_json TEXT
        );
        CREATE TABLE winners (
            order_id TEXT PRIMARY KEY,
            record_number INTEGER NOT NULL
        );
        """
    )


def _stage_order_records(
    connection: sqlite3.Connection,
    records: Iterable[object],
    *,
    adapter: TypeAdapter[OrderCreate],
    supported_currencies: frozenset[str],
    commit_interval: int,
    progress_log_interval: int,
) -> int:
    total_records = 0
    progress_events = ProgressEventGate(progress_log_interval)
    for record_number, raw in enumerate(records, start=1):
        total_records = record_number
        order_id: str | None = None
        accepted_json: str | None = None
        rejected_json: str | None = None

        if not isinstance(raw, dict):
            rejected_json = _rejection_json(record_number, "not_object", ())
        else:
            cleaning = clean_order(raw)
            if cleaning.issues:
                rejected_json = _rejection_json(
                    record_number,
                    "cleaning_failed",
                    cleaning.issues,
                )
            else:
                try:
                    order = adapter.validate_python(cleaning.value)
                except ValidationError as exc:
                    rejected_json = _schema_rejection_json(record_number, exc)
                else:
                    business_issues = _business_issues(
                        order,
                        supported_currencies,
                    )
                    if business_issues:
                        rejected_json = _rejection_json(
                        record_number,
                            "unsupported_currency",
                            business_issues,
                    )
                    else:
                        order_id = order.order_id
                        accepted_json = _accepted_json(record_number, order)

        connection.execute(
            """
            INSERT INTO records(
                record_number, order_id, accepted_json, rejected_json
            ) VALUES (?, ?, ?, ?)
            """,
            (record_number, order_id, accepted_json, rejected_json),
        )
        if order_id is not None:
            connection.execute(
                """
                INSERT INTO winners(order_id, record_number)
                VALUES (?, ?)
                ON CONFLICT(order_id) DO UPDATE
                SET record_number = excluded.record_number
                """,
                (order_id, record_number),
            )
        if record_number % commit_interval == 0:
            connection.commit()
        if progress_events.crossed(record_number):
            logger.info(
                "Streaming order parsing progress",
                extra={
                    "event": "parsing.progress",
                    "records_processed": record_number,
                },
            )
    connection.commit()
    return total_records


def _iter_source_orders(
    input_path: Path,
    *,
    maximum_record_bytes: int,
) -> Iterator[object]:
    del maximum_record_bytes
    with input_path.open("rb") as source:
        yield from iter_ijson_array(source)


def _publish_order_outputs(
    connection: sqlite3.Connection,
    *,
    accepted_path: Path,
    rejected_path: Path,
    maximum_failure_samples: int,
) -> tuple[int, int, dict[str, int], list[dict[str, object]]]:
    accepted_temporary = _temporary_output_path(accepted_path)
    rejected_temporary = _temporary_output_path(rejected_path)
    accepted_records = 0
    rejected_records = 0
    failures_by_code: dict[str, int] = {}
    failure_samples: list[dict[str, object]] = []

    try:
        with (
            accepted_temporary.open("x", encoding="utf-8", newline="\n") as accepted,
            rejected_temporary.open("x", encoding="utf-8", newline="\n") as rejected,
        ):
            rows = connection.execute(
                """
                SELECT
                    records.record_number,
                    records.order_id,
                    records.accepted_json,
                    records.rejected_json,
                    winners.record_number
                FROM records
                LEFT JOIN winners ON winners.order_id = records.order_id
                ORDER BY records.record_number
                """
            )
            for record_number, order_id, accepted_json, rejected_json, winner in rows:
                if rejected_json is not None:
                    rejection = json.loads(rejected_json)
                    _write_ndjson_line(rejected, rejection)
                    rejected_records += 1
                    _record_failure(
                        rejection,
                        failures_by_code,
                        failure_samples,
                        maximum_failure_samples=maximum_failure_samples,
                    )
                elif record_number == winner:
                    accepted.write(accepted_json)
                    accepted.write("\n")
                    accepted_records += 1
                else:
                    rejection = {
                        "record_number": record_number,
                        "error_code": "duplicate_order_id",
                        "issues": [
                            {
                                "code": "duplicate_order_id",
                                "location": ["order_id"],
                                "message": (
                                    "A later valid record with canonical order_id "
                                    f"{order_id!r} wins this import."
                                ),
                            }
                        ],
                    }
                    _write_ndjson_line(rejected, rejection)
                    rejected_records += 1
                    _record_failure(
                        rejection,
                        failures_by_code,
                        failure_samples,
                        maximum_failure_samples=maximum_failure_samples,
                    )

            accepted.flush()
            rejected.flush()
            os.fsync(accepted.fileno())
            os.fsync(rejected.fileno())

        _publish_output_pair(
            (
                (accepted_temporary, accepted_path),
                (rejected_temporary, rejected_path),
            )
        )
    except BaseException:
        accepted_temporary.unlink(missing_ok=True)
        rejected_temporary.unlink(missing_ok=True)
        raise

    return (
        accepted_records,
        rejected_records,
        failures_by_code,
        failure_samples,
    )


def _temporary_output_path(destination: Path) -> Path:
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".part",
        dir=destination.parent,
    )
    os.close(descriptor)
    path = Path(name)
    path.unlink()
    return path


def _publish_output_pair(outputs: tuple[tuple[Path, Path], ...]) -> None:
    """Publish outputs together and restore prior files after an I/O failure.

    Each individual replacement is atomic. The rollback protects against a
    normal exception between replacements; no two-path operation can guarantee
    atomicity across an abrupt process or machine failure.
    """
    backups: list[tuple[Path, Path | None]] = []
    published: list[Path] = []
    try:
        for _, destination in outputs:
            if destination.exists():
                backup = _temporary_backup_path(destination)
                os.replace(destination, backup)
                backups.append((destination, backup))
            else:
                backups.append((destination, None))

        for temporary, destination in outputs:
            os.replace(temporary, destination)
            published.append(destination)
    except BaseException:
        for destination in published:
            destination.unlink(missing_ok=True)
        for destination, backup in backups:
            if backup is not None and backup.exists():
                os.replace(backup, destination)
        raise
    else:
        for _, backup in backups:
            if backup is not None:
                try:
                    backup.unlink(missing_ok=True)
                except OSError:
                    # Both final files are already valid. A cleanup failure must
                    # not report the completed import as failed.
                    pass


def _temporary_backup_path(destination: Path) -> Path:
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".backup",
        dir=destination.parent,
    )
    os.close(descriptor)
    path = Path(name)
    path.unlink()
    return path


def _accepted_json(record_number: int, order: OrderCreate) -> str:
    return json.dumps(
        {
            "record_number": record_number,
            "order": json.loads(order.model_dump_json()),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _schema_rejection_json(record_number: int, error: ValidationError) -> str:
    issues = [
        {
            "code": _schema_issue_code(tuple(detail["loc"])),
            "location": list(detail["loc"]),
            "message": "Value does not satisfy the order schema.",
        }
        for detail in error.errors(include_input=False, include_url=False)
    ]
    return _json_text(
        {
            "record_number": record_number,
            "error_code": "invalid_order",
            "issues": issues,
        }
    )


def _schema_issue_code(location: tuple[str | int, ...]) -> str:
    if location[:1] == ("customer_email",):
        return "invalid_email"
    if location[:1] == ("amount",):
        return "invalid_amount"
    if location[:1] == ("ordered_at",):
        return "invalid_datetime"
    if location[:1] == ("status",):
        return "invalid_status"
    if location[:1] == ("currency",):
        return "invalid_currency"
    if location[:1] == ("order_id",):
        return "invalid_order_id"
    if location[-1:] == ("sku",):
        return "invalid_sku"
    if location[-1:] == ("quantity",):
        return "invalid_quantity"
    if location[:1] == ("items",):
        return "invalid_items"
    return "invalid_order"


def _rejection_json(
    record_number: int,
    error_code: str,
    issues: Iterable[CleaningIssue | ValidationIssue],
) -> str:
    return _json_text(
        {
            "record_number": record_number,
            "error_code": error_code,
            "issues": [
                {
                    "code": (
                        issue.code
                        if isinstance(issue, CleaningIssue)
                        else issue.error_type
                    ),
                    "location": list(issue.location),
                    "message": issue.message,
                }
                for issue in issues
            ],
        }
    )


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _write_ndjson_line(destination: TextIO, value: object) -> None:
    destination.write(_json_text(value))
    destination.write("\n")


def _record_failure(
    rejection: dict[str, object],
    failures_by_code: dict[str, int],
    failure_samples: list[dict[str, object]],
    *,
    maximum_failure_samples: int,
) -> None:
    code = str(rejection["error_code"])
    failures_by_code[code] = failures_by_code.get(code, 0) + 1
    if len(failure_samples) < maximum_failure_samples:
        failure_samples.append(rejection)


def _sqlite_size_bytes(connection: sqlite3.Connection) -> int:
    page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
    page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
    return page_count * page_size


def iter_ijson_array(
    source: BinaryIO,
    *,
    buffer_bytes: int = 64 * 1024,
) -> Iterator[object]:
    """Yield top-level array elements with ijson without materializing the array."""
    if buffer_bytes <= 0:
        raise ValueError("buffer_bytes must be positive")

    try:
        events = ijson.parse(source, buf_size=buffer_bytes)
        first_prefix, first_event, _ = next(events)
        if first_prefix != "" or first_event != "start_array":
            raise InvalidJsonError("Top-level value must be a JSON array")
        yield from ijson.items(events, "item")
    except StopIteration as exc:
        raise InvalidJsonError("Expected a top-level JSON array") from exc
    except ijson.JSONError as exc:
        raise InvalidJsonError(
            "Invalid JSON array",
            details={"detail": str(exc)},
        ) from exc


def iter_batches(records: Iterable[T], *, batch_size: int) -> Iterator[list[T]]:
    """Collect no more than ``batch_size`` records before yielding a batch."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    batch: list[T] = []
    for record in records:
        batch.append(record)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def clean_order_batch(
    records: Sequence[tuple[int, dict[str, object]]],
) -> list[CleanedOrder]:
    """Legacy Day 4 Polars benchmark; use ``process_order_file`` for Day 5."""
    if not records:
        return []

    record_numbers = [number for number, _ in records]
    values = [value for _, value in records]

    frame = pl.DataFrame(
        {
            "record_number": pl.Series(record_numbers, dtype=pl.UInt64),
            "order_id": _string_series("order_id", values),
            "customer_email": _string_series("customer_email", values),
            "amount_text": pl.Series(
                "amount_text",
                [_amount_text(value.get("amount")) for value in values],
                dtype=pl.String,
            ),
            "currency": _string_series("currency", values),
            "ordered_at": _string_series("ordered_at", values),
            "status": _string_series("status", values),
        }
    ).with_columns(
        pl.col("order_id").str.strip_chars(),
        pl.col("customer_email").str.strip_chars().str.to_lowercase(),
        pl.when(
            pl.col("amount_text")
            .str.strip_chars()
            .cast(pl.Float64, strict=False)
            .is_not_null()
        )
        .then(pl.col("amount_text").str.strip_chars())
        .otherwise(None)
        .alias("amount"),
        pl.col("currency").str.strip_chars().str.to_uppercase(),
        pl.col("ordered_at").str.strip_chars().str.to_datetime(
            strict=False,
            time_zone="UTC",
        ),
        pl.col("status").str.strip_chars().str.to_lowercase(),
    )

    frame = frame.with_columns(
        (
            pl.col("order_id").is_not_null()
            & (pl.col("order_id").cum_count().over("order_id") > 1)
        ).alias("duplicate_order_id")
    ).drop("amount_text")

    cleaned: list[CleanedOrder] = []
    for original, scalar_values in zip(values, frame.to_dicts(), strict=True):
        merged = dict(original)
        record_number = int(scalar_values.pop("record_number"))
        duplicate = bool(scalar_values.pop("duplicate_order_id"))
        merged.update(scalar_values)
        cleaned.append(
            CleanedOrder(
                record_number=record_number,
                value=merged,
                duplicate_order_id=duplicate,
            )
        )
    return cleaned


def iter_order_outcomes(
    source: BinaryIO,
    *,
    batch_size: int,
    supported_currencies: Iterable[str],
) -> Iterator[OrderProcessingOutcome]:
    """Legacy Day 4 iterator; it does not implement the Day 5 contract."""
    supported = frozenset(supported_currencies)
    adapter = TypeAdapter(OrderCreate)
    numbered = enumerate(iter_ijson_array(source), start=1)

    for batch in iter_batches(numbered, batch_size=batch_size):
        object_records = [
            (record_number, value)
            for record_number, value in batch
            if isinstance(value, dict)
        ]
        cleaned_by_number = {
            record.record_number: record
            for record in clean_order_batch(object_records)
        }

        for record_number, value in batch:
            if not isinstance(value, dict):
                yield OrderProcessingOutcome(
                    record_number=record_number,
                    error_code="not_object",
                )
                continue

            cleaned = cleaned_by_number[record_number]
            if cleaned.duplicate_order_id:
                yield OrderProcessingOutcome(
                    record_number=record_number,
                    error_code="duplicate_order_id",
                )
                continue

            try:
                order = adapter.validate_python(cleaned.value)
            except ValidationError as exc:
                yield OrderProcessingOutcome(
                    record_number=record_number,
                    error_code="invalid_order",
                    issues=tuple(
                        ValidationIssue(
                            location=tuple(error["loc"]),
                            message=error["msg"],
                            error_type=error["type"],
                        )
                        for error in exc.errors(include_input=False, include_url=False)
                    ),
                )
                continue

            if order.currency not in supported:
                yield OrderProcessingOutcome(
                    record_number=record_number,
                    error_code="unsupported_currency",
                )
                continue

            yield OrderProcessingOutcome(
                record_number=record_number,
                order=order,
            )


def summarize_order_file(
    path: Path,
    *,
    batch_size: int,
    supported_currencies: Iterable[str],
    maximum_failure_samples: int = 20,
) -> OrderPipelineSummary:
    """Summarize the legacy Day 4 iterator; not the canonical Day 5 import."""
    if maximum_failure_samples < 0:
        raise ValueError("maximum_failure_samples must not be negative")

    total_records = 0
    accepted_records = 0
    rejected_records = 0
    failures_by_code: dict[str, int] = {}
    failure_samples: list[dict[str, object]] = []

    with path.open("rb") as source:
        outcomes = iter_order_outcomes(
            source,
            batch_size=batch_size,
            supported_currencies=supported_currencies,
        )
        for outcome in outcomes:
            total_records += 1
            if outcome.accepted:
                accepted_records += 1
                continue

            rejected_records += 1
            code = outcome.error_code or "unknown"
            failures_by_code[code] = failures_by_code.get(code, 0) + 1
            if len(failure_samples) < maximum_failure_samples:
                failure_samples.append(
                    {
                        "record_number": outcome.record_number,
                        "error_code": code,
                        "issues": [
                            {
                                "location": issue.location,
                                "message": issue.message,
                                "error_type": issue.error_type,
                            }
                            for issue in outcome.issues
                        ],
                    }
                )

    return OrderPipelineSummary(
        total_records=total_records,
        accepted_records=accepted_records,
        rejected_records=rejected_records,
        failures_by_code=failures_by_code,
        failure_samples=failure_samples,
    )


def _string_series(
    name: str,
    values: Sequence[dict[str, object]],
) -> pl.Series:
    return pl.Series(
        name,
        [
            value.get(name) if isinstance(value.get(name), str) else None
            for value in values
        ],
        dtype=pl.String,
    )


def _amount_text(value: object) -> str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (Decimal, int, float, str)):
        return str(value)
    return None
