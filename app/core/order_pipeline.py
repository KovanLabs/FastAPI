"""Bounded cleaning and validation for orders in a top-level JSON array."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import BinaryIO, TypeVar

import ijson
import polars as pl
from pydantic import TypeAdapter, ValidationError

from app.core.parsers import InvalidJsonError
from app.schemas.orders import OrderCreate


T = TypeVar("T")


@dataclass(frozen=True)
class ValidationIssue:
    location: str
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
    """Normalize scalar order fields with Polars inside one bounded batch."""
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
    """Parse, clean, and validate orders while retaining only one batch."""
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
                            location=".".join(str(part) for part in error["loc"]),
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
    """Process one file and retain only counters plus bounded failure samples."""
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
