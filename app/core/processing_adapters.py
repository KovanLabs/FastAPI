from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol

import duckdb
import polars as pl


CANONICAL_ACCEPTED_SCHEMA = pl.Schema(
    {
        "record_number": pl.Int64,
        "order": pl.Struct(
            {
                "order_id": pl.String,
                "customer_email": pl.String,
                "amount": pl.String,
                "currency": pl.String,
                "ordered_at": pl.String,
                "status": pl.String,
                "items": pl.List(
                    pl.Struct(
                        {
                            "sku": pl.String,
                            "quantity": pl.Int64,
                        }
                    )
                ),
            }
        ),
    }
)

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
DUCKDB_ORDER_TYPE = (
    "STRUCT(order_id VARCHAR, customer_email VARCHAR, amount VARCHAR, "
    "currency VARCHAR, ordered_at VARCHAR, status VARCHAR, "
    "items STRUCT(sku VARCHAR, quantity BIGINT)[])"
)


@dataclass(frozen=True)
class AdapterResult:
    engine: str
    records_written: int
    output_path: Path


class AdapterExecutionError(RuntimeError):
    """Raised when an engine cannot process canonical adapter input."""


class OrderProcessingAdapter(Protocol):
    """Common seam implemented by each Day 6 processing engine."""

    engine: str
    maximum_record_bytes: int

    def process(
        self,
        input_path: Path,
        temporary_output_path: Path,
    ) -> AdapterResult: ...


class StandardPythonAdapter:
    engine = "python"

    def __init__(self, *, maximum_record_bytes: int = 1024 * 1024) -> None:
        if maximum_record_bytes <= 0:
            raise ValueError("maximum_record_bytes must be positive")
        self.maximum_record_bytes = maximum_record_bytes

    def process(
        self,
        input_path: Path,
        temporary_output_path: Path,
    ) -> AdapterResult:
        records_written = 0
        output_created = False
        try:
            records = _iter_canonical_records(
                input_path,
                maximum_record_bytes=self.maximum_record_bytes,
            )

            destination = temporary_output_path.open(
                "x", encoding="utf-8", newline="\n"
            )
            output_created = True
            with destination:
                for value in records:
                    processed= _add_order_metrics(value)
                    destination.write(
                        json.dumps (processed, ensure_ascii=False, separators=(",", ":"))
                    )
                    destination.write("\n")
                    records_written += 1
        except Exception as exc:
            if output_created:
                temporary_output_path.unlink(missing_ok=True)
            if isinstance(exc, AdapterExecutionError):
                raise
            raise AdapterExecutionError(
                f"{self.engine} adapter could not process canonical input"
            ) from exc
        except BaseException:
            if output_created:
                temporary_output_path.unlink(missing_ok=True)
            raise

        return AdapterResult(
            engine=self.engine,
            records_written=records_written,
            output_path=temporary_output_path,
        )


class PolarsAdapter:
    engine = "polars"

    def __init__(self, *, maximum_record_bytes: int = 1024 * 1024) -> None:
        if maximum_record_bytes <= 0:
            raise ValueError("maximum_record_bytes must be positive")
        self.maximum_record_bytes = maximum_record_bytes

    def process(
        self,
        input_path: Path,
        temporary_output_path: Path,
    ) -> AdapterResult:
        output_created = False
        try:
            temporary_output_path.open("x", encoding="utf-8").close()
            output_created = True

            source_before = _source_signature(input_path)
            records_written = _validate_canonical_ndjson(
                input_path,
                maximum_record_bytes=self.maximum_record_bytes,
            )
            if records_written == 0:
                if _source_signature(input_path) != source_before:
                    raise AdapterExecutionError(
                        "canonical adapter input changed during processing"
                    )
                return AdapterResult(
                    engine=self.engine,
                    records_written=0,
                    output_path=temporary_output_path,
                )

            plan = pl.scan_ndjson(
                input_path,
                schema=CANONICAL_ACCEPTED_SCHEMA,
                ignore_errors=False,
            )
            plan.sink_ndjson(
                temporary_output_path,
                maintain_order=True,
                engine="streaming",
            )
            if _source_signature(input_path) != source_before:
                raise AdapterExecutionError(
                    "canonical adapter input changed during processing"
                )
        except Exception as exc:
            if output_created:
                temporary_output_path.unlink(missing_ok=True)
            if isinstance(exc, AdapterExecutionError):
                raise
            raise AdapterExecutionError(
                f"{self.engine} adapter could not process canonical input"
            ) from exc
        except BaseException:
            if output_created:
                temporary_output_path.unlink(missing_ok=True)
            raise

        return AdapterResult(
            engine=self.engine,
            records_written=records_written,
            output_path=temporary_output_path,
        )


class DuckDBAdapter:
    """Transform canonical accepted NDJSON through DuckDB SQL."""

    engine = "duckdb"

    def __init__(
        self,
        *,
        memory_limit: str = "512MB",
        threads: int = 2,
        collect_profile: bool = False,
        maximum_record_bytes: int = 1024 * 1024,
    ) -> None:
        if maximum_record_bytes <= 0:
            raise ValueError("maximum_record_bytes must be positive")
        self.memory_limit = memory_limit
        self.threads = threads
        self.collect_profile = collect_profile
        self.maximum_record_bytes = maximum_record_bytes
        self.last_profile: dict[str, int] | None = None

    def process(
        self,
        input_path: Path,
        temporary_output_path: Path,
    ) -> AdapterResult:
        output_created = False
        self.last_profile = None
        try:
            temporary_output_path.open("x", encoding="utf-8").close()
            output_created = True

            source_before = _source_signature(input_path)
            records_written = _validate_canonical_ndjson(
                input_path,
                maximum_record_bytes=self.maximum_record_bytes,
            )
            if records_written == 0:
                if _source_signature(input_path) != source_before:
                    raise AdapterExecutionError(
                        "canonical adapter input changed during processing"
                    )
                return AdapterResult(
                    engine=self.engine,
                    records_written=0,
                    output_path=temporary_output_path,
                )

            with TemporaryDirectory(
                prefix=".duckdb-adapter-",
                dir=temporary_output_path.parent,
            ) as temporary_directory:
                with duckdb.connect(":memory:") as connection:
                    connection.execute(
                        "SET memory_limit = ?",
                        [self.memory_limit],
                    )
                    connection.execute("SET threads = ?", [self.threads])
                    connection.execute("SET preserve_insertion_order = true")
                    connection.execute(
                        "SET temp_directory = ?",
                        [temporary_directory],
                    )
                    profile_path = Path(temporary_directory) / "profile.json"
                    if self.collect_profile:
                        connection.execute("PRAGMA enable_profiling = 'json'")
                        connection.execute(
                            "PRAGMA profiling_output = ?",
                            [str(profile_path)],
                        )
                    relation = connection.read_json(
                        str(input_path),
                        format="newline_delimited",
                        columns={
                            "record_number": "BIGINT",
                            "order": DUCKDB_ORDER_TYPE,
                        },
                        ignore_errors=False,
                    )
                    relation.create_view("canonical_input")
                    connection.execute(
                        """
                        COPY (
                            SELECT
                                record_number,
                                struct_pack(
                                    order_id := "order".order_id,
                                    customer_email := "order".customer_email,
                                    amount := "order".amount,
                                    currency := "order".currency,
                                    ordered_at := "order".ordered_at,
                                    status := "order".status,
                                    items := "order".items
                                ) AS "order"
                            FROM canonical_input
                        ) TO ? (FORMAT JSON)
                        """,
                        [str(temporary_output_path)],
                    )
                    if self.collect_profile:
                        profile = json.loads(profile_path.read_text(encoding="utf-8"))
                        self.last_profile = {
                            "system_peak_buffer_memory": int(
                                profile["system_peak_buffer_memory"]
                            ),
                            "system_peak_temp_dir_size": int(
                                profile["system_peak_temp_dir_size"]
                            ),
                        }

            if _source_signature(input_path) != source_before:
                raise AdapterExecutionError(
                    "canonical adapter input changed during processing"
                )
        except Exception as exc:
            if output_created:
                temporary_output_path.unlink(missing_ok=True)
            if isinstance(exc, AdapterExecutionError):
                raise
            raise AdapterExecutionError(
                f"{self.engine} adapter could not process canonical input"
            ) from exc
        except BaseException:
            if output_created:
                temporary_output_path.unlink(missing_ok=True)
            raise

        return AdapterResult(
            engine=self.engine,
            records_written=records_written,
            output_path=temporary_output_path,
        )


def create_processing_adapter(
    engine: str,
    *,
    maximum_record_bytes: int = 1024 * 1024,
) -> OrderProcessingAdapter:
    """Build the configured adapter behind the engine-neutral seam."""

    adapter_factories: dict[str, Callable[[], OrderProcessingAdapter]] = {
        "python": lambda: StandardPythonAdapter(
            maximum_record_bytes=maximum_record_bytes
        ),
        "polars": lambda: PolarsAdapter(
            maximum_record_bytes=maximum_record_bytes
        ),
        "duckdb": lambda: DuckDBAdapter(
            maximum_record_bytes=maximum_record_bytes
        ),
    }
    try:
        adapter_factory = adapter_factories[engine]
    except KeyError as exc:
        raise ValueError(f"unsupported processing engine: {engine}") from exc
    return adapter_factory()


def _iter_canonical_records(
    path: Path,
    *,
    maximum_record_bytes: int,
) -> Iterator[dict[str, object]]:
    previous_record_number: int | None = None

    with path.open("rb") as source:
        while raw_line := source.readline(maximum_record_bytes + 3):
            newline_bytes = 0
            if raw_line.endswith(b"\n"):
                newline_bytes = 1
                if raw_line.endswith(b"\r\n"):
                    newline_bytes = 2
            if len(raw_line) - newline_bytes > maximum_record_bytes:
                raise AdapterExecutionError(
                    "canonical record exceeds the configured record byte limit"
                )
            record = _canonical_envelope(json.loads(raw_line))
            record_number = _require_integer(record, "record_number")

            if (
                previous_record_number is not None
                and record_number <= previous_record_number
            ):
                raise AdapterExecutionError(
                    "record_number must be strictly increasing"
                )

            previous_record_number = record_number
            yield record


def _validate_canonical_ndjson(
    path: Path,
    *,
    maximum_record_bytes: int,
) -> int:
    return sum(
        1
        for _ in _iter_canonical_records(
            path,
            maximum_record_bytes=maximum_record_bytes,
        )
    )


def _source_signature(path: Path) -> tuple[int, int, int]:
    metadata = path.stat()
    return metadata.st_ino, metadata.st_size, metadata.st_mtime_ns


def _canonical_envelope(value: object) -> dict[str, object]:
    envelope = _require_object(value, "record")
    record_number = _require_integer(envelope, "record_number")
    order = _require_object(envelope.get("order"), "order")
    items_value = order.get("items")
    if not items_value:
        raise AdapterExecutionError("order.items must be a non-empty")
    if not isinstance(items_value, list):
        raise AdapterExecutionError("order.items must be a JSON array")

    items: list[dict[str, object]] = []
    for index, item_value in enumerate(items_value):
        item = _require_object(item_value, f"order.items[{index}]")
        quantity = _require_integer(item, "quantity")
        if quantity <= 0:
            raise AdapterExecutionError(
                f"order.items[{index}].quantity must be positive"
            )

        items.append(
            {
                "sku": _require_string(item, "sku"),
                "quantity": quantity,
            }
        )


    return {
        "record_number": record_number,
        "order": {
            "order_id": _require_string(order, "order_id"),
            "customer_email": _require_string(order, "customer_email"),
            "amount": _require_string(order, "amount"),
            "currency": _require_string(order, "currency"),
            "ordered_at": _require_string(order, "ordered_at"),
            "status": _require_string(order, "status"),
            "items": items,
        },
    }


def _add_order_metrics(record: dict[str, object]) -> dict[str, object]:
    order = _require_object(record.get("order"), "order")
    items_value = order.get("items")

    if not isinstance(items_value, list):
        raise AdapterExecutionError("order.items must be a JSON array")

    total_quantity = 0
    for index, item_value in enumerate(items_value):
        item = _require_object(item_value, f"order.items[{index}]")
        total_quantity += _require_integer(item, "quantity")

    return {
        **record,
        "item_count": len(items_value),
        "total_quantity": total_quantity,
    }


def _require_object(value: object, location: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise AdapterExecutionError(f"{location} must be a JSON object")
    return value


def _require_string(value: dict[str, object], field: str) -> str:
    candidate = value.get(field)
    if not isinstance(candidate, str):
        raise AdapterExecutionError(f"{field} must be a JSON string")
    return candidate


def _require_integer(value: dict[str, object], field: str) -> int:
    candidate = value.get(field)
    if type(candidate) is not int:
        raise AdapterExecutionError(f"{field} must be a JSON integer")
    if not INT64_MIN <= candidate <= INT64_MAX:
        raise AdapterExecutionError(f"{field} must fit in a signed 64-bit integer")
    return candidate
