"""Atomic deterministic CSV publication for processed canonical orders."""

from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path

from app.core.processing_adapters import AdapterExecutionError


PROCESSED_CSV_COLUMNS = (
    "record_number",
    "order_id",
    "customer_email",
    "amount",
    "currency",
    "ordered_at",
    "status",
    "items",
    "item_count",
    "total_quantity",
)


@dataclass(frozen=True)
class CsvExportResult:
    records_written: int
    output_path: Path


def export_processed_csv(
    input_path: Path,
    output_path: Path,
    *,
    maximum_record_bytes: int = 1024 * 1024,
) -> CsvExportResult:
    """Publish processed NDJSON as one flat CSV file without full retention."""

    if maximum_record_bytes <= 0:
        raise ValueError("maximum_record_bytes must be positive")
    if output_path.exists():
        raise FileExistsError(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = output_path.parent / f".{output_path.name}.part"
    records_written = 0
    try:
        with (
            input_path.open("rb") as source,
            temporary_path.open("x", encoding="utf-8", newline="") as destination,
        ):
            writer = csv.DictWriter(
                destination,
                fieldnames=PROCESSED_CSV_COLUMNS,
                lineterminator="\n",
            )
            writer.writeheader()
            while raw_line := source.readline(maximum_record_bytes + 3):
                newline_bytes = 2 if raw_line.endswith(b"\r\n") else int(
                    raw_line.endswith(b"\n")
                )
                if len(raw_line) - newline_bytes > maximum_record_bytes:
                    raise AdapterExecutionError(
                        "processed record exceeds the configured record byte limit"
                    )
                record = _processed_record(json.loads(raw_line))
                writer.writerow(record)
                records_written += 1
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary_path, output_path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise
    return CsvExportResult(
        records_written=records_written,
        output_path=output_path,
    )


def _processed_record(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise AdapterExecutionError("processed record must be an object")
    order = value.get("order")
    if not isinstance(order, dict):
        raise AdapterExecutionError("processed order must be an object")
    items = order.get("items")
    if not isinstance(items, list):
        raise AdapterExecutionError("processed order items must be an array")
    quantities: list[int] = []
    for item in items:
        if not isinstance(item, dict) or type(item.get("quantity")) is not int:
            raise AdapterExecutionError("processed item quantity must be an integer")
        quantities.append(item["quantity"])
    item_count = value.get("item_count")
    total_quantity = value.get("total_quantity")
    if item_count != len(items) or total_quantity != sum(quantities):
        raise AdapterExecutionError("processed order metrics are inconsistent")
    required_strings = (
        "order_id",
        "customer_email",
        "amount",
        "currency",
        "ordered_at",
        "status",
    )
    if type(value.get("record_number")) is not int or any(
        not isinstance(order.get(field), str) for field in required_strings
    ):
        raise AdapterExecutionError("processed order fields have invalid types")
    return {
        "record_number": value["record_number"],
        **{field: order[field] for field in required_strings},
        "items": json.dumps(
            items,
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "item_count": item_count,
        "total_quantity": total_quantity,
    }
