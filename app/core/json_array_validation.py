from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from app.core.parsers import iter_file_chunks, iter_json_array


@dataclass(frozen=True)
class JsonArrayValidationSummary:
    valid_records: int
    failures_by_code: dict[str, int]
    failure_count: int
    failure_samples: list[dict[str, int | str]]


def summarize_json_array(
    source: BinaryIO,
    *,
    maximum_record_bytes: int,
    chunk_bytes: int = 64 * 1024,
    maximum_failure_samples: int = 20,
) -> JsonArrayValidationSummary:
    """Stream a top-level JSON array and retain only a bounded summary."""
    if maximum_failure_samples < 0:
        raise ValueError("maximum_failure_samples must not be negative")

    valid_records = 0
    failure_count = 0
    failures_by_code: dict[str, int] = {}
    failure_samples: list[dict[str, int | str]] = []

    chunks = iter_file_chunks(source, chunk_bytes=chunk_bytes)
    for record_number, value in enumerate(
        iter_json_array(chunks, maximum_record_bytes=maximum_record_bytes),
        start=1,
    ):
        if isinstance(value, dict):
            valid_records += 1
            continue

        code = "not_object"
        failure_count += 1
        failures_by_code[code] = failures_by_code.get(code, 0) + 1
        if len(failure_samples) < maximum_failure_samples:
            failure_samples.append(
                {"record_number": record_number, "error_code": code}
            )

    return JsonArrayValidationSummary(
        valid_records=valid_records,
        failures_by_code=failures_by_code,
        failure_count=failure_count,
        failure_samples=failure_samples,
    )


def summarize_json_array_file(
    path: Path,
    *,
    maximum_record_bytes: int,
    chunk_bytes: int = 64 * 1024,
    maximum_failure_samples: int = 20,
) -> JsonArrayValidationSummary:
    with path.open("rb") as source:
        return summarize_json_array(
            source,
            maximum_record_bytes=maximum_record_bytes,
            chunk_bytes=chunk_bytes,
            maximum_failure_samples=maximum_failure_samples,
        )
