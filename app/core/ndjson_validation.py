from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from app.core.parsers import iter_ndjson


@dataclass(frozen=True)
class NdjsonValidationSummary:
    valid_records: int
    skipped_blank: int
    failures_by_code: dict[str, int]
    failure_count: int
    failure_samples: list[dict[str, int | str]]


def summarize_ndjson(
    source: BinaryIO,
    *,
    maximum_record_bytes: int,
    maximum_failure_samples: int = 20,
) -> NdjsonValidationSummary:
    if maximum_failure_samples < 0:
        raise ValueError("maximum_failure_samples must not be negative")

    valid_records = 0
    skipped_blank = 0
    failures_by_code: dict[str, int] = {}
    failure_count = 0
    failure_samples: list[dict[str, int | str]] = []

    for outcome in iter_ndjson(source, maximum_record_bytes=maximum_record_bytes):
        if outcome.record is not None:
            valid_records += 1
        elif outcome.skipped_blank:
            skipped_blank += 1
        elif outcome.error_code is not None:
            code = outcome.error_code
            failures_by_code[code] = failures_by_code.get(code, 0) + 1
            failure_count += 1
            if len(failure_samples) < maximum_failure_samples:
                failure_samples.append(
                    {"line_number": outcome.line_number, "error_code": code}
                )

    return NdjsonValidationSummary(
        valid_records=valid_records,
        skipped_blank=skipped_blank,
        failures_by_code=failures_by_code,
        failure_count=failure_count,
        failure_samples=failure_samples,
    )


def summarize_ndjson_file(
    path: Path,
    *,
    maximum_record_bytes: int,
    maximum_failure_samples: int = 20,
) -> NdjsonValidationSummary:
    with path.open("rb") as source:
        return summarize_ndjson(
            source,
            maximum_record_bytes=maximum_record_bytes,
            maximum_failure_samples=maximum_failure_samples,
        )
