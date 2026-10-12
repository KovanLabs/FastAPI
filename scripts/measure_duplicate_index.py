"""Measure the Python heap used by an in-memory duplicate order-ID index."""

from __future__ import annotations

import argparse
import gc
import json
import tracemalloc
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class DuplicateIndexMeasurement:
    unique_ids: int
    current_bytes: int
    peak_bytes: int
    peak_bytes_per_id: float
    projected_peak_bytes_at_10m: int


def measure_duplicate_index(unique_ids: int) -> DuplicateIndexMeasurement:
    if unique_ids <= 0:
        raise ValueError("unique_ids must be positive")

    gc.collect()
    tracemalloc.start()
    index = {f"ORD-{number:012d}" for number in range(unique_ids)}
    current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    if len(index) != unique_ids:
        raise RuntimeError("duplicate-index fixture was not unique")

    peak_bytes_per_id = peak_bytes / unique_ids
    return DuplicateIndexMeasurement(
        unique_ids=unique_ids,
        current_bytes=current_bytes,
        peak_bytes=peak_bytes,
        peak_bytes_per_id=peak_bytes_per_id,
        projected_peak_bytes_at_10m=round(peak_bytes_per_id * 10_000_000),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unique-ids", type=int, default=100_000)
    args = parser.parse_args()
    print(json.dumps(asdict(measure_duplicate_index(args.unique_ids)), indent=2))


if __name__ == "__main__":
    main()
