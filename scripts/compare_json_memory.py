"""Measure elapsed time and peak RSS for three JSON-array reading strategies."""

from __future__ import annotations

import argparse
import json
import resource
from collections.abc import Iterable
from pathlib import Path
from time import perf_counter

import ijson

from app.core.parsers import iter_file_chunks, iter_json_array


def custom_records(path: Path, maximum_record_bytes: int) -> Iterable[object]:
    with path.open("rb") as source:
        yield from iter_json_array(
            iter_file_chunks(source),
            maximum_record_bytes=maximum_record_bytes,
        )


def ijson_records(path: Path) -> Iterable[object]:
    with path.open("rb") as source:
        yield from ijson.items(source, "item", use_float=True, buf_size=64 * 1024)


def consume(records: Iterable[object], *, materialize: bool) -> int:
    if materialize:
        return len(list(records))
    return sum(1 for _ in records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument(
        "--strategy",
        choices=("json-load", "custom", "ijson"),
        required=True,
    )
    parser.add_argument("--materialize", action="store_true")
    parser.add_argument("--maximum-record-bytes", type=int, default=1024 * 1024)
    args = parser.parse_args()

    started = perf_counter()
    if args.strategy == "json-load":
        with args.path.open("rb") as source:
            records = json.load(source)
        if not isinstance(records, list):
            raise TypeError("Expected a top-level JSON array")
        record_count = len(records)
    elif args.strategy == "custom":
        record_count = consume(
            custom_records(args.path, args.maximum_record_bytes),
            materialize=args.materialize,
        )
    else:
        record_count = consume(
            ijson_records(args.path),
            materialize=args.materialize,
        )

    elapsed_seconds = perf_counter() - started
    peak_rss_mib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    print(
        json.dumps(
            {
                "strategy": args.strategy,
                "materialize": args.materialize,
                "records": record_count,
                "elapsed_seconds": round(elapsed_seconds, 3),
                "peak_rss_mib": round(peak_rss_mib, 2),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
