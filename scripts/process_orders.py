"""Run the bounded ijson, Polars, and Pydantic order pipeline."""

from __future__ import annotations

import argparse
import json
import resource
from dataclasses import asdict
from pathlib import Path
from time import perf_counter

from app.core.config import get_settings
from app.core.order_pipeline import summarize_order_file


def main() -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.import_batch_size,
    )
    parser.add_argument("--maximum-failure-samples", type=int, default=20)
    args = parser.parse_args()

    started = perf_counter()
    summary = summarize_order_file(
        args.path,
        batch_size=args.batch_size,
        supported_currencies=settings.supported_currencies,
        maximum_failure_samples=args.maximum_failure_samples,
    )
    result = asdict(summary)
    result["batch_size"] = args.batch_size
    result["elapsed_seconds"] = round(perf_counter() - started, 3)
    result["peak_rss_mib"] = round(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        2,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
