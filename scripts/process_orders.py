"""Run the deterministic two-pass Day 5 order import pipeline."""

from __future__ import annotations

import argparse
import json
import resource
from dataclasses import asdict
from pathlib import Path
from time import perf_counter

from app.core.config import get_settings
from app.core.order_pipeline import process_order_file


def main() -> None:
    settings = get_settings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--accepted", type=Path, required=True)
    parser.add_argument("--rejected", type=Path, required=True)
    parser.add_argument("--commit-interval", type=int, default=5_000)
    parser.add_argument("--maximum-failure-samples", type=int, default=20)
    args = parser.parse_args()

    started = perf_counter()
    summary = process_order_file(
        args.path,
        accepted_path=args.accepted,
        rejected_path=args.rejected,
        supported_currencies=settings.supported_currencies,
        maximum_failure_samples=args.maximum_failure_samples,
        commit_interval=args.commit_interval,
    )
    result = asdict(summary)
    result["accepted_path"] = str(args.accepted)
    result["rejected_path"] = str(args.rejected)
    result["commit_interval"] = args.commit_interval
    result["elapsed_seconds"] = round(perf_counter() - started, 3)
    result["peak_rss_mib"] = round(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        2,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
