"""Run repeated fresh-process JSON and CSV workflows and summarize them."""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


def summarize_measurements(
    measurements: list[dict[str, Any]],
) -> dict[str, object]:
    """Aggregate repeated measurements without treating workflows as identical."""

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for measurement in measurements:
        grouped[str(measurement["workflow"])].append(measurement)
    if len(grouped) != 2:
        raise ValueError("comparison requires measurements for exactly two workflows")

    workflows: dict[str, object] = {}
    for workflow, runs in sorted(grouped.items()):
        total_seconds = [float(run["total_seconds"]) for run in runs]
        throughputs = [float(run["records_per_second"]) for run in runs]
        peak_memory = [float(run["peak_rss_mib"]) for run in runs]
        stage_names = tuple(runs[0]["stage_seconds"])
        workflows[workflow] = {
            "runs": len(runs),
            "mean_total_seconds": round(statistics.mean(total_seconds), 6),
            "median_total_seconds": round(statistics.median(total_seconds), 6),
            "min_total_seconds": min(total_seconds),
            "max_total_seconds": max(total_seconds),
            "mean_records_per_second": round(statistics.mean(throughputs), 2),
            "mean_peak_rss_mib": round(statistics.mean(peak_memory), 2),
            "max_peak_rss_mib": max(peak_memory),
            "mean_stage_seconds": {
                stage: round(
                    statistics.mean(
                        float(run["stage_seconds"][stage]) for run in runs
                    ),
                    6,
                )
                for stage in stage_names
            },
            "raw_bytes": runs[0]["raw_bytes"],
            "final_bytes": runs[0]["final_bytes"],
        }

    means = {
        workflow: float(summary["mean_total_seconds"])
        for workflow, summary in workflows.items()
    }
    faster_workflow = min(means, key=means.get)
    slower_workflow = max(means, key=means.get)
    slower_seconds = means[slower_workflow]
    advantage = (
        (slower_seconds - means[faster_workflow]) / slower_seconds * 100
        if slower_seconds
        else 0.0
    )
    return {
        "comparison_type": "different_end_to_end_workflows",
        "faster_workflow": faster_workflow,
        "mean_total_time_advantage_percent": round(advantage, 2),
        "workflows": workflows,
        "note": (
            "JSON ends in DuckDB and CSV ends in a CSV file. HTTP transport "
            "and multipart decoding are excluded, and this result does not "
            "isolate parser performance."
        ),
    }


def _run_once(
    *,
    input_format: str,
    records: int,
    strategy: str,
    run_directory: Path,
) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.benchmark_end_to_end",
            "--format",
            input_format,
            "--records",
            str(records),
            "--strategy",
            strategy,
            "--output-dir",
            str(run_directory),
        ],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return json.loads(completed.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, required=True)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument(
        "--strategy",
        choices=("python", "polars", "duckdb"),
        default="duckdb",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.records < 0:
        parser.error("--records must not be negative")
    if args.runs < 3:
        parser.error("--runs must be at least 3")
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        parser.error("--output-dir must be empty")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    measurements: list[dict[str, Any]] = []
    for run_number in range(1, args.runs + 1):
        for input_format in ("json", "csv"):
            measurement = _run_once(
                input_format=input_format,
                records=args.records,
                strategy=args.strategy,
                run_directory=(
                    args.output_dir / f"{input_format}-run-{run_number}"
                ),
            )
            measurements.append(measurement)
            metrics_path = (
                args.output_dir / f"{input_format}-run-{run_number}.json"
            )
            metrics_path.write_text(
                json.dumps(measurement, indent=2) + "\n",
                encoding="utf-8",
            )

    print(json.dumps(summarize_measurements(measurements), indent=2))


if __name__ == "__main__":
    main()
