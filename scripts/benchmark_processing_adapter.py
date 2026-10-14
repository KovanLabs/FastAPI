"""Measure one processing adapter in a fresh process and record plan evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import resource
from dataclasses import asdict
from pathlib import Path
from time import perf_counter

import duckdb
import polars as pl

from app.core.processing_adapters import (
    DuckDBAdapter,
    PolarsAdapter,
    StandardPythonAdapter,
    build_polars_order_plan,
)


def _physical_plan(input_path: Path, output_path: Path) -> str:
    plan = build_polars_order_plan(input_path)
    sink = plan.sink_ndjson(
        output_path,
        maintain_order=True,
        lazy=True,
        engine="streaming",
    )
    return str(
        sink.show_graph(
            show=False,
            raw_output=True,
            engine="streaming",
            plan_stage="physical",
        )
    )


def _plan_nodes(plan: str, fillcolor: str) -> list[str]:
    return [
        line.strip()
        for line in plan.splitlines()
        if "[label=" in line
        and "Legend" not in line
        and f'fillcolor="{fillcolor}"' in line
    ]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--strategy",
        choices=("python", "polars", "duckdb"),
        required=True,
    )
    args = parser.parse_args()

    adapter = {
        "python": StandardPythonAdapter(),
        "polars": PolarsAdapter(),
        "duckdb": DuckDBAdapter(collect_profile=True),
    }[args.strategy]
    started = perf_counter()
    result = adapter.process(args.input, args.output)
    elapsed_seconds = perf_counter() - started
    peak_rss_mib = round(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        2,
    )

    measurement: dict[str, object] = {
        **asdict(result),
        "output_path": str(result.output_path),
        "output_bytes": result.output_path.stat().st_size,
        "output_sha256": _sha256(result.output_path),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "peak_rss_mib": peak_rss_mib,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "polars_version": pl.__version__,
        "duckdb_version": duckdb.__version__,
    }
    if isinstance(adapter, DuckDBAdapter):
        measurement.update(
            {
                "duckdb_memory_limit": adapter.memory_limit,
                "duckdb_threads": adapter.threads,
                "duckdb_temp_directory_policy": "output-adjacent temporary directory",
                "duckdb_profile": adapter.last_profile,
            }
        )
    # Inspect only after recording process RSS so graph construction cannot
    # contaminate the adapter's peak-memory measurement.
    physical_plan = (
        _physical_plan(args.input, args.output)
        if args.strategy == "polars"
        else None
    )
    if physical_plan is not None:
        measurement.update(
            {
                "physical_plan": physical_plan,
                "potentially_memory_intensive_nodes": _plan_nodes(
                    physical_plan,
                    "0.16 0.3 1.0",
                ),
                "in_memory_fallback_nodes": _plan_nodes(
                    physical_plan,
                    "0.0 0.3 1.0",
                ),
            }
        )
    print(json.dumps(measurement, indent=2))


if __name__ == "__main__":
    main()
