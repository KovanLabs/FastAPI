"""Benchmark one complete JSON or CSV import workflow in a fresh process."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import platform
import resource
import sys
from pathlib import Path
from time import perf_counter

import duckdb
import polars as pl

from app.core.batch_loading import DuckDBBatchLoader
from app.core.csv_export import export_processed_csv
from app.core.order_pipeline import process_order_file
from app.core.processing_adapters import create_processing_adapter
from scripts.generate_orders_csv import write_orders_csv
from scripts.generate_orders_json import write_orders


def _timed(operation):
    started = perf_counter()
    result = operation()
    return result, perf_counter() - started


def _copy_and_checksum(source_path: Path, destination_path: Path) -> str:
    digest = hashlib.sha256()
    with (
        source_path.open("rb") as source,
        destination_path.open("xb") as destination,
    ):
        while chunk := source.read(1024 * 1024):
            destination.write(chunk)
            digest.update(chunk)
        destination.flush()
        os.fsync(destination.fileno())
    return digest.hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _generate(path: Path, input_format: str, records: int) -> None:
    with path.open("x", encoding="utf-8", newline="") as output:
        # Keep progress messages away from the JSON metrics written to stdout.
        with contextlib.redirect_stdout(sys.stderr):
            if input_format == "json":
                write_orders(output, records)
            else:
                write_orders_csv(output, records)


def benchmark_workflow(
    *,
    input_format: str,
    records: int,
    strategy: str,
    output_directory: Path,
    maximum_record_bytes: int = 1024 * 1024,
) -> dict[str, object]:
    """Run and measure one complete format-specific workflow."""

    if input_format not in {"json", "csv"}:
        raise ValueError("input_format must be 'json' or 'csv'")
    if strategy not in {"python", "polars", "duckdb"}:
        raise ValueError("unsupported processing strategy")
    if records < 0:
        raise ValueError("records must not be negative")
    if output_directory.exists() and any(output_directory.iterdir()):
        raise FileExistsError("benchmark output directory must be empty")
    output_directory.mkdir(parents=True, exist_ok=True)

    raw_path = output_directory / f"raw.{input_format}"
    uploaded_path = output_directory / f"uploaded.{input_format}"
    accepted_path = output_directory / "accepted.ndjson"
    rejected_path = output_directory / "rejected.ndjson"
    adapter_output = output_directory / ".adapter-output.ndjson.part"
    total_started = perf_counter()

    _, generation_seconds = _timed(
        lambda: _generate(raw_path, input_format, records)
    )
    uploaded_sha256, upload_seconds = _timed(
        lambda: _copy_and_checksum(raw_path, uploaded_path)
    )
    summary, cleaning_seconds = _timed(
        lambda: process_order_file(
            uploaded_path,
            accepted_path=accepted_path,
            rejected_path=rejected_path,
            supported_currencies={"EUR", "INR", "USD"},
            maximum_record_bytes=maximum_record_bytes,
        )
    )
    adapter = create_processing_adapter(
        strategy,
        maximum_record_bytes=maximum_record_bytes,
    )
    adapter_result, adapter_seconds = _timed(
        lambda: adapter.process(accepted_path, adapter_output)
    )
    if summary.total_records != records:
        raise RuntimeError("generated and cleaned record counts do not match")
    if adapter_result.records_written != summary.accepted_records:
        raise RuntimeError("cleaning and adapter record counts do not match")
    adapter_output_bytes = adapter_output.stat().st_size

    if input_format == "json":
        processed_path = output_directory / "processed.ndjson"
        os.replace(adapter_output, processed_path)
        database_path = output_directory / "imports.duckdb"
        loader = DuckDBBatchLoader(
            database_path,
            maximum_record_bytes=maximum_record_bytes,
        )
        load_result, finalization_seconds = _timed(
            lambda: loader.load(processed_path, import_id="benchmark-import")
        )
        final_artifact = database_path
        finalized_records = load_result.records_inserted
        workflow = "raw_json_to_duckdb"
    else:
        processed_csv_path = output_directory / "processed.csv"
        export_result, finalization_seconds = _timed(
            lambda: export_processed_csv(
                adapter_output,
                processed_csv_path,
                maximum_record_bytes=maximum_record_bytes,
            )
        )
        adapter_output.unlink(missing_ok=True)
        final_artifact = processed_csv_path
        finalized_records = export_result.records_written
        workflow = "raw_csv_to_processed_csv"

    if finalized_records != summary.accepted_records:
        raise RuntimeError("adapter and final artifact record counts do not match")

    total_seconds = perf_counter() - total_started
    stage_seconds = {
        "generation": round(generation_seconds, 6),
        "upload_and_checksum": round(upload_seconds, 6),
        "clean_and_validate": round(cleaning_seconds, 6),
        "adapter": round(adapter_seconds, 6),
        "finalization": round(finalization_seconds, 6),
    }
    return {
        "workflow": workflow,
        "input_format": input_format,
        "engine": adapter_result.engine,
        "records_requested": records,
        "total_records": summary.total_records,
        "accepted_records": summary.accepted_records,
        "rejected_records": summary.rejected_records,
        "finalized_records": finalized_records,
        "raw_bytes": raw_path.stat().st_size,
        "accepted_bytes": accepted_path.stat().st_size,
        "rejected_bytes": rejected_path.stat().st_size,
        "adapter_output_bytes": adapter_output_bytes,
        "uploaded_sha256": uploaded_sha256,
        "final_artifact": str(final_artifact),
        "final_bytes": final_artifact.stat().st_size,
        "final_sha256": _sha256(final_artifact),
        "stage_seconds": stage_seconds,
        "total_seconds": round(total_seconds, 6),
        "records_per_second": round(
            summary.total_records / total_seconds if total_seconds else 0.0,
            2,
        ),
        "peak_rss_mib": round(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
            2,
        ),
        "python_version": platform.python_version(),
        "polars_version": pl.__version__,
        "duckdb_version": duckdb.__version__,
        "note": (
            "This compares two different end-to-end application workflows. "
            "The upload stage is a bounded local staging copy with SHA-256; "
            "HTTP transport and multipart decoding are excluded. It is not a "
            "parser-only benchmark."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=("json", "csv"), required=True)
    parser.add_argument("--records", type=int, required=True)
    parser.add_argument(
        "--strategy",
        choices=("python", "polars", "duckdb"),
        default="duckdb",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--maximum-record-bytes", type=int, default=1024 * 1024)
    args = parser.parse_args()

    measurement = benchmark_workflow(
        input_format=args.format,
        records=args.records,
        strategy=args.strategy,
        output_directory=args.output_dir,
        maximum_record_bytes=args.maximum_record_bytes,
    )
    print(json.dumps(measurement, indent=2))


if __name__ == "__main__":
    main()
