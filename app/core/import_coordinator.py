"""Coordinate one deferred JSON-array import from staging through loading."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable
from dataclasses import asdict
from pathlib import Path
from typing import Protocol

from app.core.import_jobs import (
    ImportJob,
    ImportJobRepository,
    ImportState,
)
from app.core.order_pipeline import process_order_file
from app.core.processing_adapters import OrderProcessingAdapter


logger = logging.getLogger(__name__)


class BatchLoader(Protocol):
    def load(self, input_path: Path, *, import_id: str) -> object: ...

    def discard_import(self, import_id: str) -> None: ...


class ImportCoordinator:
    """Own the legal state and file transitions for one import job."""

    def __init__(
        self,
        *,
        repository: ImportJobRepository,
        adapter: OrderProcessingAdapter,
        batch_loader: BatchLoader,
        output_directory: Path,
        supported_currencies: Iterable[str],
    ) -> None:
        self.repository = repository
        self.adapter = adapter
        self.batch_loader = batch_loader
        self.output_directory = output_directory
        self.supported_currencies = frozenset(supported_currencies)

    def run(self, import_id: str, input_path: Path) -> ImportJob:
        self.repository.transition(import_id, ImportState.processing)
        job_directory = self.output_directory / import_id
        accepted_path = job_directory / "accepted.ndjson"
        rejected_path = job_directory / "rejected.ndjson"
        processed_path = job_directory / "processed.ndjson"
        processed_temporary = job_directory / ".processed.ndjson.part"
        summary_path = job_directory / "summary.json"
        summary_temporary = job_directory / ".summary.json.part"

        try:
            job_directory.mkdir(parents=True, exist_ok=True)
            summary = process_order_file(
                input_path,
                accepted_path=accepted_path,
                rejected_path=rejected_path,
                supported_currencies=self.supported_currencies,
            )
            self.adapter.process(accepted_path, processed_temporary)
            os.replace(processed_temporary, processed_path)
            self.batch_loader.load(processed_path, import_id=import_id)

            with summary_temporary.open(
                "x",
                encoding="utf-8",
                newline="\n",
            ) as destination:
                json.dump(
                    asdict(summary),
                    destination,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                destination.write("\n")
            os.replace(summary_temporary, summary_path)

            terminal_state = (
                ImportState.completed_with_errors
                if summary.rejected_records
                else ImportState.completed
            )
            return self.repository.transition(
                import_id,
                terminal_state,
                total_records=summary.total_records,
                accepted_records=summary.accepted_records,
                rejected_records=summary.rejected_records,
            )
        except Exception:
            logger.exception("Import processing failed", extra={"import_id": import_id})
            self._discard_loaded_import(import_id)
            for path in (
                accepted_path,
                rejected_path,
                processed_temporary,
                processed_path,
                summary_temporary,
                summary_path,
            ):
                path.unlink(missing_ok=True)
            return self.repository.transition(
                import_id,
                ImportState.failed,
                failure_code="processing_failed",
                failure_message="Import processing failed.",
            )

    def run_canonical_ndjson(self, import_id: str, input_path: Path) -> ImportJob:
        """Validate and load already-clean canonical accepted NDJSON."""

        self.repository.transition(import_id, ImportState.processing)
        job_directory = self.output_directory / import_id
        processed_path = job_directory / "processed.ndjson"
        processed_temporary = job_directory / ".processed.ndjson.part"
        rejected_path = job_directory / "rejected.ndjson"
        rejected_temporary = job_directory / ".rejected.ndjson.part"
        summary_path = job_directory / "summary.json"
        summary_temporary = job_directory / ".summary.json.part"

        try:
            job_directory.mkdir(parents=True, exist_ok=True)
            result = self.adapter.process(input_path, processed_temporary)
            os.replace(processed_temporary, processed_path)
            self.batch_loader.load(processed_path, import_id=import_id)

            rejected_temporary.open("x", encoding="utf-8").close()
            os.replace(rejected_temporary, rejected_path)
            with summary_temporary.open(
                "x",
                encoding="utf-8",
                newline="\n",
            ) as destination:
                json.dump(
                    {
                        "total_records": result.records_written,
                        "accepted_records": result.records_written,
                        "rejected_records": 0,
                        "failures_by_code": {},
                        "failure_samples": [],
                    },
                    destination,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                destination.write("\n")
            os.replace(summary_temporary, summary_path)

            return self.repository.transition(
                import_id,
                ImportState.completed,
                total_records=result.records_written,
                accepted_records=result.records_written,
                rejected_records=0,
            )
        except Exception:
            logger.exception("Import processing failed", extra={"import_id": import_id})
            self._discard_loaded_import(import_id)
            for path in (
                processed_temporary,
                processed_path,
                rejected_temporary,
                rejected_path,
                summary_temporary,
                summary_path,
            ):
                path.unlink(missing_ok=True)
            return self.repository.transition(
                import_id,
                ImportState.failed,
                failure_code="processing_failed",
                failure_message="Import processing failed.",
            )

    def _discard_loaded_import(self, import_id: str) -> None:
        try:
            self.batch_loader.discard_import(import_id)
        except Exception:
            logger.exception(
                "Failed to compensate imported database rows",
                extra={"import_id": import_id},
            )
