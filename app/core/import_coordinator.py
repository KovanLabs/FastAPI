"""Coordinate deferred JSON and canonical NDJSON imports."""

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
from app.core.log_context import bind_log_context
from app.core.order_pipeline import process_order_file
from app.core.processing_adapters import AdapterResult, OrderProcessingAdapter


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
        progress_log_interval: int = 100_000,
    ) -> None:
        self.repository = repository
        self.adapter = adapter
        self.batch_loader = batch_loader
        self.output_directory = output_directory
        self.supported_currencies = frozenset(supported_currencies)
        self.progress_log_interval = progress_log_interval

    def run(self, import_id: str, input_path: Path) -> ImportJob:
        with bind_log_context(import_id=import_id, attempt=1):
            return self._run_json_import(import_id, input_path)

    def _run_json_import(self, import_id: str, input_path: Path) -> ImportJob:
        self.repository.transition(import_id, ImportState.processing)
        logger.info("Import processing started", extra={"event": "import.started"})
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
                progress_log_interval=self.progress_log_interval,
                maximum_record_bytes=self.adapter.maximum_record_bytes,
            )
            self._process_with_adapter(accepted_path, processed_temporary)
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
            completed = self.repository.transition(
                import_id,
                terminal_state,
                total_records=summary.total_records,
                accepted_records=summary.accepted_records,
                rejected_records=summary.rejected_records,
            )
            self._log_import_completed(completed)
            return completed
        except Exception:
            logger.exception(
                "Import processing failed",
                extra={"event": "import.failed", "import_id": import_id},
            )
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

        with bind_log_context(import_id=import_id, attempt=1):
            return self._run_canonical_ndjson(import_id, input_path)

    def _run_canonical_ndjson(
        self,
        import_id: str,
        input_path: Path,
    ) -> ImportJob:
        self.repository.transition(import_id, ImportState.processing)
        logger.info("Import processing started", extra={"event": "import.started"})
        job_directory = self.output_directory / import_id
        processed_path = job_directory / "processed.ndjson"
        processed_temporary = job_directory / ".processed.ndjson.part"
        rejected_path = job_directory / "rejected.ndjson"
        rejected_temporary = job_directory / ".rejected.ndjson.part"
        summary_path = job_directory / "summary.json"
        summary_temporary = job_directory / ".summary.json.part"

        try:
            job_directory.mkdir(parents=True, exist_ok=True)
            logger.info(
                "Canonical NDJSON parsing started",
                extra={"event": "parsing.started"},
            )
            try:
                result = self._process_with_adapter(
                    input_path,
                    processed_temporary,
                )
            except Exception:
                logger.exception(
                    "Canonical NDJSON parsing failed",
                    extra={"event": "parsing.failed"},
                )
                raise
            logger.info(
                "Canonical NDJSON parsing completed",
                extra={
                    "event": "parsing.completed",
                    "records_processed": result.records_written,
                },
            )
            logger.info(
                "Canonical NDJSON validation completed",
                extra={
                    "event": "validation.completed",
                    "total_records": result.records_written,
                    "accepted_records": result.records_written,
                    "rejected_records": 0,
                },
            )
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

            completed = self.repository.transition(
                import_id,
                ImportState.completed,
                total_records=result.records_written,
                accepted_records=result.records_written,
                rejected_records=0,
            )
            self._log_import_completed(completed)
            return completed
        except Exception:
            logger.exception(
                "Import processing failed",
                extra={"event": "import.failed", "import_id": import_id},
            )
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

    def _process_with_adapter(
        self,
        input_path: Path,
        temporary_output_path: Path,
    ) -> AdapterResult:
        with bind_log_context(engine=self.adapter.engine):
            logger.debug(
                "Processing engine configuration",
                extra={
                    "event": "engine.configuration",
                    "maximum_record_bytes": self.adapter.maximum_record_bytes,
                },
            )
            logger.info(
                "Processing engine started",
                extra={"event": "engine.started"},
            )
            try:
                result = self.adapter.process(input_path, temporary_output_path)
            except Exception:
                logger.exception(
                    "Processing engine failed",
                    extra={"event": "engine.failed"},
                )
                raise
            logger.info(
                "Processing engine completed",
                extra={
                    "event": "engine.completed",
                    "records_processed": result.records_written,
                },
            )
            return result

    @staticmethod
    def _log_import_completed(job: ImportJob) -> None:
        logger.info(
            "Import processing completed",
            extra={
                "event": "import.completed",
                "import_state": job.state.value,
                "total_records": job.total_records,
                "accepted_records": job.accepted_records,
                "rejected_records": job.rejected_records,
            },
        )

    def _discard_loaded_import(self, import_id: str) -> None:
        try:
            self.batch_loader.discard_import(import_id)
        except Exception:
            logger.exception(
                "Failed to compensate imported database rows",
                extra={
                    "event": "import.compensation_failed",
                    "import_id": import_id,
                },
            )
