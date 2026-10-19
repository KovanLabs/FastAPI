import asyncio
from dataclasses import asdict
import json
import logging
import os
import shutil

from pathlib import Path
from _thread import LockType
from typing import Any, Literal

from fastapi import (
    APIRouter,
    BackgroundTasks,
    File,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import FileResponse

from app.api.dependencies import (
    ImportJobRepositoryDependency,
    ImportWriterLockDependency,
    OrderValidationServiceDependency,
    SettingsDependency,
)
from app.core.exceptions import (
    ImportNotFoundError,
    ImportNotReadyError,
    OrderAlreadyExistsError,
    OrderNotFoundError,
    StorageDirectoryUnavailableError,
    UnsupportedMediaTypeError,
)
from app.core.batch_loading import DuckDBBatchLoader
from app.core.config import ProcessingEngine, Settings
from app.core.database_health import check_database_health
from app.core.import_coordinator import ImportCoordinator
from app.core.import_jobs import (
    ImportJob,
    IllegalImportTransition,
    ImportJobNotFound,
    ImportState,
)
from app.core.import_jobs import ImportJobRepository
from app.core.log_context import bind_log_context
from app.core.import_service import store_upload
from app.core.json_array_validation import summarize_json_array_file
from app.core.ndjson_validation import summarize_ndjson_file
from app.core.order_pipeline import process_order_file
from app.core.processing_adapters import (
    create_processing_adapter,
    validate_canonical_ndjson,
)
from app.schemas.imports import (
    ImportResponse,
    ImportStageResponse,
    ImportErrorsResponse,
    ImportSummaryResponse,
    ImportStatusResponse,
    JsonArrayValidationResponse,
    NdjsonValidationResponse,
)
from app.schemas.orders import (
    CheckoutOrderRequest,
    OrderCreate,
    OrderResponse,
    OrderStatus,
)

router = APIRouter()
logger = logging.getLogger(__name__)
ORDERS: dict[str, OrderResponse] = {}


@router.get("/health/live")
def liveness() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready")
def readiness(settings: SettingsDependency) -> dict[str, str | object]:
    upload_dir = settings.upload_directory
    output_dir = settings.output_directory

    upload_ok = (
        upload_dir.exists()
        and upload_dir.is_dir()
        and bool(upload_dir.stat().st_mode & 0o222)
    )
    output_ok = (
        output_dir.exists()
        and output_dir.is_dir()
        and bool(output_dir.stat().st_mode & 0o222)
    )

    if not upload_ok:
        raise StorageDirectoryUnavailableError(str(upload_dir))
    if not output_ok:
        raise StorageDirectoryUnavailableError(str(output_dir))

    check_database_health(output_dir / "imports.duckdb")

    return {
        "status": "ready",
        "upload_directory": str(upload_dir),
        "output_directory": str(output_dir),
    }


@router.post(
    "/orders",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_order(order: OrderCreate) -> OrderResponse:
    if order.order_id in ORDERS:
        raise OrderAlreadyExistsError(order.order_id)
    response = OrderResponse(**order.model_dump())
    ORDERS[order.order_id] = response
    return response


@router.post(
    "/orders/validate",
    response_model=OrderResponse,
    status_code=status.HTTP_200_OK,
)
def validate_order(
    order: OrderCreate,
    service: OrderValidationServiceDependency,
) -> OrderResponse:
    return service.validate(order)


@router.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: str) -> OrderResponse:
    order = ORDERS.get(order_id)
    if order is None:
        raise OrderNotFoundError(order_id)
    return order


@router.get("/orders", response_model=list[OrderResponse])
def list_orders(
    order_status: OrderStatus | None = Query(default=None, alias="status"),
) -> list[OrderResponse]:
    values = list(ORDERS.values())
    if order_status is None:
        return values
    return [order for order in values if order.status == order_status]


@router.post(
    "/orders/checkout",
    response_model=CheckoutOrderRequest,
    status_code=status.HTTP_201_CREATED,
)
def create_checkout_order(payload: CheckoutOrderRequest):
    return payload


@router.post(
    "/imports",
    response_model=ImportResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Queue a JSON-array or canonical-NDJSON order import",
    description=(
        "JSON uploads are cleaned and validated by the Day 5 pipeline. "
        "NDJSON uploads must already follow the canonical accepted-record schema."
    ),
)
async def import_orders(
    request: Request,
    settings: SettingsDependency,
    repository: ImportJobRepositoryDependency,
    writer_lock: ImportWriterLockDependency,
    background_tasks: BackgroundTasks,
    auto_start: bool = Query(
        default=True,
        description=(
            "Start the complete import pipeline after upload. Set to false "
            "to run the clean, process, and load stages explicitly."
        ),
    ),
    file: UploadFile = File(...),
) -> ImportResponse:
    media_type = (file.content_type or "").split(";", 1)[0].strip().lower()
    suffix_by_type = {
        "application/x-ndjson": ".ndjson",
        "application/json": ".json",
    }
    suffix = suffix_by_type.get(media_type)
    if suffix is None:
        logger.warning(
            "Upload rejected because its media type is unsupported",
            extra={
                "event": "upload.rejected",
                "reason": "unsupported_media_type",
            },
        )
        raise UnsupportedMediaTypeError(
            "Expected an NDJSON or JSON file",
            details={"received": media_type},
        )

    result = await store_upload(file, settings, suffix=suffix)
    repository.create(
        import_id=result.import_id,
        stored_filename=result.stored_filename,
        checksum=result.checksum,
        bytes_received=result.bytes_received,
    )
    job = repository.get(result.import_id)
    with bind_log_context(import_id=result.import_id):
        if auto_start:
            job = repository.transition(result.import_id, ImportState.queued)
            logger.info(
                "Import queued",
                extra={"event": "import.queued"},
            )
            background_tasks.add_task(
                _run_import,
                repository,
                settings,
                result.import_id,
                settings.upload_directory / result.stored_filename,
                suffix,
                writer_lock,
                request.state.request_id,
            )
        else:
            logger.info(
                "Import stored for staged processing",
                extra={"event": "import.uploaded"},
            )

    return ImportResponse(
        import_id=result.import_id,
        checksum=result.checksum,
        stored_filename=result.stored_filename,
        bytes_received=result.bytes_received,
        state=job.state,
    )


@router.post(
    "/imports/{import_id}/clean",
    response_model=ImportStageResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Clean and validate a stored raw JSON import",
)
def clean_import(
    import_id: str,
    request: Request,
    settings: SettingsDependency,
    repository: ImportJobRepositoryDependency,
    background_tasks: BackgroundTasks,
) -> ImportStageResponse:
    try:
        job = repository.get(import_id)
    except ImportJobNotFound as exc:
        raise ImportNotFoundError(f"Import {import_id} was not found") from exc
    if job.state is not ImportState.uploaded:
        raise ImportNotReadyError(
            f"Import {import_id} cannot be cleaned from state {job.state}"
        )
    cleaning = _transition_for_request(
        repository,
        import_id,
        ImportState.cleaning,
        stage_attempt=1,
    )
    background_tasks.add_task(
        _run_clean_stage,
        repository,
        settings,
        import_id,
        settings.upload_directory / job.stored_filename,
        request.state.request_id,
    )
    return ImportStageResponse(import_id=import_id, state=cleaning.state)


def _run_clean_stage(
    repository: ImportJobRepository,
    settings: Settings,
    import_id: str,
    input_path: Path,
    request_id: str | None = None,
    attempt: int = 1,
) -> None:
    job_directory = settings.output_directory / import_id
    accepted_path = job_directory / "accepted.ndjson"
    rejected_path = job_directory / "rejected.ndjson"
    summary_path = job_directory / "summary.json"
    summary_temporary = job_directory / ".summary.json.part"
    with bind_log_context(
        request_id=request_id,
        import_id=import_id,
        attempt=attempt,
    ):
        logger.info("Import cleaning started", extra={"event": "cleaning.started"})
        try:
            job_directory.mkdir(parents=True, exist_ok=True)
            if input_path.suffix == ".ndjson":
                accepted_temporary = job_directory / ".accepted.ndjson.part"
                rejected_temporary = job_directory / ".rejected.ndjson.part"
                record_count = validate_canonical_ndjson(
                    input_path,
                    maximum_record_bytes=settings.maximum_record_bytes,
                )
                with input_path.open("rb") as source, accepted_temporary.open(
                    "xb"
                ) as destination:
                    shutil.copyfileobj(
                        source,
                        destination,
                        length=settings.upload_chunk_bytes,
                    )
                rejected_temporary.open("xb").close()
                summary_data = {
                    "total_records": record_count,
                    "accepted_records": record_count,
                    "rejected_records": 0,
                    "failures_by_code": {},
                    "failure_samples": [],
                    "duplicate_index_backend": "not_required",
                    "sqlite_working_store_bytes": 0,
                }
                os.replace(accepted_temporary, accepted_path)
                os.replace(rejected_temporary, rejected_path)
            else:
                summary = process_order_file(
                    input_path,
                    accepted_path=accepted_path,
                    rejected_path=rejected_path,
                    supported_currencies=settings.supported_currencies,
                    progress_log_interval=settings.progress_log_interval,
                    maximum_record_bytes=settings.maximum_record_bytes,
                )
                summary_data = asdict(summary)
            with summary_temporary.open(
                "x", encoding="utf-8", newline="\n"
            ) as destination:
                json.dump(
                    summary_data,
                    destination,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                destination.write("\n")
            os.replace(summary_temporary, summary_path)
            repository.transition(
                import_id,
                ImportState.cleaned,
                total_records=int(summary_data["total_records"]),
                accepted_records=int(summary_data["accepted_records"]),
                rejected_records=int(summary_data["rejected_records"]),
            )
            logger.info(
                "Import cleaning completed",
                extra={
                    "event": "cleaning.completed",
                    "total_records": summary_data["total_records"],
                    "accepted_records": summary_data["accepted_records"],
                    "rejected_records": summary_data["rejected_records"],
                },
            )
        except Exception:
            for path in (
                job_directory / ".accepted.ndjson.part",
                job_directory / ".rejected.ndjson.part",
                accepted_path,
                rejected_path,
                summary_temporary,
                summary_path,
            ):
                path.unlink(missing_ok=True)
            logger.exception(
                "Import cleaning failed",
                extra={"event": "cleaning.failed"},
            )
            repository.transition(
                import_id,
                ImportState.failed,
                failure_code="cleaning_failed",
                failure_message="Import cleaning failed.",
                failed_stage="cleaning",
            )


@router.post(
    "/imports/{import_id}/process",
    response_model=ImportStageResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Transform a cleaned import with the selected adapter",
)
def process_import(
    import_id: str,
    request: Request,
    settings: SettingsDependency,
    repository: ImportJobRepositoryDependency,
    background_tasks: BackgroundTasks,
    engine: ProcessingEngine = Query(default=ProcessingEngine.duckdb),
) -> ImportStageResponse:
    try:
        job = repository.get(import_id)
    except ImportJobNotFound as exc:
        raise ImportNotFoundError(f"Import {import_id} was not found") from exc
    if job.state is not ImportState.cleaned:
        raise ImportNotReadyError(
            f"Import {import_id} cannot be processed from state {job.state}"
        )

    processing = _transition_for_request(
        repository,
        import_id,
        ImportState.processing,
        stage_attempt=1,
        processing_engine=engine.value,
    )
    background_tasks.add_task(
        _run_process_stage,
        repository,
        settings,
        import_id,
        engine,
        request.state.request_id,
    )
    return ImportStageResponse(import_id=import_id, state=processing.state)


def _run_process_stage(
    repository: ImportJobRepository,
    settings: Settings,
    import_id: str,
    engine: ProcessingEngine,
    request_id: str | None = None,
    attempt: int = 1,
) -> None:
    job_directory = settings.output_directory / import_id
    accepted_path = job_directory / "accepted.ndjson"
    processed_path = job_directory / "processed.ndjson"
    processed_temporary = job_directory / ".processed.ndjson.part"
    adapter = create_processing_adapter(
        engine,
        maximum_record_bytes=settings.maximum_record_bytes,
    )
    with bind_log_context(
        request_id=request_id,
        import_id=import_id,
        engine=adapter.engine,
        attempt=attempt,
    ):
        logger.info(
            "Processing engine started",
            extra={"event": "engine.started"},
        )
        try:
            result = adapter.process(accepted_path, processed_temporary)
            os.replace(processed_temporary, processed_path)
            repository.transition(import_id, ImportState.processed)
            logger.info(
                "Processing engine completed",
                extra={
                    "event": "engine.completed",
                    "records_processed": result.records_written,
                },
            )
        except Exception:
            processed_temporary.unlink(missing_ok=True)
            processed_path.unlink(missing_ok=True)
            logger.exception(
                "Processing engine failed",
                extra={"event": "engine.failed"},
            )
            repository.transition(
                import_id,
                ImportState.failed,
                failure_code="processing_failed",
                failure_message="Import processing failed.",
                failed_stage="processing",
            )


@router.post(
    "/imports/{import_id}/load",
    response_model=ImportStageResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Load a processed import into DuckDB",
)
def load_import(
    import_id: str,
    request: Request,
    settings: SettingsDependency,
    repository: ImportJobRepositoryDependency,
    writer_lock: ImportWriterLockDependency,
    background_tasks: BackgroundTasks,
) -> ImportStageResponse:
    try:
        job = repository.get(import_id)
    except ImportJobNotFound as exc:
        raise ImportNotFoundError(f"Import {import_id} was not found") from exc
    if job.state is not ImportState.processed:
        raise ImportNotReadyError(
            f"Import {import_id} cannot be loaded from state {job.state}"
        )

    loading = _transition_for_request(
        repository,
        import_id,
        ImportState.loading,
        stage_attempt=1,
    )
    background_tasks.add_task(
        _run_load_stage,
        repository,
        settings,
        import_id,
        writer_lock,
        request.state.request_id,
    )
    return ImportStageResponse(import_id=import_id, state=loading.state)


def _run_load_stage(
    repository: ImportJobRepository,
    settings: Settings,
    import_id: str,
    writer_lock: LockType,
    request_id: str | None = None,
    attempt: int = 1,
) -> None:
    processed_path = (
        settings.output_directory / import_id / "processed.ndjson"
    )
    loader = DuckDBBatchLoader(
        settings.output_directory / "imports.duckdb",
        batch_size=settings.import_batch_size,
        maximum_batch_bytes=settings.import_batch_bytes,
        maximum_record_bytes=settings.maximum_record_bytes,
        maximum_items_per_order=settings.maximum_items_per_order,
        progress_log_interval=settings.progress_log_interval,
        maximum_attempts=settings.batch_max_attempts,
    )
    with bind_log_context(
        request_id=request_id,
        import_id=import_id,
        engine="duckdb",
        attempt=attempt,
    ):
        try:
            with writer_lock:
                loader.load(
                    processed_path,
                    import_id=import_id,
                    attempt=attempt,
                )
            current = repository.get(import_id)
            terminal_state = (
                ImportState.completed_with_errors
                if current.rejected_records
                else ImportState.completed
            )
            completed = repository.transition(import_id, terminal_state)
            logger.info(
                "Import processing completed",
                extra={
                    "event": "import.completed",
                    "import_state": completed.state.value,
                    "total_records": completed.total_records,
                    "accepted_records": completed.accepted_records,
                    "rejected_records": completed.rejected_records,
                },
            )
        except Exception:
            try:
                loader.discard_import(import_id)
            except Exception:
                logger.exception(
                    "Failed to compensate imported database rows",
                    extra={"event": "import.compensation_failed"},
                )
            logger.exception(
                "Import loading failed",
                extra={"event": "import.failed"},
            )
            repository.transition(
                import_id,
                ImportState.failed,
                failure_code="loading_failed",
                failure_message="Import loading failed.",
                failed_stage="loading",
            )


@router.post(
    "/imports/{import_id}/retry",
    response_model=ImportStageResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Retry only the failed import stage",
)
def retry_import(
    import_id: str,
    request: Request,
    settings: SettingsDependency,
    repository: ImportJobRepositoryDependency,
    writer_lock: ImportWriterLockDependency,
    background_tasks: BackgroundTasks,
) -> ImportStageResponse:
    try:
        job = repository.get(import_id)
    except ImportJobNotFound as exc:
        raise ImportNotFoundError(f"Import {import_id} was not found") from exc
    if job.state is not ImportState.failed:
        raise ImportNotReadyError(
            f"Import {import_id} cannot be retried from state {job.state}"
        )

    stage_by_failure = {
        "cleaning": ImportState.cleaning,
        "processing": ImportState.processing,
        "loading": ImportState.loading,
    }
    target = stage_by_failure.get(job.failed_stage or "")
    if target is None:
        raise ImportNotReadyError(
            f"Import {import_id} does not have a retryable staged failure"
        )

    job_directory = settings.output_directory / import_id
    if target is ImportState.processing and not (
        job_directory / "accepted.ndjson"
    ).is_file():
        raise ImportNotReadyError(
            f"Import {import_id} has no accepted artifact to reprocess"
        )
    if target is ImportState.loading and not (
        job_directory / "processed.ndjson"
    ).is_file():
        raise ImportNotReadyError(
            f"Import {import_id} has no processed artifact to reload"
        )

    retry_engine: str | None = None
    if target is ImportState.processing:
        retry_engine = job.processing_engine or ProcessingEngine.duckdb.value
    elif target is ImportState.loading:
        retry_engine = ProcessingEngine.duckdb.value
    next_attempt = job.stage_attempt + 1
    retried = _transition_for_request(
        repository,
        import_id,
        target,
        stage_attempt=next_attempt,
    )
    with bind_log_context(
        request_id=request.state.request_id,
        import_id=import_id,
        engine=retry_engine,
        attempt=next_attempt,
    ):
        logger.warning(
            "Import stage retry scheduled",
            extra={
                "event": "import.retry_scheduled",
                "reason": job.failure_code,
                "next_attempt": next_attempt,
                "retryable": True,
            },
        )
    if target is ImportState.cleaning:
        background_tasks.add_task(
            _run_clean_stage,
            repository,
            settings,
            import_id,
            settings.upload_directory / job.stored_filename,
            request.state.request_id,
            next_attempt,
        )
    elif target is ImportState.processing:
        background_tasks.add_task(
            _run_process_stage,
            repository,
            settings,
            import_id,
            ProcessingEngine(retry_engine or ProcessingEngine.duckdb.value),
            request.state.request_id,
            next_attempt,
        )
    else:
        background_tasks.add_task(
            _run_load_stage,
            repository,
            settings,
            import_id,
            writer_lock,
            request.state.request_id,
            next_attempt,
        )
    return ImportStageResponse(import_id=import_id, state=retried.state)


def _transition_for_request(
    repository: ImportJobRepository,
    import_id: str,
    target: ImportState,
    **changes: Any,
) -> ImportJob:
    try:
        return repository.transition(import_id, target, **changes)
    except IllegalImportTransition as exc:
        raise ImportNotReadyError(
            f"Import {import_id} cannot transition to {target}"
        ) from exc


@router.get(
    "/imports/{import_id}/artifacts/{artifact}",
    response_class=FileResponse,
    summary="Download a published import artifact",
)
def get_import_artifact(
    import_id: str,
    artifact: Literal[
        "source",
        "accepted",
        "rejected",
        "processed",
        "summary",
    ],
    settings: SettingsDependency,
    repository: ImportJobRepositoryDependency,
) -> FileResponse:
    try:
        job = repository.get(import_id)
    except ImportJobNotFound as exc:
        raise ImportNotFoundError(f"Import {import_id} was not found") from exc

    if artifact == "source":
        path = settings.upload_directory / job.stored_filename
        media_type = (
            "application/x-ndjson"
            if path.suffix == ".ndjson"
            else "application/json"
        )
    else:
        filename_by_artifact = {
            "accepted": "accepted.ndjson",
            "rejected": "rejected.ndjson",
            "processed": "processed.ndjson",
            "summary": "summary.json",
        }
        path = (
            settings.output_directory
            / import_id
            / filename_by_artifact[artifact]
        )
        media_type = (
            "application/json"
            if artifact == "summary"
            else "application/x-ndjson"
        )
    if not path.is_file():
        raise ImportNotReadyError(
            f"Artifact {artifact} is not available for import {import_id}"
        )
    return FileResponse(path, media_type=media_type, filename=path.name)


def _run_import(
    repository: ImportJobRepository,
    settings: Settings,
    import_id: str,
    input_path: Path,
    suffix: str,
    writer_lock: LockType,
    request_id: str | None = None,
) -> None:
    with bind_log_context(
        request_id=request_id,
        import_id=import_id,
        attempt=1,
    ):
        coordinator = ImportCoordinator(
            repository=repository,
            adapter=create_processing_adapter(
                settings.processing_engine,
                maximum_record_bytes=settings.maximum_record_bytes,
            ),
            batch_loader=DuckDBBatchLoader(
                settings.output_directory / "imports.duckdb",
                batch_size=settings.import_batch_size,
                maximum_batch_bytes=settings.import_batch_bytes,
                maximum_record_bytes=settings.maximum_record_bytes,
                maximum_items_per_order=settings.maximum_items_per_order,
                progress_log_interval=settings.progress_log_interval,
                maximum_attempts=settings.batch_max_attempts,
            ),
            output_directory=settings.output_directory,
            supported_currencies=settings.supported_currencies,
            progress_log_interval=settings.progress_log_interval,
        )
        with writer_lock:
            if suffix == ".json":
                coordinator.run(import_id, input_path)
            else:
                coordinator.run_canonical_ndjson(import_id, input_path)


@router.get(
    "/imports/{import_id}",
    response_model=ImportStatusResponse,
    status_code=status.HTTP_200_OK,
)
def get_import_status(
    import_id: str,
    repository: ImportJobRepositoryDependency,
) -> ImportStatusResponse:
    try:
        job = repository.get(import_id)
    except ImportJobNotFound as exc:
        raise ImportNotFoundError(f"Import {import_id} was not found") from exc

    return ImportStatusResponse(
        import_id=job.import_id,
        state=job.state,
        bytes_received=job.bytes_received,
        total_records=job.total_records,
        accepted_records=job.accepted_records,
        rejected_records=job.rejected_records,
        created_at=job.created_at,
        updated_at=job.updated_at,
        failure_code=job.failure_code,
        failure_message=job.failure_message,
        failed_stage=job.failed_stage,
        stage_attempt=job.stage_attempt,
        processing_engine=job.processing_engine,
    )


@router.get(
    "/imports/{import_id}/summary",
    response_model=ImportSummaryResponse,
    status_code=status.HTTP_200_OK,
)
def get_import_summary(
    import_id: str,
    settings: SettingsDependency,
    repository: ImportJobRepositoryDependency,
) -> ImportSummaryResponse:
    try:
        job = repository.get(import_id)
    except ImportJobNotFound as exc:
        raise ImportNotFoundError(f"Import {import_id} was not found") from exc

    if job.state not in {
        ImportState.completed,
        ImportState.completed_with_errors,
    }:
        raise ImportNotReadyError(f"Import {import_id} has no published summary")
    summary_path = settings.output_directory / import_id / "summary.json"
    if not summary_path.is_file():
        raise ImportNotReadyError(f"Import {import_id} has no published summary")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    return ImportSummaryResponse(
        import_id=import_id,
        state=job.state,
        total_records=job.total_records,
        accepted_records=job.accepted_records,
        rejected_records=job.rejected_records,
        failures_by_code=summary["failures_by_code"],
    )


@router.get(
    "/imports/{import_id}/errors",
    response_model=ImportErrorsResponse,
    status_code=status.HTTP_200_OK,
)
def get_import_errors(
    import_id: str,
    settings: SettingsDependency,
    repository: ImportJobRepositoryDependency,
    limit: int = Query(default=20, ge=1, le=100),
) -> ImportErrorsResponse:
    try:
        job = repository.get(import_id)
    except ImportJobNotFound as exc:
        raise ImportNotFoundError(f"Import {import_id} was not found") from exc

    if job.state not in {
        ImportState.completed,
        ImportState.completed_with_errors,
    }:
        raise ImportNotReadyError(f"Import {import_id} has no published errors")
    rejected_path = settings.output_directory / import_id / "rejected.ndjson"
    if not rejected_path.is_file():
        raise ImportNotReadyError(f"Import {import_id} has no published errors")

    errors: list[dict[str, object]] = []
    with rejected_path.open("r", encoding="utf-8") as source:
        for line in source:
            errors.append(json.loads(line))
            if len(errors) == limit:
                break
    return ImportErrorsResponse(
        import_id=import_id,
        total_errors=job.rejected_records,
        returned_errors=len(errors),
        errors=errors,
    )


@router.post(
    "/imports/ndjson/validate",
    response_model=NdjsonValidationResponse,
    status_code=status.HTTP_200_OK,
    summary="Upload and validate an NDJSON file",
    description=(
        "Stores the uploaded NDJSON file, parses it one bounded line at a time, "
        "and returns counts plus at most 20 record-level failure samples. "
        "JSON arrays are not accepted by this endpoint."
    ),
)
async def validate_ndjson_import(
    settings: SettingsDependency,
    file: UploadFile = File(...),
) -> NdjsonValidationResponse:
    media_type = (file.content_type or "").split(";", 1)[0].strip().lower()
    if media_type not in {"application/x-ndjson", "application/octet-stream"}:
        raise UnsupportedMediaTypeError(
            "Expected an NDJSON file",
            details={"received": media_type},
        )

    upload = await store_upload(file, settings, suffix=".ndjson")
    stored_path = settings.upload_directory / upload.stored_filename
    summary = await asyncio.to_thread(
        summarize_ndjson_file,
        stored_path,
        maximum_record_bytes=settings.maximum_record_bytes,
    )

    return NdjsonValidationResponse(
        import_id=upload.import_id,
        checksum=upload.checksum,
        stored_filename=upload.stored_filename,
        bytes_received=upload.bytes_received,
        state="completed",
        valid_records=summary.valid_records,
        skipped_blank=summary.skipped_blank,
        failures_by_code=summary.failures_by_code,
        failure_count=summary.failure_count,
        failure_samples=summary.failure_samples,
    )


@router.post(
    "/imports/json/validate",
    response_model=JsonArrayValidationResponse,
    status_code=status.HTTP_200_OK,
    summary="Upload and validate a JSON array",
    description=(
        "Stores a standard JSON file whose top-level value is an array, reads "
        "it in 64 KiB chunks, and returns a bounded validation summary."
    ),
)
async def validate_json_array_import(
    settings: SettingsDependency,
    file: UploadFile = File(...),
) -> JsonArrayValidationResponse:
    media_type = (file.content_type or "").split(";", 1)[0].strip().lower()
    if media_type not in {"application/json", "application/octet-stream"}:
        raise UnsupportedMediaTypeError(
            "Expected a JSON file",
            details={"received": media_type},
        )

    upload = await store_upload(file, settings, suffix=".json")
    stored_path = settings.upload_directory / upload.stored_filename
    summary = await asyncio.to_thread(
        summarize_json_array_file,
        stored_path,
        maximum_record_bytes=settings.maximum_record_bytes,
    )

    return JsonArrayValidationResponse(
        import_id=upload.import_id,
        checksum=upload.checksum,
        stored_filename=upload.stored_filename,
        bytes_received=upload.bytes_received,
        state="completed",
        valid_records=summary.valid_records,
        failures_by_code=summary.failures_by_code,
        failure_count=summary.failure_count,
        failure_samples=summary.failure_samples,
    )
