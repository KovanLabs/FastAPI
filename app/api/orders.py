import asyncio
import json
import logging

from pathlib import Path
from _thread import LockType

from fastapi import (
    APIRouter,
    BackgroundTasks,
    File,
    Query,
    Request,
    UploadFile,
    status,
)

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
from app.core.config import Settings
from app.core.import_coordinator import ImportCoordinator
from app.core.import_jobs import ImportJobNotFound, ImportState
from app.core.import_jobs import ImportJobRepository
from app.core.log_context import bind_log_context
from app.core.import_service import store_upload
from app.core.json_array_validation import summarize_json_array_file
from app.core.ndjson_validation import summarize_ndjson_file
from app.core.processing_adapters import create_processing_adapter
from app.schemas.imports import (
    ImportResponse,
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
    file: UploadFile = File(...),
) -> ImportResponse:
    media_type = (file.content_type or "").split(";", 1)[0].strip().lower()
    suffix_by_type = {
        "application/x-ndjson": ".ndjson",
        "application/json": ".json",
    }
    suffix = suffix_by_type.get(media_type)
    if suffix is None:
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
    job = repository.transition(result.import_id, ImportState.queued)
    with bind_log_context(import_id=result.import_id):
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

    return ImportResponse(
        import_id=result.import_id,
        checksum=result.checksum,
        stored_filename=result.stored_filename,
        bytes_received=result.bytes_received,
        state=job.state,
    )


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
            ),
            output_directory=settings.output_directory,
            supported_currencies=settings.supported_currencies,
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
