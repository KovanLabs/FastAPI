import asyncio

from fastapi import APIRouter, File, Query, UploadFile, status

from app.api.dependencies import OrderValidationServiceDependency, SettingsDependency
from app.core.exceptions import (
    OrderAlreadyExistsError,
    OrderNotFoundError,
    StorageDirectoryUnavailableError,
    UnsupportedMediaTypeError,
)
from app.core.import_service import store_upload
from app.core.json_array_validation import summarize_json_array_file
from app.core.ndjson_validation import summarize_ndjson_file
from app.schemas.imports import (
    ImportResponse,
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
)
async def import_orders(
    settings: SettingsDependency,
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

    return ImportResponse(
        import_id=result.import_id,
        checksum=result.checksum,
        stored_filename=result.stored_filename,
        bytes_received=result.bytes_received,
        state="queued",
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
    # Some browsers report an unknown .ndjson file as application/octet-stream.
    # The endpoint path already selects the NDJSON parser, so allow that generic
    # media type while still rejecting explicitly different formats such as JSON.
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
