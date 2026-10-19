from contextlib import asynccontextmanager
import logging
import re
from threading import Lock
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.api.orders import router
from app.core.config import get_settings
from app.core.exceptions import ApplicationError
from app.core.import_jobs import InMemoryImportJobRepository
from app.core.log_context import bind_log_context
from app.core.logging_config import configure_logging


logger = logging.getLogger(__name__)
_SAFE_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging("INFO")
    environment = "unknown"
    try:
        settings = get_settings()
        environment = settings.environment
        app.title = settings.app_name
        configure_logging(
            settings.log_level,
            file_path=settings.log_file_path,
            file_backup_count=settings.log_retention_days,
        )
        settings.upload_directory.mkdir(parents=True, exist_ok=True)
        settings.output_directory.mkdir(parents=True, exist_ok=True)
        app.state.import_job_repository = InMemoryImportJobRepository()
        app.state.import_writer_lock = Lock()
    except Exception:
        logger.critical(
            "Application startup failed",
            exc_info=True,
            extra={
                "event": "application.startup_failed",
                "environment": environment,
            },
        )
        raise
    logger.info(
        "Application started",
        extra={
            "event": "application.started",
            "environment": settings.environment,
            "configured_log_level": settings.log_level.value,
        },
    )
    yield


app = FastAPI(
    title="Vendor Order Import API",
    version="0.2.0",
    lifespan=lifespan,
)
app.include_router(router)


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming_headers = dict(scope.get("headers", []))
        request_id = incoming_headers.get(b"x-request-id", b"").decode("latin-1")
        if _SAFE_REQUEST_ID.fullmatch(request_id) is None:
            request_id = f"req_{uuid4().hex}"
        scope.setdefault("state", {})["request_id"] = request_id
        started = perf_counter()
        response_status = 500

        async def add_response_headers(message: Message) -> None:
            nonlocal response_status
            if message["type"] == "http.response.start":
                response_status = message["status"]
                headers = MutableHeaders(scope=message)
                headers["X-Process-Time"] = f"{perf_counter() - started:.6f}"
                headers["X-Request-ID"] = request_id
            await send(message)

        with bind_log_context(request_id=request_id):
            logger.info(
                "HTTP request started",
                extra={
                    "event": "request.started",
                    "http_method": scope["method"],
                },
            )
            try:
                await self.app(scope, receive, add_response_headers)
            except Exception:
                logger.exception(
                    "HTTP request failed",
                    extra={
                        "event": "request.failed",
                        "http_method": scope["method"],
                        "status_code": response_status,
                        "duration_ms": round(
                            (perf_counter() - started) * 1_000,
                            3,
                        ),
                    },
                )
                raise
            else:
                logger.info(
                    "HTTP request completed",
                    extra={
                        "event": "request.completed",
                        "http_method": scope["method"],
                        "status_code": response_status,
                        "duration_ms": round(
                            (perf_counter() - started) * 1_000,
                            3,
                        ),
                    },
                )


app.add_middleware(RequestContextMiddleware)


@app.exception_handler(ApplicationError)
async def application_error_handler(
    request: Request,
    exc: ApplicationError,
) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.error_code,
                "message": exc.message,
                "details": exc.details,
            },
            "request_id": request.state.request_id,
        },
    )
