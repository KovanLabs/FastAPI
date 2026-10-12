from contextlib import asynccontextmanager
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.api.orders import router
from app.core.config import get_settings
from app.core.exceptions import ApplicationError


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    settings.upload_directory.mkdir(parents=True, exist_ok=True)
    settings.output_directory.mkdir(parents=True, exist_ok=True)
    yield


app = FastAPI(
    title=get_settings().app_name,
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
        request_id = request_id or f"req_{uuid4().hex}"
        scope.setdefault("state", {})["request_id"] = request_id
        started = perf_counter()

        async def add_response_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                headers["X-Process-Time"] = f"{perf_counter() - started:.6f}"
                headers["X-Request-ID"] = request_id
            await send(message)

        await self.app(scope, receive, add_response_headers)


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
