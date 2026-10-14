"""Task-local correlation context for structured application logs."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token


_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_import_id: ContextVar[str | None] = ContextVar("import_id", default=None)
_engine: ContextVar[str | None] = ContextVar("engine", default=None)
_batch_number: ContextVar[int | None] = ContextVar("batch_number", default=None)
_attempt: ContextVar[int | None] = ContextVar("attempt", default=None)

_CONTEXT_VARIABLES: dict[str, ContextVar[str | int | None]] = {
    "request_id": _request_id,
    "import_id": _import_id,
    "engine": _engine,
    "batch_number": _batch_number,
    "attempt": _attempt,
}


@contextmanager
def bind_log_context(
    *,
    request_id: str | None = None,
    import_id: str | None = None,
    engine: str | None = None,
    batch_number: int | None = None,
    attempt: int | None = None,
) -> Iterator[None]:
    """Bind supplied correlation fields and reset them on scope exit."""

    values: dict[str, str | int | None] = {
        "request_id": request_id,
        "import_id": import_id,
        "engine": engine,
        "batch_number": batch_number,
        "attempt": attempt,
    }
    tokens: list[
        tuple[ContextVar[str | int | None], Token[str | int | None]]
    ] = []
    try:
        for field, value in values.items():
            if value is not None:
                variable = _CONTEXT_VARIABLES[field]
                tokens.append((variable, variable.set(value)))
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


def current_log_context() -> dict[str, str | int]:
    """Return correlation fields bound to the current execution context."""

    return {
        field: value
        for field, variable in _CONTEXT_VARIABLES.items()
        if (value := variable.get()) is not None
    }
