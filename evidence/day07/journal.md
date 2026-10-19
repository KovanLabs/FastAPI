# Day 07 Engineering Journal

## What I built

Structured JSON logging now covers application, request, upload, cleaning,
parsing, validation, processing engine, batch loading, retry, database health,
and terminal import events. Context variables correlate request, import,
engine, batch, and attempt scopes and reset using their tokens.

## What I can now explain without notes

A log level describes severity, while a stable event name describes what
happened. Request context ends with the HTTP request; background work explicitly
binds its request and import identifiers. The formatter serializes allowlisted
fields and a protected exception representation.

## Most important failure I diagnosed

The staged `import.retry_scheduled` event originally omitted the selected
processing engine. A public-workflow test exposed the missing field. The retry
now derives its bounded engine from persisted job state, so a retry cannot
silently switch engines and its timeline remains reconstructable.

## Evidence that changed my explanation

The complete staged success test showed that correlation must cross four
separate HTTP requests rather than rely on one long request scope. The failed
retry test showed that `import_id` provides job continuity while each stage can
retain its own initiating `request_id`.

## Documentation used

- Python Logging HOWTO, Logging exceptions: https://docs.python.org/3/howto/logging.html
- Python Logging Cookbook, Use of contextvars: https://docs.python.org/3/howto/logging-cookbook.html
- Python logging reference, LogRecord and handlers: https://docs.python.org/3/library/logging.html
- Python contextvars, Token and reset: https://docs.python.org/3/library/contextvars.html
- FastAPI Middleware, before and after the response: https://fastapi.tiangolo.com/tutorial/middleware/
- FastAPI Handling Errors, custom exception handlers: https://fastapi.tiangolo.com/tutorial/handling-errors/

## Remaining uncertainty

The in-process rotating file handler is a single-process course implementation.
A multi-worker deployment needs one logging listener or an external collector.

## What I will retrieve from memory tomorrow

The difference between log events, trace spans, and metrics, and how request and
job correlation should connect to OpenTelemetry trace context.
