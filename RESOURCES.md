# FastAPI Application Resources

## Knowledge

- [FastAPI: Dependencies](https://fastapi.tiangolo.com/tutorial/dependencies/)
  Primary reference for `Depends`, `Annotated`, dependency graphs, and shared dependency aliases.
- [FastAPI: Lifespan Events](https://fastapi.tiangolo.com/advanced/events/)
  Primary reference for startup/shutdown work and `asynccontextmanager`-based lifespan functions.
- [FastAPI: Request Files](https://fastapi.tiangolo.com/tutorial/request-files/)
  Primary reference for `UploadFile`, multipart form uploads, and why `python-multipart` is installed.
- [FastAPI: Background Tasks](https://fastapi.tiangolo.com/tutorial/background-tasks/)
  Primary reference for in-process work that runs after the response is sent; this is suitable for the course integration but is not a durable external job queue.
- [FastAPI: Response Models](https://fastapi.tiangolo.com/tutorial/response-model/)
  Primary reference for output validation, serialization, OpenAPI schemas, and filtering undeclared fields.
- [FastAPI: Handling Errors](https://fastapi.tiangolo.com/tutorial/handling-errors/)
  Primary reference for application-wide exception handlers and JSON error responses.
- [FastAPI: Middleware](https://fastapi.tiangolo.com/tutorial/middleware/)
  Primary reference for request/response middleware work and middleware execution order; used for the Day 7 request-correlation lifecycle.
- [Python: Logging HOWTO](https://docs.python.org/3/howto/logging.html)
  Primary tutorial for choosing levels, logging variable data, and recording exceptions.
- [Python: Logging Cookbook](https://docs.python.org/3/howto/logging-cookbook.html)
  Primary recipes for contextual logging, `contextvars`, and avoiding duplicate or unbounded logger configurations.
- [Python: `logging`](https://docs.python.org/3/library/logging.html)
  API reference for loggers, `LogRecord` fields, handlers, filters, and formatters.
- [Python: `contextvars`](https://docs.python.org/3/library/contextvars.html)
  API reference for context-local correlation values, tokens, reset behavior, and copied contexts.
- [Pydantic: Settings Management](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
  Primary reference for `BaseSettings`, `.env` loading, environment prefixes, and typed configuration.
- [Pydantic: Models](https://docs.pydantic.dev/latest/concepts/models/)
  Primary reference for `BaseModel`, parsing, validation, serialization, and model inheritance.
- [Pydantic: Validators](https://docs.pydantic.dev/latest/concepts/validators/)
  Primary reference for field and model validators, before/after modes, validator ordering, and custom validation errors.
- [Pydantic: Error Handling](https://docs.pydantic.dev/latest/errors/errors/)
  Primary reference for `ValidationError.errors()` and structured `ErrorDetails`, especially nested `loc` paths and machine-readable `type` values.
- [Python: `asyncio.to_thread`](https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread)
  Primary reference for moving blocking file writes off the event-loop thread.
- [ASGI specification](https://asgi.readthedocs.io/en/latest/specs/main.html)
  Primary reference for the `scope`, `receive`, and `send` interface used by the custom middleware.
- [Python `io`](https://docs.python.org/3/library/io.html)
  Primary reference for binary streams, text decoding, buffering, and bounded `readline(size)`; use when reasoning about parser memory limits and UTF-8 policy.
- [Python `json`](https://docs.python.org/3/library/json.html)
  Primary reference for `dump`, `dumps`, `load`, `loads`, and `JSONDecodeError`; use when separating JSON text parsing from file I/O and classifying malformed records.
- [FastAPI: Stream JSON Lines](https://fastapi.tiangolo.com/tutorial/stream-json-lines/)
  Shows JSON Lines as one object per line and an outgoing streaming response; use to distinguish response streaming from parsing uploaded NDJSON.
- [ijson repository](https://github.com/ICRAR/ijson)
  Primary reference for incremental iteration over structured JSON, including top-level array items; use for the NDJSON versus JSON-array comparison.
- [Pydantic: TypeAdapter](https://docs.pydantic.dev/latest/concepts/type_adapter/)
  Primary reference for validating arbitrary types without a wrapper model; use after JSON syntax parsing when records need schema validation.
- [Python: `decimal`](https://docs.python.org/3/library/decimal.html)
  Primary reference for exact decimal construction, contexts, traps, and explicit `quantize()` rounding; use when the money policy is approved.
- [Python: `sqlite3`](https://docs.python.org/3/library/sqlite3.html)
  Primary reference for disk-backed duplicate state, parameter binding, commits, rollbacks, and transaction control.
- [Polars: `read_ndjson`](https://docs.pola.rs/api/python/stable/reference/api/polars.read_ndjson.html) and [Polars: `scan_ndjson`](https://docs.pola.rs/api/python/stable/reference/api/polars.scan_ndjson.html)
  Official API references for eager DataFrame reads and lazy NDJSON scans; use to decide whether Polars belongs after per-record recovery or as the reader for clean files.
- [Polars: Streaming](https://docs.pola.rs/user-guide/concepts/streaming/)
  Official guide to streaming execution and physical-plan inspection; use to identify operators that are potentially memory-intensive or fall back to the in-memory engine.
- [Polars: Lists](https://docs.pola.rs/user-guide/expressions/lists-and-arrays/)
  Official guide to expression-based operations on nested list values; used by the Day 6 adapter to derive item counts and quantity totals without Python row callbacks.
- [Python: Sorting HOW TO](https://docs.python.org/3/howto/sorting.html)
  Official reference for stable sorting and Timsort; use when comparing a sort-based implementation with an ordered-input streaming contract.
- [DuckDB: Loading JSON](https://duckdb.org/docs/stable/data/json/loading_json)
  Official reference for NDJSON readers, explicit nested schemas, object-size controls, and JSON export.
- [DuckDB: Resource Management](https://duckdb.org/docs/stable/configuration/pragmas.html#resource-management)
  Official reference for memory limits, worker threads, and temporary spill directories; the documented limit applies mainly to the buffer manager rather than total process RSS.
- [DuckDB: Profiling](https://duckdb.org/docs/stable/dev/profiling)
  Official reference for query plans and peak buffer/temp-directory metrics used as Day 6 resource evidence.

## Wisdom (Communities)

- [FastAPI GitHub Discussions](https://github.com/fastapi/fastapi/discussions)
  Use for design questions or behavior not resolved by the official documentation.

## Gaps

- `app/core/parsers.py` is custom code, so its intended behavior must be inferred from its docstrings and compared with repository tests rather than learned from a framework reference.
