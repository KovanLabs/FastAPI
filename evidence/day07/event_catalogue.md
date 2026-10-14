# Day 07 event catalogue

This catalogue is the contract for structured application logging. Event
names are stable values; request, import, engine, and batch identifiers belong
in fields rather than in event names.

## Core JSON fields

Every emitted application event contains:

- `timestamp`: Asia/Kolkata ISO 8601 timestamp with a `+05:30` offset;
- `level`: `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`;
- `logger`: module logger name;
- `event`: stable event name;
- `message`: safe human-readable description.

Events are emitted to the console and to `logs/application.log` by default.
The file handler rotates at the host operating system's local midnight and
retains 30 archived daily files. Production hosts must use the
`Asia/Kolkata` timezone when rollover must align with the timestamps. The path
and retention can be configured with `APP_LOG_FILE_PATH` and
`APP_LOG_RETENTION_DAYS`.
The in-process rotating handler is intended for a single application process.
Multi-worker deployments must send logs to one listener or use an external log
collector instead of allowing workers to rotate the same file.

Only explicitly allowlisted event fields are serialized. Correlated events may
contain `request_id`, `import_id`, `engine`, `batch_number`, and `attempt`.
Lifecycle events may also contain bounded counts, status, and duration fields.

## Implemented events

| Event | Level | Emitted when | Required event fields | Volume rule |
|---|---|---|---|---|
| `application.started` | `INFO` | Application lifespan setup completes | `environment`, `configured_log_level` | Once per application lifespan |
| `request.started` | `INFO` | An HTTP request enters the application | `request_id`, `http_method` | Once per request |
| `request.completed` | `INFO` | An HTTP request completes | `request_id`, `http_method`, `status_code`, `duration_ms` | Once per completed request |
| `request.failed` | `ERROR` | An unhandled request exception escapes | Request fields and protected `exception` | Once per failed request |
| `import.queued` | `INFO` | A stored upload becomes a queued job | `request_id`, `import_id` | Once per queued import |
| `import.started` | `INFO` | Background import processing begins | `request_id`, `import_id`, `attempt` | Once per execution attempt |
| `engine.started` | `INFO` | The selected processing adapter begins | Import fields and `engine` | Once per engine execution |
| `engine.completed` | `INFO` | The selected processing adapter completes | Engine fields and `records_processed` | Once per engine execution |
| `engine.failed` | `ERROR` | The selected processing adapter fails | Engine fields and protected `exception` | Once per failed execution |
| `batch.loading_started` | `INFO` | DuckDB loading begins | `import_id`, `engine`, `attempt` | Once per load attempt |
| `batch.progress` | `INFO` | A configured record threshold is crossed | Batch fields and `records_processed` | At most once per 100,000 records |
| `batch.failed` | `ERROR` | A specific batch cannot commit | Batch fields and protected `exception` | Once per failed batch attempt |
| `batch.loading_failed` | `ERROR` | The batch-loading operation cannot complete | Load fields, `retryable`, protected `exception` | Once per failed load attempt |
| `batch.loading_completed` | `INFO` | DuckDB loading completes | Load fields and bounded result counts | Once per load attempt |
| `import.completed` | `INFO` | An import reaches a successful terminal state | Import fields and bounded result counts | Once per completed import |
| `import.failed` | `ERROR` | An import cannot complete | `import_id`, protected `exception` when available | Once per failed import execution |
| `import.compensation_failed` | `ERROR` | Cleanup of loaded rows fails | `import_id`, protected `exception` when available | Once per failed compensation attempt |

The protected exception representation keeps the exception class and stack
frame filenames, line numbers, and function names. It excludes the raw
exception message, source lines, and absolute filesystem paths.

Messages redact email addresses, bearer tokens, database URLs, and common
secret assignments. Arbitrary extra fields are excluded unless allowlisted;
order payloads and customer fields are never catalogued logging fields.

`ContextVar` scopes are reset with their tokens after requests, imports,
engines, and batches. This prevents concurrent operations from inheriting one
another's identifiers. The default 100,000-record progress interval produces
at most 100 progress events for 10 million records.

## Level policy

| Level | Intended use | Example event |
|---|---|---|
| `DEBUG` | Optional internal diagnostics | `engine.configuration` (planned) |
| `INFO` | Normal lifecycle confirmation | `import.completed` |
| `WARNING` | Recoverable unexpected condition | `batch.retry_scheduled` (planned) |
| `ERROR` | An operation cannot complete | `import.failed` |
| `CRITICAL` | The application cannot safely continue | `application.startup_failed` (planned) |

Automatic retry scheduling and its warning event are outside this increment.
When a caller retries a load, it supplies the new positive `attempt` number so
failure and progress events remain distinguishable from the original attempt.
