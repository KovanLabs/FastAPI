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
| `application.startup_failed` | `CRITICAL` | Application lifespan setup cannot complete | `environment`, protected `exception` | Once per failed startup |
| `request.started` | `INFO` | An HTTP request enters the application | `request_id`, `http_method` | Once per request |
| `request.completed` | `INFO` | An HTTP request completes | `request_id`, `http_method`, `status_code`, `duration_ms` | Once per completed request |
| `request.failed` | `ERROR` | An unhandled request exception escapes | Request fields and protected `exception` | Once per failed request |
| `upload.started` | `INFO` | Bounded upload storage begins | Request/import fields, `maximum_bytes`, `upload_chunk_bytes` | Once per upload |
| `upload.completed` | `INFO` | The upload is atomically stored | Request/import fields, `bytes_received`, `checksum_sha256` | Once per stored upload |
| `upload.rejected` | `WARNING` | The upload is empty or exceeds its byte limit | Request/import fields, `reason`, bounded byte fields | Once per rejected upload |
| `upload.failed` | `ERROR` | Upload storage cannot complete | Request/import fields, `reason`, protected `exception` | Once per failed upload |
| `upload.cleanup_failed` | `ERROR` | A partial upload cannot be removed after failure | Request/import fields, `reason`, protected `exception` | At most once per failed upload |
| `import.queued` | `INFO` | A stored upload becomes a queued job | `request_id`, `import_id` | Once per queued import |
| `import.uploaded` | `INFO` | A stored upload is retained for explicit staged processing | `request_id`, `import_id` | Once per staged import |
| `import.started` | `INFO` | Background import processing begins | `request_id`, `import_id`, `attempt` | Once per execution attempt |
| `cleaning.started` | `INFO` | A staged import begins parsing, cleaning, and validation | `request_id`, `import_id`, `attempt` | Once per cleaning attempt |
| `cleaning.completed` | `INFO` | Accepted, rejected, and summary artifacts are published | `request_id`, `import_id`, `attempt`, accepted/rejected totals | Once per successful cleaning attempt |
| `cleaning.failed` | `ERROR` | A staged cleaning attempt cannot publish its artifacts | `request_id`, `import_id`, `attempt`, protected `exception` | Once per failed cleaning attempt |
| `parsing.started` | `INFO` | JSON-array or canonical-NDJSON parsing and validation begins | Import fields | Once per import |
| `parsing.progress` | `INFO` | A configured JSON-array parsing record threshold is crossed | Import fields and `records_processed` | At most once per 100,000 records |
| `parsing.completed` | `INFO` | The input parsing pass completes | Import fields and `records_processed` | Once per import |
| `parsing.failed` | `ERROR` | The input parsing pass cannot complete | Import fields and protected `exception` | Once per failed parsing pass |
| `validation.completed` | `INFO` | Accepted and rejected outputs reconcile | Import fields and accepted/rejected totals | Once per import |
| `engine.configuration` | `DEBUG` | The selected adapter configuration is recorded | Engine fields and `maximum_record_bytes` | Once per engine execution |
| `engine.started` | `INFO` | The selected processing adapter begins | Import fields and `engine` | Once per engine execution |
| `engine.completed` | `INFO` | The selected processing adapter completes | Engine fields and `records_processed` | Once per engine execution |
| `engine.failed` | `ERROR` | The selected processing adapter fails | Engine fields and protected `exception` | Once per failed execution |
| `batch.loading_started` | `INFO` | DuckDB loading begins | `import_id`, `engine`, `attempt` | Once per load attempt |
| `batch.progress` | `INFO` | A configured record threshold is crossed | Batch fields and `records_processed` | At most once per 100,000 records |
| `batch.failed` | `ERROR` | A specific batch cannot commit | Batch fields and protected `exception` | Once per failed batch attempt |
| `batch.loading_failed` | `ERROR` | The batch-loading operation cannot complete | Load fields, `retryable`, protected `exception` | Once per failed load attempt |
| `batch.retry_scheduled` | `WARNING` | A retryable load failure has another bounded attempt | Load fields, `next_attempt`, `retryable` | At most `batch_max_attempts - 1` per load |
| `batch.loading_completed` | `INFO` | DuckDB loading completes | Load fields and bounded result counts | Once per load attempt |
| `import.completed` | `INFO` | An import reaches a successful terminal state | Import fields and bounded result counts | Once per completed import |
| `import.failed` | `ERROR` | An import cannot complete | `import_id`, protected `exception` when available | Once per failed import execution |
| `import.retry_scheduled` | `WARNING` | A failed staged operation is scheduled again | `request_id`, `import_id`, `attempt`, `next_attempt`, `retryable`, `reason`, and bounded `engine` when processing or loading | Once per explicit staged retry |
| `import.compensation_failed` | `ERROR` | Cleanup of loaded rows fails | `import_id`, protected `exception` when available | Once per failed compensation attempt |
| `database.health_check_passed` | `DEBUG` | DuckDB opens and answers `SELECT 1` | Request fields and `database_status` | Once per readiness request when DEBUG is enabled |
| `database.health_check_failed` | `ERROR` | DuckDB cannot answer its readiness query | Request fields, `database_status`, protected `exception` | Once per failed readiness request |

The protected exception representation keeps the exception class and stack
frame filenames, line numbers, and function names. It excludes the raw
exception message, source lines, and absolute filesystem paths.

Messages redact email addresses, bearer tokens, database URLs, and common
secret assignments. Arbitrary extra fields are excluded unless allowlisted;
order payloads and customer fields are never catalogued logging fields.

`ContextVar` scopes are reset with their tokens after requests, uploads,
imports, engines, and batches. This prevents concurrent operations from
inheriting one another's identifiers. The default 100,000-record progress
interval produces at most 100 parsing events and 100 loading events for 10
million records.

## Level policy

| Level | Intended use | Example event |
|---|---|---|
| `DEBUG` | Optional internal diagnostics | `engine.configuration` |
| `INFO` | Normal lifecycle confirmation | `import.completed` |
| `WARNING` | Recoverable unexpected condition | `batch.retry_scheduled` |
| `ERROR` | An operation cannot complete | `import.failed` |
| `CRITICAL` | The application cannot safely continue | `application.startup_failed` |

Retryable DuckDB infrastructure failures are retried within the configured
`APP_BATCH_MAX_ATTEMPTS` bound, which defaults to two and cannot exceed ten.
Permanent validation and replay failures are not retried. Every attempt has a
positive `attempt` number so failure and progress events remain distinguishable.

Before the persistent DuckDB file exists, readiness checks the DuckDB engine
with an in-memory query so the probe does not create application state. After
the file exists, the same query is executed against the configured database.
