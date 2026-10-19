# Day 07 Verification

## Successful staged import

The public HTTP workflow test
`test_staged_import_can_be_reconstructed_from_correlated_logs` exercises upload,
clean, DuckDB processing, and DuckDB loading. Filtering by its `import_id`
produces this ordered timeline:

```text
upload.started
upload.completed
import.uploaded
cleaning.started
parsing.started
parsing.completed
validation.completed
cleaning.completed
engine.started
engine.completed
batch.loading_started
batch.loading_completed
import.completed
```

Every event carries the initiating `request_id` and `import_id`. Engine and
batch events carry the bounded engine value `duckdb`. The input contains the
unique marker `secret-buyer@example.com`; the test proves that it is absent
from serialized logs.

## Failed and retried stage

The public HTTP workflow test
`test_failed_staged_processing_retry_keeps_engine_and_attempt_context` forces
the DuckDB processing stage to fail its record-size check, increases the limit,
and retries only that stage. The correlated timeline is:

```text
engine.started          engine=duckdb attempt=1
engine.failed           engine=duckdb attempt=1 exception=AdapterExecutionError
import.retry_scheduled  engine=duckdb attempt=2 next_attempt=2 retryable=true
engine.started          engine=duckdb attempt=2
engine.completed        engine=duckdb attempt=2
```

The retry events use the retry request ID, retain the originally selected
engine, and preserve protected exception type and frames without serializing
the raw exception message. The unique marker `retry-secret@example.com` is
absent from serialized logs.

## Cleaning failure

`test_failed_staged_cleaning_emits_one_protected_correlated_event` uploads
truncated JSON and proves that exactly one `cleaning.failed` event contains the
request ID, import ID, attempt, and protected `InvalidJsonError` stack. It also
proves that accepted and rejected artifacts are not left published.

## Concurrency context and handler ownership

`test_concurrent_tasks_keep_their_correlation_context_isolated` proves that two
concurrent contexts retain different import IDs. Sequential request and raised
scope tests prove token-based reset. Repeated configuration tests prove that
console and file handlers do not duplicate events.

## Database health exceptions redaction and volume

Database-health tests cover successful and failed DuckDB checks with protected
exceptions. Redaction tests cover email addresses, authorization values,
tokens, database URLs, and arbitrary non-allowlisted extras. The progress gate
test proves that ten million records produce at most 100 progress events at the
default 100,000-record interval.

## Focused verification command

```bash
.venv/bin/python -c 'import pytest; raise SystemExit(pytest.main(["tests/test_logging_config.py", "tests/test_database_health.py", "tests/test_progress_logging.py", "-q"]))'
```

Observed result on 16 October 2026: `35 passed, 1 warning in 1.62s`. The warning
is Starlette's deprecation notice for the AnyIO `BlockingPortal` alias.

An event-name comparison across `app/**/*.py` and `event_catalogue.md` found 37
emitted events, zero events missing from the catalogue, and zero catalogue
events without an emitter.

The complete repository regression suite passed with `284 passed, 1 warning in
5.71s` on 16 October 2026.
