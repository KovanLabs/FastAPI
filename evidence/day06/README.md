# Day 06 evidence index

This directory maps the implementation and tests to the Day 06 exit gate.

## Implemented evidence

- [Engine equivalence contract](equivalence_contract.md)
- [State machine and API contracts](state_and_api_contracts.md)
- [Failure, restart, concurrency, and resource evidence](failure_and_resource_evidence.md)
- [Production replacement design](production_replacement_design.md)
- [Technical journal](journal.md)
- Adapter implementation: `app/core/processing_adapters.py`
- Transactional loader: `app/core/batch_loading.py`
- Repository and state machine: `app/core/import_jobs.py`
- Coordinator: `app/core/import_coordinator.py`
- HTTP workflow: `app/api/orders.py`
- Accepted-input golden fixture: `tests/golden/day5/`
- Enriched-output golden fixture: `tests/golden/day6/`

The longer benchmark record remains in `evidence/day-06-polars-adapter.md`.

## Configuration

The request path runs one configured adapter, not all adapters. The default is
the low-memory Python reference and can be changed with:

```bash
APP_PROCESSING_ENGINE=polars
APP_PROCESSING_ENGINE=duckdb
```

Supported values are `python`, `polars`, and `duckdb`. Invalid engines and
non-positive resource limits fail settings validation during startup.

The loader is bounded by four dimensions:

- 5,000 records per transaction by default;
- 16 MiB of encoded NDJSON lines per transaction by default;
- 1 MiB per record and 1,000 items per order by default.

## Exit-gate status

On 12 October 2026, the complete repository suite reported `236 passed` with
one third-party Starlette/AnyIO deprecation warning. Syntax compilation,
dependency consistency, and diff whitespace checks also passed.

The implementation, automated tests, and technical evidence are present. The
learner quiz score and personal teach-back remain pending; they must be
completed by the learner and must not be inferred from passing tests.
