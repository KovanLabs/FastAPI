# Day 6 processing-engine evidence

## Implemented engine contract

- `StandardPythonAdapter`, `PolarsAdapter`, and `DuckDBAdapter` implement the
  same `OrderProcessingAdapter.process(input_path, temporary_output_path)`
  interface.
- Input is canonical accepted NDJSON produced by Day 5. Record numbers must be
  strictly increasing; duplicates and decreasing values fail closed.
- The ordered-input contract removed the retained Python list and both engine
  sorts. Standard Python now validates and writes one record at a time.
- Polars uses `scan_ndjson()` with an explicit nested schema and a streaming
  NDJSON sink. Lazy expressions derive `item_count` and `total_quantity` from
  each order's nested item list.
- DuckDB uses an explicit `BIGINT`/nested `STRUCT` schema, two threads, a
  512 MB buffer-manager limit, insertion-order preservation, and an
  output-adjacent temporary directory. Its SQL projection derives the same two
  metrics.
- Canonical integers are restricted to the signed 64-bit range used by both
  accelerated engines, including the summed quantity. Items must be non-empty
  and every quantity must be positive.
- All engines produce the separate Day 06 enriched golden shape. The DuckDB
  loader recalculates the metrics before storing them and upgrades a Day 05
  `accepted_orders` table with the new columns when necessary.
- Existing outputs are not overwritten; newly created partial outputs are
  deleted after failure. Input identity, size, and modification time are
  checked before and after accelerated processing.

## Correctness verification

Run on 12 October 2026:

```bash
.venv/bin/python -m pytest -q tests/test_processing_adapters.py
```

Result:

```text
62 passed
```

The same behavior matrix now covers all three engines: golden output,
deterministic field/order preservation, idempotence, strict ordering, empty
input, existing-output protection, malformed later input, missing fields,
exact JSON types, empty item lists, non-positive quantities, and signed-64-bit
integer/summed-quantity overflow.

The adapter, batch-loader, and coordinator tests pass together:

```text
81 passed
```

The complete repository suite passes:

```text
236 passed in 16.29s
```

The complete suite must be run outside this workspace's restricted execution
sandbox. The sandbox denies the cross-thread socket-pair write used by
`asyncio.call_soon_threadsafe()`, which Starlette's `TestClient` requires. A
minimal socket-pair test reproduced the restriction; it is not an application
or AnyIO failure.

## Ordered benchmark fixtures

Fixtures are generated incrementally in ascending `record_number` order:

```bash
.venv/bin/python -m scripts.generate_canonical_orders_ndjson \
  --records 100000 \
  --output /tmp/day06-enrichment-benchmark/input-100k.ndjson
```

Each engine runs in a fresh process through
`scripts.benchmark_processing_adapter`, which records elapsed time, process
peak RSS, output size, and SHA-256.

## Observed results on this machine

| Engine | Records | Seconds | Peak RSS MiB | Output bytes |
|---|---:|---:|---:|---:|
| Standard Python | 100,000 | 10.597 | 77.65 | 28,277,832 |
| Polars | 100,000 | 6.137 | 238.62 | 28,277,832 |
| DuckDB | 100,000 | 5.989 | 171.48 | 28,277,832 |

All three enriched outputs had checksum:

```text
b2dbf955ab6e641e6f0aa8e80368e067f3bceeeb2fb77c0d25dd3c309198542b
```

These are single development-machine runs, not production guarantees. They
used Python 3.11.15, Polars 1.44.2, DuckDB 1.5.6, and Linux
6.14.0-37-generic x86_64 with glibc 2.39.

## DuckDB engine resource profile

The benchmark can opt in to DuckDB's JSON profiler. Normal adapter calls do
not pay this profiling cost. The official execution metrics reported:

| Records | Peak buffer memory bytes | Peak temporary-directory bytes | Output SHA-256 matched |
|---:|---:|---:|:---:|
| 100,000 | 175,308,800 | 0 | yes |

For this ordered scan-and-project workload DuckDB did not spill to disk at the
measured size. That is evidence for this run only, not a promise that joins,
sorts, grouping, or the 2.2 GB input will avoid spilling. DuckDB's 512 MB
setting limits its buffer manager rather than total process RSS.

## Polars physical plan and memory conclusion

The current streaming graph is:

```text
multi-scan[ndjson] -> with-columns(item_count, total_quantity) -> ndjson-sink
```

It contains no reported potentially memory-intensive node and no in-memory
fallback. Polars completed the 100,000-record transformation faster than the
Python reference but used about 3.1 times its peak RSS. Plan inspection supports
the claim that no fallback was visible; process measurement does not support a
hard constant-memory or lower-memory claim. Standard Python remains the default
because it had the lowest measured peak RSS for this workload.

## Transaction and job-state foundation

- `DuckDBBatchLoader` commits bounded batches and records the batch checksum in
  the same transaction as accepted orders/items. It validates and stores the
  two derived metrics and supports the prior Day 05 table shape.
- A completed-import manifest records the full input checksum, total batches,
  and total records. Identical replays are skipped; modified, extended,
  truncated, or incomplete-shape replays raise `BatchReplayConflict`.
- A failed batch is rolled back; a corrected retry can then commit.
- If later coordinator publication fails after loading, a compensating DuckDB
  transaction removes that import's orders, items, ledgers, and manifest.
- `InMemoryImportJobRepository` enforces
  `uploaded -> queued -> processing -> completed|completed_with_errors|failed`
  and rejects illegal transitions without changing state.
- A second coordinator run for a terminal job is rejected, preventing duplicate
  scheduling from silently loading the same job again.
- Malformed input transitions to a safe `processing_failed` result and removes
  partial processed/summary files. Mixed valid and invalid records complete as
  `completed_with_errors` with reconciled counters.

## HTTP/background integration

- `POST /imports` stores the upload, records `uploaded -> queued`, and schedules
  processing with FastAPI `BackgroundTasks`.
- The background coordinator runs Day 5 cleaning/validation, the configured
  adapter, and transactional DuckDB batch loading for JSON arrays. The default
  request engine is Standard Python; `APP_PROCESSING_ENGINE` can select Polars
  or DuckDB. NDJSON on
  this route must already follow the canonical accepted-record schema and goes
  directly through the adapter and loader.
- `GET /imports/{import_id}` exposes state and reconciled counters.
- `GET /imports/{import_id}/summary` exposes compact failure counts.
- `GET /imports/{import_id}/errors?limit=...` reads at most 100 rejected records
  and does not load the complete rejection file into memory.
- Summary and error artifacts remain hidden until the job reaches
  `completed` or `completed_with_errors`.
- A process-local writer lock serializes DuckDB writes from background tasks.
  The existing raw-NDJSON validation route remains separate.

This course implementation deliberately uses an in-memory job repository and
in-process `BackgroundTasks`. A process restart loses job status and unfinished
tasks, even though already-published files and the DuckDB database remain on
disk. Multiple application processes still need an external durable queue and
a cross-process database-writer strategy; those are production follow-up work,
not properties claimed by this implementation.

## Remaining scale work

- Progress through larger fixtures only while measured RSS and free disk make
  the next step safe; the 2.2 GB file has not yet been run through all engines.
- Replace the in-process queue/repository if crash recovery or multi-process
  execution becomes a requirement.
- Batch retention is now bounded independently by row count, encoded bytes,
  per-record bytes, and item count. The defaults are 5,000 rows, 16 MiB per
  batch, 1 MiB per record, and 1,000 items per order.
