# Failure and resource evidence

## Automated failure evidence

- A mid-batch constraint failure rolls back accepted rows and the idempotency
  ledger together; corrected input can then be retried.
- An identical completed replay skips committed batches and inserts no rows.
- Changed, extended, truncated, and incomplete-shape replays fail closed.
- Input/contract failures are `PermanentBatchLoadError`; DuckDB connection,
  I/O, interruption, memory, serialization, and transaction failures are
  `RetryableBatchLoadError`. Classification does not imply an in-process retry.
- Adapter failures remove their partial output; coordinator failures compensate
  loaded rows and remove unpublished artifacts.
- Empty item lists, non-positive quantities, and a derived quantity total over
  the signed 64-bit range fail consistently in all three adapters.
- The loader recalculates `item_count` and `total_quantity` and rejects a
  processed line whose supplied metrics do not match its nested items.
- Unknown IDs, early summary/error access, illegal transitions, malformed
  input, duplicate scheduling, and lost in-memory restart state are tested.

## Bounded loading

`DuckDBBatchLoader` reads one line through a bounded binary `readline`, rejects
records over the record-byte limit before retaining an arbitrary line, and
commits when either the record-count or encoded-byte limit is reached. It also
rejects orders over the configured item count. Settings validation guarantees
that the batch-byte budget can hold one maximum-size record plus a line ending.
All three adapters use the same bounded-line pre-validation, so a direct
canonical NDJSON import cannot allocate an unbounded line before loading.

## Polars observation

The inspected physical graph for the current ordered enrichment workload is:

```text
multi-scan[ndjson] -> with-columns(item_count, total_quantity) -> ndjson-sink
```

No potentially memory-intensive or in-memory fallback node was reported. In a
fresh-process 100,000-record development run, Polars used 238.62 MiB peak RSS
and finished in 6.137 seconds; Standard Python used 77.65 MiB and finished in
10.597 seconds. Polars was faster for this workload, but the evidence does not
support a lower-memory or constant-memory claim.

## DuckDB observation

DuckDB runs with two threads, a 512 MB buffer-manager limit, insertion-order
preservation, and a dedicated output-adjacent temporary directory. Profiling
reported 175,308,800 peak buffer-memory bytes and zero temporary-directory
bytes for the measured 100,000-record enrichment run. The plan did not contain
a blocking sort, join, or aggregation. This is observed behavior, not a
guarantee that other plans will not spill.

A simulated DuckDB `IOException` representing unavailable/exhausted temporary
storage is classified as retryable. The process RSS can exceed DuckDB's memory
setting because that setting is not a hard operating-system process cap.

## Concurrency and restart evidence

`tests/test_import_scheduling.py` starts two background imports together and
observes a maximum of one active write section. A process-local lock therefore
serializes writers inside one application process.

That lock is not shared by multiple Uvicorn worker processes. The local course
deployment must use one writer process; a multi-process deployment requires
external coordination or a database designed for the chosen concurrent-write
model.

`tests/test_import_jobs.py` recreates the in-memory repository and proves that
the former job cannot be retrieved. Uploaded files and committed DuckDB data
may remain on disk, but queued/running state and an in-process task do not
survive process termination. This is an infrastructure limitation, not a
record-level failure.
