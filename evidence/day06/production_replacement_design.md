# Production replacement design

The course implementation intentionally uses FastAPI `BackgroundTasks`, an
in-memory job repository, and a process-local DuckDB writer lock. Production
durability requires the following replacement boundary.

```text
POST /imports
  -> durable object storage
  -> durable job row + outbox message (one transaction)
  -> queue
  -> independently deployed worker
  -> staged artifacts / production database
  -> durable terminal job state
```

## Ownership and durability

- Store uploads in durable object storage and address them by immutable object
  key plus SHA-256.
- Persist job state, counters, timestamps, engine selection, attempts, and safe
  failure codes in a transactional database.
- Publish work through an outbox/queue so a process crash cannot leave a
  durable job without a durable message.
- A worker claims a job with an expiring lease and heartbeat. Only the current
  lease owner may publish progress or terminal state.
- The API reports durable state; it does not infer completion from local files.

## Idempotency and retry

- Use `import_id + stage + batch_number` as the idempotency key and commit data
  plus the key atomically.
- Retry only classified transient failures with bounded exponential backoff and
  jitter. Permanent schema/contract failures go directly to a terminal state.
- Reclaim an expired lease after worker termination. A retry rechecks batch
  ledgers and output publication markers before doing work.
- Send repeatedly failing jobs to a dead-letter/manual-review path with a
  bounded attempt count.

## Database writer model

For a single embedded DuckDB file, route all writes to one owned writer service
and allow separate read-only consumers as supported. If horizontally scalable
concurrent writes are required, replace the serving store with a client/server
database and retain DuckDB only for isolated analytical staging where useful.

## Atomic artifact publication

Write every generated artifact under an attempt-specific temporary key. Record
its checksum and size, then publish a manifest or atomically promote the object.
Readers use only the published manifest. Cleanup safely removes abandoned
attempt artifacts after their lease expires.

This design supplies the properties missing from `BackgroundTasks`: durable
messages, persisted ownership, crash recovery, controlled retries,
cross-process coordination, and operational visibility.
