# Import state and API contracts

## State machine

```text
uploaded -> queued -> processing -> completed
                              \-> completed_with_errors
                              \-> failed
```

Only the transitions shown above are legal. Terminal states cannot transition
again. The repository stores created/updated timestamps, bounded counters, and
safe failure fields. Illegal transitions raise `IllegalImportTransition`
without modifying the current job.

A rejected source record is not a failed job. Mixed valid and invalid records
finish as `completed_with_errors`; an engine, database, invariant, or artifact
publication failure finishes as `failed` with a stable public failure code.

## HTTP contract

| Route | Normal result | Important alternate result |
|---|---|---|
| `POST /imports` | `202`, persisted upload metadata and `queued` | upload/media-type errors |
| `GET /imports/{id}` | `200`, state, counters, timestamps, safe failure | `404` unknown import |
| `GET /imports/{id}/summary` | `200` after a successful terminal state | `409` while not ready |
| `GET /imports/{id}/errors?limit=N` | `200`, bounded rejection view | `409` while not ready |

`202 Accepted` means the upload was persisted, the job record was created, and
in-process work was scheduled. It does not promise successful completion,
exactly-once execution, retry, or survival after process termination.

The background function receives stable paths and identifiers and constructs
its own adapter and DuckDB loader. It does not retain the request's open upload
handle or a request-scoped database connection.
