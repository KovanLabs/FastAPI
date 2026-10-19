# Day 2 implementation evidence

## Implemented structure

- `app/main.py` owns application assembly, lifespan, middleware, and the application-exception handler.
- `app/api/orders.py` owns HTTP routes.
- `app/api/dependencies.py` assembles settings and `OrderValidationService` at the FastAPI boundary.
- `app/services/order_validation.py` contains an ordinary service that can be tested without FastAPI.
- `app/core/config.py` owns environment-backed settings.
- `app/core/exceptions.py` owns stable application exception types.
- Tests prove direct service use, dependency replacement and cleanup, request correlation, and complete public error responses.

## Stable application error example

```json
{
  "error": {
    "code": "unsupported_currency",
    "message": "Currency JPY is not supported",
    "details": {"currency": "JPY"}
  },
  "request_id": "req-domain-test"
}
```

## Execution experiment

Two concurrent requests were sent in-process to each temporary learning route. Both routes waited for 0.15 seconds; one used `time.sleep(0.15)` inside `async def`, and the other used `await asyncio.sleep(0.15)`.

Measured on 25 September 2026:

```text
blocking_two_requests_seconds=0.3047
nonblocking_two_requests_seconds=0.1537
```

The blocking calls ran almost serially because `time.sleep()` occupied the event-loop thread. The awaited sleeps yielded control, so their waiting periods overlapped. No artificial sleep endpoint was retained in the project.

## Verification limitation

The Day 1–2 suite passes 20 tests. The whole repository suite currently stalls in the pre-existing raw-request-stream `/imports` test. That import implementation is outside the completed Day 1–2 scope and must be replaced through the Day 3 upload contract rather than silently repaired early.

The learner-authored journal and quiz result are intentionally not fabricated here; they remain knowledge exit-gate evidence.
