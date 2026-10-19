# Day 1 implementation evidence

## Implemented contract

- `POST /orders/validate` accepts a canonical order request and returns the declared `OrderResponse`.
- The request model covers the order ID, customer email, positive decimal amount, three-character currency, timezone-capable datetime, status, and at least one nested item.
- Path, query, header, and body locations are exercised by API tests.
- Request-validation failures remain FastAPI/Pydantic 422 responses; unsupported currency is translated through the application's stable error envelope.
- A dependency-override test proves that the response model removes an internal-only field.
- `/openapi.json` is checked for the health and validation routes.

## Verification

Command run on 25 September 2026:

```text
UV_CACHE_DIR=/tmp/fastapi_learning_uv_cache uv run \
  --with fastapi==0.141.1 \
  --with anyio==4.9.0 \
  --with pydantic-settings \
  --with email-validator \
  --with httpx==0.28.1 \
  --with pytest==8.4.2 \
  pytest -q tests/test_orders.py tests/test_order_validation.py
```

Result:

```text
....................                                                     [100%]
20 passed in 1.10s
```

The learner-authored journal and quiz result are intentionally not fabricated here; they remain knowledge exit-gate evidence.
