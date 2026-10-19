# Day 5 deterministic order pipeline evidence

## Completed flow

The standard-Python reference engine now performs these explicit stages:

1. Incrementally parse one top-level JSON array with `ijson`.
2. Deep-copy and deterministically clean each object with `clean_order`.
3. Reject cleaning failures with stable project codes and structured locations.
4. Validate clean values with one reused `TypeAdapter(OrderCreate)`.
5. Map Pydantic error locations to structured lists and project-owned codes.
6. Apply supported-currency business policy after schema validation.
7. Store record decisions and last-valid winners in temporary SQLite tables.
8. Walk the SQLite spool in input order and incrementally write accepted and
   rejected NDJSON temporary files.
9. Flush and replace each final output file, roll both back after an ordinary
   publication exception, return reconciled counts, and delete SQLite state.

Invalid records never update the winners table. Consequently, a later invalid
record does not displace an earlier valid record. When two valid records share
the same trimmed and uppercased `order_id`, the last valid record is accepted
and every earlier valid occurrence is rejected as `duplicate_order_id`.

## Run it

```bash
.venv/bin/python -m scripts.process_orders data/samples/orders.json \
  --accepted output/accepted.ndjson \
  --rejected output/rejected.ndjson
```

## Verification

```bash
.venv/bin/python -m pytest -q \
  tests/test_order_cleaning.py \
  tests/test_order_pipeline.py \
  tests/test_day5_order_import.py
```

Result: 80 tests passed.

The focused tests cover parameterized cleaning, half-up decimal rounding,
strict quantity input, Asia/Kolkata normalization, cleaning idempotence,
non-mutation, nested multi-error locations, omitted extras, import-wide
last-valid-wins duplicates across SQLite commits, invalid duplicates not
reserving an ID, business rejection, byte-identical reruns, failure-safe output
publication, reconciled counts, bounded failure samples, and frozen golden
files under `tests/golden/day5/`.

The non-`TestClient` repository suite passed 97 tests. The complete suite was
also attempted but timed out when entering the existing FastAPI `TestClient`
tests. No Day 5 test failed.

## Audit contract

Each accepted and rejected line carries the one-based source `record_number`.
Rejected lines also carry a stable record-level `error_code` and every issue's
stable code, structured location, and message. Summary totals reconcile and
failure samples are bounded. The source JSON remains the authoritative raw
record; production callers must retain it with the SHA-256 checksum already
produced by the upload-storage stage. The output pair uses temporary files and
restores both previous files after a normal publication exception. Like any
two independent filesystem paths, it cannot promise pair-atomic recovery from
an abrupt process or machine failure between replacements.

## Golden reference

The standard-Python engine's canonical input, accepted NDJSON, rejected NDJSON,
and summary are frozen in `tests/golden/day5/`. A later Polars, DuckDB, or other
engine must reproduce those files exactly.

## Official references

- Pydantic models and data conversion:
  https://docs.pydantic.dev/latest/concepts/models/
- Pydantic validators and validator modes:
  https://docs.pydantic.dev/latest/concepts/validators/
- Pydantic validation error details:
  https://docs.pydantic.dev/latest/errors/errors/
- Pydantic TypeAdapter:
  https://docs.pydantic.dev/latest/concepts/type_adapter/
- Python `decimal`, exact conversion, and rounding:
  https://docs.python.org/3/library/decimal.html
- Python `sqlite3`, transactions, and disk-backed storage:
  https://docs.python.org/3/library/sqlite3.html
