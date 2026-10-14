# Engine equivalence contract

## Boundary

All adapters consume only Day 05 canonical accepted NDJSON. Parsing malformed
raw JSON, cleaning, schema validation, business validation, duplicate handling,
and rejected-record reporting remain owned by the record pipeline. An engine
adapter must not silently replace those rules with `ignore_errors` behavior.

Each input line has this shape:

```json
{"record_number":1,"order":{"order_id":"ORD-1","customer_email":"user@example.com","amount":"10.00","currency":"INR","ordered_at":"2026-10-05T04:30:00+00:00","status":"pending","items":[{"sku":"SKU-1","quantity":1}]}}
```

Each output line retains that canonical record and adds two top-level derived
fields:

```json
{"record_number":1,"order":{"order_id":"ORD-1","customer_email":"user@example.com","amount":"10.00","currency":"INR","ordered_at":"2026-10-05T04:30:00+00:00","status":"pending","items":[{"sku":"SKU-1","quantity":1}]},"item_count":1,"total_quantity":1}
```

## Required semantics

Every adapter must:

1. Require the explicit canonical fields and exact JSON scalar types.
2. Restrict integers to the signed 64-bit range shared by Polars and DuckDB.
3. Require strictly increasing `record_number` values.
4. Require at least one item and a positive integer quantity for every item.
5. Derive `item_count` from the number of items and `total_quantity` from the
   sum of item quantities; the sum must fit in a signed 64-bit integer.
6. Preserve record order, item order, decimal strings, timestamps, and stable
   output field order.
7. Produce byte-identical enriched NDJSON matching
   `tests/golden/day6/processed.ndjson` for the declared fixtures.
8. Refuse to overwrite an existing destination.
9. Remove a newly created partial destination after any failure.
10. Detect source replacement or modification during accelerated processing.

`StandardPythonAdapter` is the correctness reference and calculates both
metrics record by record. `PolarsAdapter` uses an explicit nested schema, lazy
`scan_ndjson`, expression-based list calculations, and a streaming NDJSON
sink. `DuckDBAdapter` uses an explicit nested SQL type, set-based list
projection, a dedicated temporary directory, and configured memory/thread
settings. `DuckDBBatchLoader` independently verifies the metrics before
storing them, so a forged or inconsistent processed record fails closed.

## Verification

`tests/test_processing_adapters.py` parameterizes correctness and failure cases
over all three adapters. It checks golden equality, deterministic output,
idempotence, empty input, strict ordering, malformed later input, missing
fields, exact types, empty items, non-positive quantities, integer and summed
quantity overflow, and destination protection.

Performance results are considered only after these equivalence tests pass.
The benchmark harness runs each engine in a fresh process and records elapsed
time, peak RSS, output bytes, SHA-256, versions, and available engine evidence.
