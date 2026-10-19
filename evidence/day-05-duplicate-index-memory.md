# Day 5 duplicate-index memory evidence

Measured on 2026-10-06 with CPython 3.11.15 using:

```bash
.venv/bin/python scripts/measure_duplicate_index.py --unique-ids 100000
```

Observed `tracemalloc` result:

```json
{
  "unique_ids": 100000,
  "current_bytes": 10694520,
  "peak_bytes": 11403699,
  "peak_bytes_per_id": 114.03699,
  "projected_peak_bytes_at_10m": 1140369900
}
```

The projected Python-heap peak for 10 million IDs is about 1.14 GB. This is an
estimate based on the measured fixture and excludes input batches, validated
models, output buffers, allocator overhead not observed by `tracemalloc`, and
the rest of the application. Day 5 therefore uses a temporary SQLite database
for the import-wide last-valid-wins index and record spool instead of retaining
all IDs in a Python set.

The SQLite database is deleted after both final NDJSON files have been flushed
and published. `OrderImportSummary.sqlite_working_store_bytes` records the
logical page footprint of the complete SQLite spool for each completed import.
