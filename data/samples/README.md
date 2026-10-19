# Ten-million-order JSON fixture

Generate the ignored large artifact with:

```bash
.venv/bin/python scripts/generate_orders_json.py
```

This produces `data/samples/orders_10m.json`: a standard JSON document whose
top-level value is an array of 10,000,000 order objects. The generated artifact
is intentionally git-ignored; the deterministic generator is kept in source.

Each object follows the order shape in `app/schemas/orders.py` and includes a
nested `items` array. Most rows contain ordinary values. Ten rows in each
10,000-order block each introduce one syntactically valid data-quality
anomaly: malformed, blank, or null email; non-numeric or negative amount;
whitespace-padded currency; inconsistent status casing; blank SKU; zero
quantity; or an ID duplicated from the previous order. These are semantic
cleaning cases for a future Polars stage, not broken
JSON syntax; broken syntax inside a top-level array would invalidate the whole
document and would not be a useful cleaning fixture.

The file is compact and streamed to disk during generation. Do not load all of
it with `json.load()` or upload it through the current Swagger endpoint (the
configured request limit is 100 MiB). Use it for local incremental-parser and
memory experiments. For future Polars work, feed bounded batches from the JSON
array parser into Polars; Polars' lazy `scan_ndjson` is specifically for
newline-delimited JSON, while this fixture preserves the standard JSON array
format supplied by the project.
