# Vendor Order Import API

A FastAPI service for validating, cleaning, processing, and loading vendor order
files without loading an entire upload into memory. It accepts standard JSON
arrays, the project's raw CSV contract, and canonical NDJSON. JSON and NDJSON
imports end in DuckDB; CSV imports publish a final CSV file without database
insertion.

## Features

- Streamed uploads with configurable request and record-size limits
- JSON-array parsing without `json.load()` on the complete file
- Bounded CSV parsing with an exact header contract and JSON-encoded `items`
- Line-by-line NDJSON validation
- Order cleaning, schema validation, business validation, and duplicate handling
- Selectable processing engines: standard Python, Polars, or DuckDB
- Row- and byte-bounded transactional loading into DuckDB
- Idempotent batch replay protection
- Import status, summary, and rejected-record endpoints
- Liveness and readiness probes
- Request IDs and request-duration response headers

## Requirements

- Python 3.11 or newer
- A writable directory for uploaded and processed files

## Quick start

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
uvicorn app.main:app --reload
```

The API is available at `http://127.0.0.1:8000`. Interactive documentation is
available at:

- Swagger UI: `http://127.0.0.1:8000/docs`
- OpenAPI schema: `http://127.0.0.1:8000/openapi.json`

Check service health:

```bash
curl http://127.0.0.1:8000/health/live
curl http://127.0.0.1:8000/health/ready
```

## Import an order file

Upload a standard JSON array:

```bash
curl -X POST http://127.0.0.1:8000/imports \
  -F 'file=@orders.json;type=application/json'
```

Or upload canonical NDJSON:

```bash
curl -X POST http://127.0.0.1:8000/imports \
  -F 'file=@orders.ndjson;type=application/x-ndjson'
```

Or upload raw CSV:

```bash
curl -X POST http://127.0.0.1:8000/imports \
  -F 'file=@orders.csv;type=text/csv'
```

CSV must use this exact header order:

```text
order_id,customer_email,amount,currency,ordered_at,status,items
```

The `items` cell is compact JSON, `\N` means JSON null, and an empty cell stays
an empty string for the cleaning and validation rules to evaluate.

The response has an `import_id` and starts in the `queued` state:

```json
{
  "import_id": "9a83f7d1f95c44aeb15a976e5262d7d1",
  "checksum": "<sha256>",
  "stored_filename": "9a83f7d1f95c44aeb15a976e5262d7d1.json",
  "bytes_received": 12345,
  "state": "queued"
}
```

Use the returned ID to inspect the import:

```bash
curl http://127.0.0.1:8000/imports/IMPORT_ID
curl http://127.0.0.1:8000/imports/IMPORT_ID/summary
curl 'http://127.0.0.1:8000/imports/IMPORT_ID/errors?limit=20'
```

Terminal states are `completed`, `completed_with_errors`, and `failed`.

### Run the import as explicit stages

Set `auto_start=false` when the upload should be stored without immediately
running the complete pipeline:

```bash
curl -X POST 'http://127.0.0.1:8000/imports?auto_start=false' \
  -F 'file=@orders.json;type=application/json'

curl -X POST http://127.0.0.1:8000/imports/IMPORT_ID/clean
curl -X POST 'http://127.0.0.1:8000/imports/IMPORT_ID/process?engine=duckdb'
curl -X POST http://127.0.0.1:8000/imports/IMPORT_ID/load
```

Each stage returns `202 Accepted`; use `GET /imports/IMPORT_ID` to observe its
result. A failed staged import can resume from its failed stage without
repeating successful earlier stages:

```bash
curl -X POST http://127.0.0.1:8000/imports/IMPORT_ID/retry
```

Published files can be downloaded using the allow-listed artifact endpoint:

```bash
curl -O http://127.0.0.1:8000/imports/IMPORT_ID/artifacts/accepted
curl -O http://127.0.0.1:8000/imports/IMPORT_ID/artifacts/rejected
curl -O http://127.0.0.1:8000/imports/IMPORT_ID/artifacts/processed
curl -O http://127.0.0.1:8000/imports/IMPORT_ID/artifacts/processed_csv
curl -O http://127.0.0.1:8000/imports/IMPORT_ID/artifacts/summary
```

## Processing flow

```text
upload -> bounded staging file + SHA-256 checksum
       -> clean and validate records
       -> accepted.ndjson / rejected.ndjson
       -> selected processing adapter
          |-> JSON/NDJSON: processed.ndjson -> bounded DuckDB transactions
          `-> CSV: atomic processed.csv publication (no DuckDB insertion)
       -> summary.json
```

JSON arrays and raw CSV pass through the complete cleaning and validation
pipeline. NDJSON submitted to `/imports` must already use the canonical
accepted-record shape. Published import artifacts are stored below
`data/output/<import_id>/` by default. JSON/NDJSON accepted rows are loaded into
`data/output/imports.duckdb`; CSV instead ends at
`data/output/<import_id>/processed.csv`.

## API endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health/live` | Process liveness |
| `GET` | `/health/ready` | Storage readiness |
| `POST` | `/orders` | Create an in-memory order |
| `GET` | `/orders` | List orders, optionally filtered by `status` |
| `GET` | `/orders/{order_id}` | Retrieve an order |
| `POST` | `/orders/validate` | Validate and normalize one order |
| `POST` | `/orders/checkout` | Validate a checkout payload |
| `POST` | `/imports` | Queue a JSON, CSV, or canonical-NDJSON import |
| `POST` | `/imports?auto_start=false` | Store an import for explicit staged processing |
| `POST` | `/imports/{import_id}/clean` | Clean and validate the stored source |
| `POST` | `/imports/{import_id}/process?engine=...` | Run Python, Polars, or DuckDB transformation |
| `POST` | `/imports/{import_id}/load` | Load processed records into DuckDB in bounded batches |
| `POST` | `/imports/{import_id}/retry` | Retry only the failed staged operation |
| `GET` | `/imports/{import_id}` | Read import status and counts |
| `GET` | `/imports/{import_id}/artifacts/{artifact}` | Download a published import artifact |
| `GET` | `/imports/{import_id}/summary` | Read a completed import summary |
| `GET` | `/imports/{import_id}/errors` | Read bounded rejected-record details |
| `POST` | `/imports/json/validate` | Validate a JSON array without loading it |
| `POST` | `/imports/ndjson/validate` | Validate NDJSON one bounded line at a time |

## Configuration

Settings can be placed in `.env` or exported as environment variables. Every
variable uses the `APP_` prefix.

| Variable | Default | Description |
| --- | --- | --- |
| `APP_UPLOAD_DIRECTORY` | `data/uploads` | Staged uploads |
| `APP_OUTPUT_DIRECTORY` | `data/output` | Import artifacts and DuckDB database |
| `APP_MAXIMUM_UPLOAD_BYTES` | `104857600` | Maximum upload size (100 MiB) |
| `APP_UPLOAD_CHUNK_BYTES` | `1048576` | Upload streaming chunk size |
| `APP_MAXIMUM_RECORD_BYTES` | `1048576` | Maximum encoded record size |
| `APP_IMPORT_BATCH_SIZE` | `5000` | Maximum records per database batch |
| `APP_IMPORT_BATCH_BYTES` | `16777216` | Maximum bytes per database batch |
| `APP_MAXIMUM_ITEMS_PER_ORDER` | `1000` | Maximum nested items per order |
| `APP_PROCESSING_ENGINE` | `python` | `python`, `polars`, or `duckdb` |
| `APP_SUPPORTED_CURRENCIES` | `EUR`, `INR`, `USD` | Accepted currency codes |

`APP_IMPORT_BATCH_BYTES` must be at least
`APP_MAXIMUM_RECORD_BYTES + 2` to allow a record and its line ending.

Example `.env`:

```dotenv
APP_ENVIRONMENT=development
APP_PROCESSING_ENGINE=python
APP_MAXIMUM_UPLOAD_BYTES=104857600
APP_SUPPORTED_CURRENCIES=["EUR","INR","USD"]
```

## Command-line processing

Run the JSON-array or CSV cleaning and validation pipeline without starting the API:

```bash
python scripts/process_orders.py orders.json \
  --accepted data/output/accepted.ndjson \
  --rejected data/output/rejected.ndjson
```

Generate a deterministic sample file:

```bash
python scripts/generate_orders_json.py \
  --records 10000 \
  --output data/samples/orders.json
```

Generate the equivalent deterministic CSV fixture:

```bash
python -m scripts.generate_orders_csv \
  --records 10000 \
  --output data/samples/orders.csv
```

### End-to-end JSON versus CSV benchmark

Run each workflow in a separate Python process so its peak RSS measurement is
independent. The JSON run ends in DuckDB, while the CSV run ends in
`processed.csv`. A bounded local staging copy with SHA-256 represents the
upload-storage stage; network transfer and multipart decoding are excluded:

```bash
mkdir -p data/benchmarks/e2e

python -m scripts.benchmark_end_to_end \
  --format json --records 100000 --strategy duckdb \
  --output-dir data/benchmarks/e2e/json-run-1 \
  > data/benchmarks/e2e/json-run-1.json

python -m scripts.benchmark_end_to_end \
  --format csv --records 100000 --strategy duckdb \
  --output-dir data/benchmarks/e2e/csv-run-1 \
  > data/benchmarks/e2e/csv-run-1.json
```

Repeat with new output directories for at least three runs per format. Compare
`stage_seconds`, `total_seconds`, `records_per_second`, `peak_rss_mib`, and
artifact sizes. This intentionally compares the two different end-to-end
workflows; it is not a parser-only speed comparison.

Or run three alternating fresh-process measurements per format and generate an
aggregate comparison in one command:

```bash
python -m scripts.compare_end_to_end \
  --records 100000 --runs 3 --strategy duckdb \
  --output-dir data/benchmarks/e2e/comparison-1 \
  > data/benchmarks/e2e/comparison-1.json
```

The output directory must be new or empty. Per-run metrics remain inside it;
the redirected JSON contains means, medians, ranges, stage means, throughput,
peak RSS, artifact sizes, and the faster workflow by mean total time.

## Tests

Run the complete test suite:

```bash
pytest -q
```

Run a focused module while developing:

```bash
pytest -q tests/test_imports.py
```

## Current operational limits

- Import-job state and the basic `/orders` store are held in memory and are lost
  when the process restarts.
- Import work uses FastAPI background tasks rather than a durable external queue.
- A process-local lock serializes database-writing imports. Multiple application
  processes do not share that lock.
- Uploaded files and generated artifacts use local storage.
- The default 100 MiB upload limit is intentionally smaller than very large
  benchmark fixtures; use the command-line pipeline for those files.

For production deployment, replace in-memory job state, local background tasks,
and local files with durable shared infrastructure appropriate to the target
environment.

## Repository

GitHub: <https://github.com/KovanLabs/FastAPI>
