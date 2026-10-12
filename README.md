# Vendor Order Import API

A FastAPI service for validating, cleaning, processing, and loading vendor order
files without loading an entire upload into memory. It accepts standard JSON
arrays and canonical NDJSON, records import progress, publishes bounded error
summaries, and stores accepted orders in DuckDB.

## Features

- Streamed uploads with configurable request and record-size limits
- JSON-array parsing without `json.load()` on the complete file
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

## Processing flow

```text
upload
  -> bounded staging file + SHA-256 checksum
  -> clean and validate records
  -> accepted.ndjson / rejected.ndjson
  -> selected processing adapter
  -> processed.ndjson
  -> bounded DuckDB transactions
  -> summary.json
```

JSON arrays pass through the complete cleaning and validation pipeline. NDJSON
submitted to `/imports` must already use the canonical accepted-record shape.
Published import artifacts are stored below `data/output/<import_id>/` by
default, and accepted rows are loaded into `data/output/imports.duckdb`.

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
| `POST` | `/imports` | Queue a JSON or canonical-NDJSON import |
| `GET` | `/imports/{import_id}` | Read import status and counts |
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

Run the JSON-array cleaning and validation pipeline without starting the API:

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
