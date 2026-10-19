# Day 3 implementation evidence

## Upload contract

- `POST /imports` accepts one multipart file field named `file`.
- The file part must declare `application/x-ndjson` or `application/json`.
- The service rejects empty uploads and counts actual file bytes against `APP_MAXIMUM_UPLOAD_BYTES`.
- Reads use `APP_UPLOAD_CHUNK_BYTES`; the client filename is not used to build a path.
- The service calculates SHA-256 while copying to a generated temporary path, then atomically renames the complete file.
- Success returns `202 Accepted` with the import ID, checksum, safe stored filename, byte count, and initial `queued` state. Parsing is not part of this day.
- Size, media-type, empty-file, storage, and interruption failures do not leave a partial file.

## Verification

The Day 3 suite exercises multipart NDJSON and JSON uploads, wrong media type, empty input, one byte, exact byte limit, over-limit cleanup, generated filenames for unsafe and repeated client names, configured chunk sizes, read interruption, and injected disk-write failure.

Command run with an isolated dependency environment on 28 September 2026:

```text
uv run --no-project --with-requirements requirements.txt \
  pytest -q tests/test_imports.py
```

Result:

```text
12 passed in 0.63s
```

Starlette is pinned alongside the recorded HTTPX version because the repository environment also contains `httpx2`, which was selected by Starlette 1.7 and caused the test client to stall. The selected Starlette 0.48.0 test client completed the upload tests normally.

The bounded-read test verifies the application requests at most the configured chunk size from the upload object. The separate memory experiment below compares whole-file and bounded reads; end-to-end process RSS and upload-throughput measurements remain for the later large-input test work.

## Memory experiment

Generated sparse files of 16 MiB and 64 MiB in a temporary directory. With `tracemalloc` active, compared one `read()` with a loop that reads 1 MiB at a time and discards each chunk. The measurement ran under the repository's Python 3.11 virtual environment.

```text
16 MiB: whole_read_peak=16.00 MiB; bounded_read_peak=2.01 MiB
64 MiB: whole_read_peak=64.00 MiB; bounded_read_peak=2.00 MiB
```

This measures Python allocations for file reads. It excludes multipart parsing and framework spooling, process RSS, and filesystem cache. The results show the expected scaling difference; they are not a production throughput benchmark.

The complete repository suite also passed with the pinned dependencies:

```text
uv run --no-project --with-requirements requirements.txt pytest -q
32 passed in 0.97s
```
