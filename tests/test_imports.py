import hashlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.core.config import Settings, get_settings
from app.main import app

@pytest.fixture(autouse=True)
def override_test_settings(tmp_path):
    settings = Settings(
        app_name="Vendor Order Import API",
        environment="test",
        upload_directory=tmp_path / "uploads",
        output_directory=tmp_path / "output",
        maximum_upload_bytes=100 * 1024 * 1024,
        upload_chunk_bytes=1024 * 1024,
        maximum_record_bytes=1024 * 1024,
        progress_log_interval=10_000,
        otel_exporter_endpoint="http://localhost:4317",
        telemetry_enabled=False,
    )
    app.dependency_overrides[get_settings] = lambda: settings
    settings.upload_directory.mkdir(parents=True, exist_ok=True)
    settings.output_directory.mkdir(parents=True, exist_ok=True)
    yield settings
    app.dependency_overrides.clear()


@pytest.fixture
def client(override_test_settings):
    with TestClient(app) as test_client:
        yield test_client


NDJSON_BODY = "\n".join(
    json.dumps(record)
    for record in [
        {"order_id": "ORD-1001", "amount": "1299.50", "currency": "INR"},
        {"order_id": "ORD-1002", "amount": "445.00", "currency": "USD"},
        {},
    ]
) + "\n"


def test_import_stores_multipart_upload_and_returns_accepted_metadata(override_test_settings, client) -> None:
    response = client.post(
        "/imports",
        files={"file": ("orders.ndjson", NDJSON_BODY.encode(), "application/x-ndjson")},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["state"] == "queued"
    assert body["import_id"]
    assert body["checksum"] == hashlib.sha256(NDJSON_BODY.encode()).hexdigest()
    assert body["bytes_received"] == len(NDJSON_BODY.encode())
    assert body["stored_filename"] == f"{body['import_id']}.ndjson"

    stored = override_test_settings.upload_directory / body["stored_filename"]
    assert stored.exists()
    assert stored.read_bytes() == NDJSON_BODY.encode()
    assert list(override_test_settings.upload_directory.glob("*.part")) == []


def test_import_rejects_wrong_content_type(client) -> None:
    response = client.post(
        "/imports",
        files={"file": ("orders.csv", b"id,amount\n1,2", "text/csv")},
    )
    assert response.status_code == 415


def test_ndjson_validate_endpoint_counts_mixed_records_and_recovers(
    override_test_settings, client
) -> None:
    body = (
        b'{"order_id":"ORD-1"}\n'
        b"   \n"
        b'{"broken"\n'
        b"\xff\n"
        b"[]\n"
        b'{"order_id":"ORD-2"}\n'
    )

    response = client.post(
        "/imports/ndjson/validate",
        files={"file": ("mixed.ndjson", body, "application/x-ndjson")},
    )

    assert response.status_code == 200
    result = response.json()
    assert result["state"] == "completed"
    assert result["valid_records"] == 2
    assert result["skipped_blank"] == 1
    assert result["failure_count"] == 3
    assert result["failures_by_code"] == {
        "invalid_json": 1,
        "invalid_utf8": 1,
        "not_object": 1,
    }
    assert result["failure_samples"] == [
        {"line_number": 3, "error_code": "invalid_json"},
        {"line_number": 4, "error_code": "invalid_utf8"},
        {"line_number": 5, "error_code": "not_object"},
    ]
    assert result["bytes_received"] == len(body)
    assert (override_test_settings.upload_directory / result["stored_filename"]).read_bytes() == body


def test_ndjson_validate_endpoint_rejects_json_array_upload(client) -> None:
    response = client.post(
        "/imports/ndjson/validate",
        files={"file": ("orders.json", b"[{}, {}]", "application/json")},
    )

    assert response.status_code == 415
    assert response.json()["error"]["code"] == "unsupported_media_type"


def test_ndjson_validate_endpoint_accepts_generic_browser_file_type(client) -> None:
    response = client.post(
        "/imports/ndjson/validate",
        files={"file": ("orders.ndjson", b"{}\n", "application/octet-stream")},
    )

    assert response.status_code == 200
    assert response.json()["valid_records"] == 1


def test_ndjson_validate_endpoint_caps_failure_samples(override_test_settings, client) -> None:
    body = b'{"broken"}\n' * 25
    response = client.post(
        "/imports/ndjson/validate",
        files={"file": ("many-errors.ndjson", body, "application/x-ndjson")},
    )

    assert response.status_code == 200
    result = response.json()
    assert result["failure_count"] == 25
    assert result["failures_by_code"] == {"invalid_json": 25}
    assert len(result["failure_samples"]) == 20


def test_json_validate_endpoint_streams_array_and_reports_non_object(client) -> None:
    body = b'[{"order_id":"ORD-1"},3,{"order_id":"ORD-2"}]'

    response = client.post(
        "/imports/json/validate",
        files={"file": ("orders.json", body, "application/json")},
    )

    assert response.status_code == 200
    result = response.json()
    assert result["state"] == "completed"
    assert result["valid_records"] == 2
    assert result["failure_count"] == 1
    assert result["failures_by_code"] == {"not_object": 1}
    assert result["failure_samples"] == [
        {"record_number": 2, "error_code": "not_object"}
    ]


def test_json_validate_endpoint_rejects_malformed_array(client) -> None:
    response = client.post(
        "/imports/json/validate",
        files={"file": ("orders.json", b'[{"id":1}', "application/json")},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_json"


def test_import_rejects_missing_file_field(client) -> None:
    response = client.post("/imports", data={})

    assert response.status_code == 422
    assert any(
        error["loc"][-1] == "file"
        for error in response.json()["detail"]
    )


def test_import_rejects_oversized_upload(override_test_settings, client) -> None:
    override_test_settings.maximum_upload_bytes = 16
    response = client.post(
        "/imports",
        files={"file": ("orders.ndjson", NDJSON_BODY.encode(), "application/x-ndjson")},
    )
    assert response.status_code == 413
    assert list(override_test_settings.upload_directory.iterdir()) == []


def test_import_service_cleans_up_partial_file_on_read_failure(override_test_settings) -> None:
    import asyncio

    from app.core.import_service import store_upload

    class FailingUpload:
        def __init__(self):
            self._sent = False

        async def read(self, size: int = -1):
            if not self._sent:
                self._sent = True
                return b'{"order_id": "ORD-1"}\n'
            raise RuntimeError("simulated mid-upload failure")

    with pytest.raises(RuntimeError, match="simulated mid-upload failure"):
        asyncio.run(
            store_upload(FailingUpload(), override_test_settings, suffix=".ndjson")
        )

    assert list(override_test_settings.upload_directory.iterdir()) == []


def test_import_service_reads_only_configured_chunk_sizes(override_test_settings) -> None:
    import asyncio

    from app.core.import_service import store_upload

    class RecordingUpload:
        def __init__(self, body: bytes):
            self.body = body
            self.read_sizes: list[int] = []

        async def read(self, size: int = -1) -> bytes:
            self.read_sizes.append(size)
            chunk, self.body = self.body[:size], self.body[size:]
            return chunk

    override_test_settings.upload_chunk_bytes = 3
    upload = RecordingUpload(b"1234567")
    result = asyncio.run(
        store_upload(upload, override_test_settings, suffix=".ndjson")
    )

    assert upload.read_sizes == [3, 3, 3, 3]
    assert result.bytes_received == 7
    assert (override_test_settings.upload_directory / result.stored_filename).read_bytes() == b"1234567"


def test_import_service_cleans_partial_file_after_disk_write_failure(
    override_test_settings, monkeypatch
) -> None:
    import asyncio

    from app.core.exceptions import UploadStorageError
    from app.core.import_service import store_upload

    real_open = Path.open

    class FailingWriter:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return self.handle.__exit__(exc_type, exc_value, traceback)

        def write(self, data: bytes) -> int:
            self.handle.write(data[:1])
            raise OSError("simulated disk full")

    def open_with_disk_failure(path: Path, *args, **kwargs):
        handle = real_open(path, *args, **kwargs)
        if path.suffix == ".part":
            return FailingWriter(handle)
        return handle

    class Upload:
        async def read(self, size: int = -1) -> bytes:
            return b"1234567"

    monkeypatch.setattr(Path, "open", open_with_disk_failure)
    with pytest.raises(UploadStorageError):
        asyncio.run(store_upload(Upload(), override_test_settings, suffix=".ndjson"))

    assert list(override_test_settings.upload_directory.iterdir()) == []


def test_import_rejects_empty_file(override_test_settings, client) -> None:
    response = client.post(
        "/imports",
        files={"file": ("empty.ndjson", b"", "application/x-ndjson")},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "empty_upload"
    assert list(override_test_settings.upload_directory.iterdir()) == []


def test_import_accepts_json_and_ignores_client_filename(override_test_settings, client) -> None:
    body = b'{"orders": []}'
    response = client.post(
        "/imports",
        files={"file": ("../../outside.json", body, "application/json")},
    )

    assert response.status_code == 202
    result = response.json()
    assert result["stored_filename"] == f"{result['import_id']}.json"
    assert (override_test_settings.upload_directory / result["stored_filename"]).read_bytes() == body
    assert not (override_test_settings.upload_directory.parent / "outside.json").exists()


def test_import_accepts_file_exactly_at_configured_limit(override_test_settings, client) -> None:
    body = b"{}\n"
    override_test_settings.maximum_upload_bytes = len(body)
    response = client.post(
        "/imports",
        files={"file": ("orders.ndjson", body, "application/x-ndjson")},
    )

    assert response.status_code == 202
    assert response.json()["bytes_received"] == len(body)


def test_import_accepts_one_byte_file_without_parsing_it(client) -> None:
    response = client.post(
        "/imports",
        files={"file": ("one-byte.json", b"x", "application/json")},
    )

    assert response.status_code == 202
    assert response.json()["bytes_received"] == 1


def test_importing_same_client_filename_creates_distinct_safe_files(client, override_test_settings) -> None:
    first = client.post(
        "/imports",
        files={"file": ("orders.ndjson", b"{}\n", "application/x-ndjson")},
    )
    second = client.post(
        "/imports",
        files={"file": ("orders.ndjson", b"{}\n", "application/x-ndjson")},
    )

    assert first.status_code == second.status_code == 202
    assert first.json()["stored_filename"] != second.json()["stored_filename"]
    assert len(list(override_test_settings.upload_directory.glob("*.ndjson"))) == 2
