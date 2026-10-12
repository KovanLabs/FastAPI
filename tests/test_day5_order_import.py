import json
from pathlib import Path

import pytest

import app.core.order_pipeline as order_pipeline
from app.core.order_pipeline import process_order_file, validate_order_business
from app.core.parsers import InvalidJsonError
from app.schemas.orders import OrderCreate
from scripts.measure_duplicate_index import measure_duplicate_index


SUPPORTED_CURRENCIES = {"EUR", "INR", "USD"}
GOLDEN_DIRECTORY = Path(__file__).parent / "golden" / "day5"


def _order(order_id: str, **overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "order_id": order_id,
        "customer_email": "USER@EXAMPLE.COM",
        "amount": "10.005",
        "currency": " inr ",
        "ordered_at": "2026-10-05T04:30:00+00:00",
        "status": " PENDING ",
        "items": [{"sku": " sku-AbC ", "quantity": 2}],
    }
    value.update(overrides)
    return value


def _read_ndjson(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_business_validation_is_separate_from_schema_validation() -> None:
    order = OrderCreate.model_validate(
        {
            "order_id": "ORD-1",
            "customer_email": "user@example.com",
            "amount": "10.00",
            "currency": "GBP",
            "ordered_at": "2026-10-05T10:00:00+05:30",
            "status": "pending",
            "items": [{"sku": "SKU-1", "quantity": 1}],
        }
    )

    issues = validate_order_business(order, SUPPORTED_CURRENCIES)

    assert [(issue.error_type, issue.location) for issue in issues] == [
        ("unsupported_currency", ("currency",)),
    ]


def test_process_order_file_writes_reconciled_last_valid_wins_outputs(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "orders.json"
    accepted_path = tmp_path / "accepted.ndjson"
    rejected_path = tmp_path / "rejected.ndjson"
    input_path.write_text(
        json.dumps(
            [
                _order(" ord-1 ", amount="1.00"),
                _order(
                    "cleaning-bad",
                    customer_email=" ",
                    items=[{"sku": " ", "quantity": True}],
                ),
                _order(
                    "schema-bad",
                    customer_email="not-an-email",
                    items=[{"sku": "x" * 81, "quantity": 1}],
                ),
                _order("ORD-1", amount="2.345"),
                _order("business-bad", currency="GBP"),
                _order("ord-1", amount="N/A"),
                _order(
                    "ord-2",
                    customer_email="SECOND@EXAMPLE.COM",
                    extra="omit me",
                    items=[
                        {"sku": "Case-Sensitive", "quantity": 3, "extra": "omit me"}
                    ],
                ),
                7,
            ]
        ),
        encoding="utf-8",
    )

    summary = process_order_file(
        input_path,
        accepted_path=accepted_path,
        rejected_path=rejected_path,
        supported_currencies=SUPPORTED_CURRENCIES,
        maximum_failure_samples=2,
        commit_interval=2,
    )

    accepted = _read_ndjson(accepted_path)
    rejected = _read_ndjson(rejected_path)

    assert summary.total_records == 8
    assert summary.accepted_records == 2
    assert summary.rejected_records == 6
    assert summary.total_records == summary.accepted_records + summary.rejected_records
    assert len(summary.failure_samples) == 2
    assert summary.failures_by_code == {
        "cleaning_failed": 2,
        "duplicate_order_id": 1,
        "invalid_order": 1,
        "not_object": 1,
        "unsupported_currency": 1,
    }
    assert [entry["record_number"] for entry in accepted] == [4, 7]
    assert accepted[0]["order"] == {
        "order_id": "ORD-1",
        "customer_email": "user@example.com",
        "amount": "2.35",
        "currency": "INR",
        "ordered_at": "2026-10-05T10:00:00+05:30",
        "status": "pending",
        "items": [{"sku": "sku-AbC", "quantity": 2}],
    }
    assert accepted[1]["order"]["order_id"] == "ORD-2"
    assert "extra" not in accepted[1]["order"]
    assert "extra" not in accepted[1]["order"]["items"][0]

    assert [entry["record_number"] for entry in rejected] == [1, 2, 3, 5, 6, 8]
    assert [entry["error_code"] for entry in rejected] == [
        "duplicate_order_id",
        "cleaning_failed",
        "invalid_order",
        "unsupported_currency",
        "cleaning_failed",
        "not_object",
    ]
    cleaning_locations = {
        tuple(issue["location"])
        for issue in rejected[1]["issues"]
    }
    assert cleaning_locations == {
        ("customer_email",),
        ("items", 0, "sku"),
        ("items", 0, "quantity"),
    }
    schema_locations = {
        tuple(issue["location"])
        for issue in rejected[2]["issues"]
    }
    assert schema_locations == {
        ("customer_email",),
        ("items", 0, "sku"),
    }
    assert {issue["code"] for issue in rejected[2]["issues"]} == {
        "invalid_email",
        "invalid_sku",
    }
    assert not list(tmp_path.glob("*.part"))


def test_process_order_file_is_idempotent(tmp_path: Path) -> None:
    input_path = tmp_path / "orders.json"
    accepted_path = tmp_path / "accepted.ndjson"
    rejected_path = tmp_path / "rejected.ndjson"
    input_path.write_text(
        json.dumps([_order("same-id"), _order("SAME-ID", amount="20.00")]),
        encoding="utf-8",
    )

    first = process_order_file(
        input_path,
        accepted_path=accepted_path,
        rejected_path=rejected_path,
        supported_currencies=SUPPORTED_CURRENCIES,
        commit_interval=1,
    )
    first_accepted = accepted_path.read_bytes()
    first_rejected = rejected_path.read_bytes()

    second = process_order_file(
        input_path,
        accepted_path=accepted_path,
        rejected_path=rejected_path,
        supported_currencies=SUPPORTED_CURRENCIES,
        commit_interval=1,
    )

    assert second == first
    assert accepted_path.read_bytes() == first_accepted
    assert rejected_path.read_bytes() == first_rejected


def test_failed_import_does_not_replace_existing_outputs(tmp_path: Path) -> None:
    input_path = tmp_path / "broken.json"
    accepted_path = tmp_path / "accepted.ndjson"
    rejected_path = tmp_path / "rejected.ndjson"
    input_path.write_text('[{"order_id":"unfinished"}', encoding="utf-8")
    accepted_path.write_text("previous accepted\n", encoding="utf-8")
    rejected_path.write_text("previous rejected\n", encoding="utf-8")

    with pytest.raises(InvalidJsonError):
        process_order_file(
            input_path,
            accepted_path=accepted_path,
            rejected_path=rejected_path,
            supported_currencies=SUPPORTED_CURRENCIES,
        )

    assert accepted_path.read_text(encoding="utf-8") == "previous accepted\n"
    assert rejected_path.read_text(encoding="utf-8") == "previous rejected\n"
    assert not list(tmp_path.glob("*.part"))
    assert not list(tmp_path.glob("*.sqlite3"))


def test_second_output_publication_failure_restores_both_previous_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_path = tmp_path / "orders.json"
    accepted_path = tmp_path / "accepted.ndjson"
    rejected_path = tmp_path / "rejected.ndjson"
    input_path.write_text(json.dumps([_order("ORD-1")]), encoding="utf-8")
    accepted_path.write_text("previous accepted\n", encoding="utf-8")
    rejected_path.write_text("previous rejected\n", encoding="utf-8")
    real_replace = order_pipeline.os.replace

    def fail_rejected_publication(source: object, destination: object) -> None:
        if Path(source).suffix == ".part" and Path(destination) == rejected_path:
            raise OSError("simulated rejected-output publication failure")
        real_replace(source, destination)

    monkeypatch.setattr(order_pipeline.os, "replace", fail_rejected_publication)

    with pytest.raises(OSError, match="simulated"):
        process_order_file(
            input_path,
            accepted_path=accepted_path,
            rejected_path=rejected_path,
            supported_currencies=SUPPORTED_CURRENCIES,
        )

    assert accepted_path.read_text(encoding="utf-8") == "previous accepted\n"
    assert rejected_path.read_text(encoding="utf-8") == "previous rejected\n"
    assert not list(tmp_path.glob("*.part"))
    assert not list(tmp_path.glob("*.backup"))
    assert not list(tmp_path.glob("*.sqlite3"))


def test_standard_python_engine_matches_frozen_golden_output(tmp_path: Path) -> None:
    accepted_path = tmp_path / "accepted.ndjson"
    rejected_path = tmp_path / "rejected.ndjson"

    summary = process_order_file(
        GOLDEN_DIRECTORY / "orders.json",
        accepted_path=accepted_path,
        rejected_path=rejected_path,
        supported_currencies=SUPPORTED_CURRENCIES,
        commit_interval=2,
    )

    assert accepted_path.read_bytes() == (
        GOLDEN_DIRECTORY / "accepted.ndjson"
    ).read_bytes()
    assert rejected_path.read_bytes() == (
        GOLDEN_DIRECTORY / "rejected.ndjson"
    ).read_bytes()
    expected_summary = json.loads(
        (GOLDEN_DIRECTORY / "summary.json").read_text(encoding="utf-8")
    )
    assert {
        "total_records": summary.total_records,
        "accepted_records": summary.accepted_records,
        "rejected_records": summary.rejected_records,
        "failures_by_code": summary.failures_by_code,
    } == expected_summary


def test_duplicate_index_memory_measurement_is_reproducible() -> None:
    measurement = measure_duplicate_index(1_000)

    assert measurement.unique_ids == 1_000
    assert measurement.current_bytes > 0
    assert measurement.peak_bytes >= measurement.current_bytes
    assert measurement.peak_bytes_per_id > 0
    assert measurement.projected_peak_bytes_at_10m > measurement.peak_bytes
