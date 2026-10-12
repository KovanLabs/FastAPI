import json
from io import BytesIO

import pytest

from app.core.order_pipeline import (
    clean_order_batch,
    iter_batches,
    iter_ijson_array,
    iter_order_outcomes,
    summarize_order_file,
)
from app.core.parsers import InvalidJsonError
from scripts.generate_orders_json import make_order


SUPPORTED_CURRENCIES = {"EUR", "INR", "USD"}


def test_ijson_iterator_requires_one_top_level_array() -> None:
    assert list(iter_ijson_array(BytesIO(b'[{"id":1},{"id":2}]'))) == [
        {"id": 1},
        {"id": 2},
    ]

    with pytest.raises(InvalidJsonError, match="JSON array"):
        list(iter_ijson_array(BytesIO(b'{"id":1}')))


def test_ijson_iterator_translates_malformed_json_error() -> None:
    with pytest.raises(InvalidJsonError, match="Invalid JSON array"):
        list(iter_ijson_array(BytesIO(b'[{"id":1}')))


def test_iter_batches_never_exceeds_configured_size() -> None:
    assert list(iter_batches(range(7), batch_size=3)) == [
        [0, 1, 2],
        [3, 4, 5],
        [6],
    ]


def test_polars_cleans_scalar_fields_and_marks_later_duplicate() -> None:
    records = [
        (
            1,
            {
                "order_id": " ORD-1 ",
                "customer_email": " USER@EXAMPLE.COM ",
                "amount": " 12.30 ",
                "currency": " usd ",
                "ordered_at": " 2026-09-01T00:00:00.000Z ",
                "status": " PENDING ",
                "items": [{"sku": "SKU-1", "quantity": 1}],
            },
        ),
        (2, {"order_id": "ORD-1"}),
    ]

    first, second = clean_order_batch(records)

    assert first.value["order_id"] == "ORD-1"
    assert first.value["customer_email"] == "user@example.com"
    assert first.value["amount"] == "12.30"
    assert first.value["currency"] == "USD"
    assert first.value["ordered_at"].isoformat() == "2026-09-01T00:00:00+00:00"
    assert first.value["status"] == "pending"
    assert first.duplicate_order_id is False
    assert second.duplicate_order_id is True


def test_polars_does_not_silently_round_amount() -> None:
    cleaned = clean_order_batch([(1, {"amount": " 1.239 "})])

    assert cleaned[0].value["amount"] == "1.239"


def test_pipeline_separates_cleaned_valid_and_rejected_orders() -> None:
    records = [make_order(index) for index in range(10)]
    source = BytesIO(json.dumps(records).encode())

    outcomes = list(
        iter_order_outcomes(
            source,
            batch_size=10,
            supported_currencies=SUPPORTED_CURRENCIES,
        )
    )

    assert [outcome.record_number for outcome in outcomes if outcome.accepted] == [
        4,
        5,
    ]
    assert [outcome.error_code for outcome in outcomes] == [
        "invalid_order",
        "invalid_order",
        "invalid_order",
        None,
        None,
        "invalid_order",
        "invalid_order",
        "invalid_order",
        "invalid_order",
        "duplicate_order_id",
    ]
    assert outcomes[3].order is not None
    assert outcomes[3].order.currency == "INR"
    assert outcomes[4].order is not None
    assert outcomes[4].order.status == "pending"
    assert outcomes[5].issues[0].location == ("items", 0, "sku")


def test_pipeline_reports_non_object_and_unsupported_currency() -> None:
    unsupported = make_order(10)
    unsupported["currency"] = "GBP"
    source = BytesIO(json.dumps([3, unsupported]).encode())

    outcomes = list(
        iter_order_outcomes(
            source,
            batch_size=2,
            supported_currencies=SUPPORTED_CURRENCIES,
        )
    )

    assert [outcome.error_code for outcome in outcomes] == [
        "not_object",
        "unsupported_currency",
    ]


def test_summary_retains_counts_but_caps_failure_samples(tmp_path) -> None:
    path = tmp_path / "orders.json"
    path.write_text(json.dumps([None, 1, [], "bad"]), encoding="utf-8")

    summary = summarize_order_file(
        path,
        batch_size=2,
        supported_currencies=SUPPORTED_CURRENCIES,
        maximum_failure_samples=2,
    )

    assert summary.total_records == 4
    assert summary.accepted_records == 0
    assert summary.rejected_records == 4
    assert summary.failures_by_code == {"not_object": 4}
    assert len(summary.failure_samples) == 2
