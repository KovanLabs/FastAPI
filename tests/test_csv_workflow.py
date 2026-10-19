import csv
import json
from io import StringIO
from pathlib import Path

import pytest

from app.core import order_pipeline
from app.core.csv_export import export_processed_csv
from app.core.order_pipeline import iter_csv_orders, process_order_file
from app.core.parsers import InvalidCsvError, RecordTooLargeError
from app.core.processing_adapters import AdapterExecutionError
from scripts.generate_orders_csv import write_orders_csv
from scripts.generate_orders_json import make_order, write_orders
from scripts.compare_end_to_end import summarize_measurements


def test_csv_generator_preserves_the_json_fixture_values() -> None:
    output = StringIO(newline="")

    write_orders_csv(output, 10)

    rows = list(csv.DictReader(StringIO(output.getvalue(), newline="")))
    assert len(rows) == 10
    for index, row in enumerate(rows):
        expected = make_order(index)
        assert row["order_id"] == str(expected["order_id"])
        expected_email = expected["customer_email"]
        assert row["customer_email"] == (
            r"\N" if expected_email is None else str(expected_email)
        )
        assert row["amount"] == str(expected["amount"])
        assert row["currency"] == str(expected["currency"])
        assert row["ordered_at"] == str(expected["ordered_at"])
        assert row["status"] == str(expected["status"])
        assert json.loads(row["items"]) == expected["items"]


def test_csv_and_json_cleaning_produce_equivalent_canonical_outputs(
    tmp_path: Path,
) -> None:
    json_source = tmp_path / "orders.json"
    csv_source = tmp_path / "orders.csv"
    with json_source.open("w", encoding="utf-8") as output:
        write_orders(output, 10)
    with csv_source.open("w", encoding="utf-8", newline="") as output:
        write_orders_csv(output, 10)

    results = {}
    for source in (json_source, csv_source):
        accepted = tmp_path / f"{source.suffix}.accepted.ndjson"
        rejected = tmp_path / f"{source.suffix}.rejected.ndjson"
        summary = process_order_file(
            source,
            accepted_path=accepted,
            rejected_path=rejected,
            supported_currencies={"EUR", "INR", "USD"},
        )
        results[source.suffix] = (
            accepted.read_bytes(),
            rejected.read_bytes(),
            summary.total_records,
            summary.accepted_records,
            summary.rejected_records,
            summary.failures_by_code,
        )

    assert results[".csv"] == results[".json"]


def test_processed_ndjson_is_atomically_exported_as_deterministic_csv(
    tmp_path: Path,
) -> None:
    processed = tmp_path / "processed.ndjson"
    processed.write_text(
        json.dumps(
            {
                "record_number": 4,
                "order": {
                    "order_id": "ORD-4",
                    "customer_email": "buyer@example.com",
                    "amount": "10.50",
                    "currency": "INR",
                    "ordered_at": "2026-10-05T10:00:00+05:30",
                    "status": "pending",
                    "items": [{"sku": "SKU,1", "quantity": 2}],
                },
                "item_count": 1,
                "total_quantity": 2,
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    output = tmp_path / "result" / "processed.csv"

    result = export_processed_csv(processed, output)

    assert result.records_written == 1
    assert result.output_path == output
    assert list(output.parent.glob("*.part")) == []
    rows = list(csv.DictReader(output.open(encoding="utf-8", newline="")))
    assert rows == [
        {
            "record_number": "4",
            "order_id": "ORD-4",
            "customer_email": "buyer@example.com",
            "amount": "10.50",
            "currency": "INR",
            "ordered_at": "2026-10-05T10:00:00+05:30",
            "status": "pending",
            "items": '[{"sku":"SKU,1","quantity":2}]',
            "item_count": "1",
            "total_quantity": "2",
        }
    ]


def test_csv_parser_supports_a_quoted_physical_newline(tmp_path: Path) -> None:
    source = tmp_path / "orders.csv"
    source.write_text(
        "order_id,customer_email,amount,currency,ordered_at,status,items\n"
        'ORD-1,buyer@example.com,10.50,INR,2026-10-05T04:30:00+00:00,'
        '"pen\nding","[{""sku"":""SKU-1"",""quantity"":2}]"\n',
        encoding="utf-8",
    )

    records = list(iter_csv_orders(source, maximum_record_bytes=1024))

    assert len(records) == 1
    assert records[0]["status"] == "pen\nding"


def test_csv_parser_starts_with_polars_streaming_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "orders.csv"
    with source.open("w", encoding="utf-8", newline="") as output:
        write_orders_csv(output, 2)
    observed: dict[str, object] = {}
    original_scan_csv = order_pipeline.pl.scan_csv

    class ObservedScan:
        def __init__(self, lazy_frame):
            self.lazy_frame = lazy_frame

        def collect_schema(self):
            return self.lazy_frame.collect_schema()

        def collect_batches(self, **kwargs):
            observed["batch_options"] = kwargs
            return self.lazy_frame.collect_batches(**kwargs)

    def observe_scan_csv(*args, **kwargs):
        observed["source"] = args[0]
        observed["scan_options"] = kwargs
        return ObservedScan(original_scan_csv(*args, **kwargs))

    monkeypatch.setattr(order_pipeline.pl, "scan_csv", observe_scan_csv)

    records = list(
        iter_csv_orders(
            source,
            maximum_record_bytes=1024,
            batch_rows=1,
        )
    )

    assert len(records) == 2
    assert observed["source"] == source
    assert observed["scan_options"] == {
        "has_header": True,
        "schema_overrides": order_pipeline.CSV_SCHEMA,
        "infer_schema": False,
        "null_values": order_pipeline.CSV_NULL,
        "empty_string_is_null": False,
        "encoding": "utf8",
        "ignore_errors": False,
        "truncate_ragged_lines": False,
    }
    assert observed["batch_options"] == {
        "chunk_size": 1,
        "maintain_order": True,
        "engine": "streaming",
    }


@pytest.mark.parametrize(
    "body",
    [
        "",
        "order_id,customer_email,amount,currency,ordered_at,status\n",
        (
            "customer_email,order_id,amount,currency,ordered_at,status,items\n"
        ),
    ],
)
def test_csv_parser_rejects_a_missing_or_noncanonical_header(
    tmp_path: Path,
    body: str,
) -> None:
    source = tmp_path / "orders.csv"
    source.write_text(body, encoding="utf-8")

    with pytest.raises(InvalidCsvError):
        list(iter_csv_orders(source, maximum_record_bytes=1024))


def test_csv_parser_rejects_a_record_over_the_byte_limit(tmp_path: Path) -> None:
    source = tmp_path / "orders.csv"
    source.write_text(
        "order_id,customer_email,amount,currency,ordered_at,status,items\n"
        + "X" * 200
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RecordTooLargeError):
        list(iter_csv_orders(source, maximum_record_bytes=100))


def test_csv_cleaning_rejects_bad_items_and_continues(tmp_path: Path) -> None:
    source = tmp_path / "orders.csv"
    source.write_text(
        "order_id,customer_email,amount,currency,ordered_at,status,items\n"
        "ORD-1,buyer@example.com,10.50,INR,2026-10-05T04:30:00Z,"
        "pending,not-json\n"
        'ORD-2,buyer@example.com,12.50,INR,2026-10-05T04:30:00Z,'
        'pending,"[{""sku"":""SKU-2"",""quantity"":1}]"\n',
        encoding="utf-8",
    )
    accepted = tmp_path / "accepted.ndjson"
    rejected = tmp_path / "rejected.ndjson"

    summary = process_order_file(
        source,
        accepted_path=accepted,
        rejected_path=rejected,
        supported_currencies={"INR"},
    )

    assert summary.total_records == 2
    assert summary.accepted_records == 1
    assert summary.rejected_records == 1
    assert json.loads(accepted.read_text())["order"]["order_id"] == "ORD-2"


def test_csv_export_removes_partial_output_when_a_record_is_invalid(
    tmp_path: Path,
) -> None:
    processed = tmp_path / "processed.ndjson"
    processed.write_text(
        json.dumps(
            {
                "record_number": 1,
                "order": {
                    "order_id": "ORD-1",
                    "customer_email": "buyer@example.com",
                    "amount": "10.50",
                    "currency": "INR",
                    "ordered_at": "2026-10-05T04:30:00+00:00",
                    "status": "pending",
                    "items": [{"sku": "SKU-1", "quantity": 2}],
                },
                "item_count": 99,
                "total_quantity": 2,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    output = tmp_path / "processed.csv"

    with pytest.raises(AdapterExecutionError):
        export_processed_csv(processed, output)

    assert not output.exists()
    assert list(tmp_path.glob("*.part")) == []


def test_end_to_end_comparison_summarizes_repeated_runs() -> None:
    measurements = [
        {
            "workflow": "raw_json_to_duckdb",
            "total_seconds": 4.0,
            "records_per_second": 25.0,
            "peak_rss_mib": 100.0,
            "raw_bytes": 1_000,
            "final_bytes": 500,
            "stage_seconds": {"generation": 1.0, "finalization": 3.0},
        },
        {
            "workflow": "raw_json_to_duckdb",
            "total_seconds": 6.0,
            "records_per_second": 20.0,
            "peak_rss_mib": 120.0,
            "raw_bytes": 1_000,
            "final_bytes": 500,
            "stage_seconds": {"generation": 2.0, "finalization": 4.0},
        },
        {
            "workflow": "raw_csv_to_processed_csv",
            "total_seconds": 2.0,
            "records_per_second": 50.0,
            "peak_rss_mib": 80.0,
            "raw_bytes": 800,
            "final_bytes": 400,
            "stage_seconds": {"generation": 0.5, "finalization": 1.5},
        },
        {
            "workflow": "raw_csv_to_processed_csv",
            "total_seconds": 3.0,
            "records_per_second": 40.0,
            "peak_rss_mib": 90.0,
            "raw_bytes": 800,
            "final_bytes": 400,
            "stage_seconds": {"generation": 1.0, "finalization": 2.0},
        },
    ]

    summary = summarize_measurements(measurements)

    assert summary["faster_workflow"] == "raw_csv_to_processed_csv"
    assert summary["workflows"]["raw_json_to_duckdb"]["runs"] == 2
    assert summary["workflows"]["raw_json_to_duckdb"]["mean_total_seconds"] == 5.0
    assert summary["workflows"]["raw_csv_to_processed_csv"]["max_peak_rss_mib"] == 90.0
