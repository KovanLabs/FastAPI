from pathlib import Path

import pytest

from app.core.processing_adapters import (
    AdapterExecutionError,
    DuckDBAdapter,
    PolarsAdapter,
    StandardPythonAdapter,
    create_processing_adapter,
)


GOLDEN_DIRECTORY = Path(__file__).parent / "golden" / "day5"
GOLDEN_PROCESSED = Path(__file__).parent / "golden" / "day6" / "processed.ndjson"


@pytest.mark.parametrize("engine", ["python", "polars", "duckdb"])
def test_processing_adapter_factory_selects_configured_engine(engine: str) -> None:
    adapter = create_processing_adapter(engine, maximum_record_bytes=123)

    assert adapter.engine == engine
    assert adapter.maximum_record_bytes == 123


def test_standard_python_adapter_reproduces_golden_accepted_output(
    tmp_path: Path,
) -> None:
    source = GOLDEN_DIRECTORY / "accepted.ndjson"
    output = tmp_path / "accepted.ndjson.part"

    result = StandardPythonAdapter().process(source, output)

    assert result.engine == "python"
    assert result.records_written == 1
    assert result.output_path == output
    assert output.read_bytes() == GOLDEN_PROCESSED.read_bytes()


def test_polars_adapter_reproduces_golden_accepted_output(tmp_path: Path) -> None:
    source = GOLDEN_DIRECTORY / "accepted.ndjson"
    output = tmp_path / "accepted.ndjson.part"

    result = PolarsAdapter().process(source, output)

    assert result.engine == "polars"
    assert result.records_written == 1
    assert result.output_path == output
    assert output.read_bytes() == GOLDEN_PROCESSED.read_bytes()


def test_duckdb_adapter_reproduces_golden_accepted_output(tmp_path: Path) -> None:
    source = GOLDEN_DIRECTORY / "accepted.ndjson"
    output = tmp_path / "accepted.ndjson.part"

    result = DuckDBAdapter().process(source, output)

    assert result.engine == "duckdb"
    assert result.records_written == 1
    assert result.output_path == output
    assert output.read_bytes() == GOLDEN_PROCESSED.read_bytes()


def test_duckdb_adapter_can_capture_peak_engine_resources(tmp_path: Path) -> None:
    source = GOLDEN_DIRECTORY / "accepted.ndjson"
    output = tmp_path / "accepted.ndjson.part"
    adapter = DuckDBAdapter(collect_profile=True)

    adapter.process(source, output)

    assert adapter.last_profile is not None
    assert adapter.last_profile["system_peak_buffer_memory"] > 0
    assert adapter.last_profile["system_peak_temp_dir_size"] >= 0
    assert list(tmp_path.glob(".duckdb-adapter-*")) == []


@pytest.mark.parametrize(
    "adapter_type",
    [StandardPythonAdapter, PolarsAdapter, DuckDBAdapter],
    ids=["python", "polars", "duckdb"],
)
def test_adapters_are_idempotent(
    tmp_path: Path,
    adapter_type: type[StandardPythonAdapter]
    | type[PolarsAdapter]
    | type[DuckDBAdapter],
) -> None:
    source = GOLDEN_DIRECTORY / "accepted.ndjson"
    first_output = tmp_path / "first.ndjson.part"
    second_output = tmp_path / "second.ndjson.part"
    adapter = adapter_type()

    adapter.process(source, first_output)
    adapter.process(source, second_output)

    assert first_output.read_bytes() == second_output.read_bytes()


@pytest.mark.parametrize(
    "adapter_type",
    [StandardPythonAdapter, PolarsAdapter, DuckDBAdapter],
    ids=["python", "polars", "duckdb"],
)
def test_adapters_order_records_and_fields_deterministically(
    tmp_path: Path,
    adapter_type: type[StandardPythonAdapter]
    | type[PolarsAdapter]
    | type[DuckDBAdapter],
) -> None:
    source = tmp_path / "source.ndjson"
    output = tmp_path / "output.ndjson.part"
    source.write_text(
        '{"order":{"items":[{"quantity":2,"sku":"A-1"},'
        '{"quantity":3,"sku":"A-2"}],"status":"completed",'
        '"ordered_at":"2026-10-06T09:00:00+05:30","currency":"USD",'
        '"amount":"1.00","customer_email":"a@example.com","order_id":"A"},'
        '"record_number":1}\n'
        '{"order":{"status":"pending","items":['
        '{"quantity":2,"sku":"B-2"},{"sku":"B-1","quantity":1}],'
        '"ordered_at":"2026-10-06T10:00:00+05:30","currency":"INR",'
        '"amount":"2.00","customer_email":"b@example.com","order_id":"B"},'
        '"record_number":2}\n',
        encoding="utf-8",
    )

    adapter_type().process(source, output)

    assert output.read_text(encoding="utf-8") == (
        '{"record_number":1,"order":{"order_id":"A",'
        '"customer_email":"a@example.com","amount":"1.00","currency":"USD",'
        '"ordered_at":"2026-10-06T09:00:00+05:30","status":"completed",'
        '"items":[{"sku":"A-1","quantity":2},'
        '{"sku":"A-2","quantity":3}]},'
        '"item_count":2,"total_quantity":5}\n'
        '{"record_number":2,"order":{"order_id":"B",'
        '"customer_email":"b@example.com","amount":"2.00","currency":"INR",'
        '"ordered_at":"2026-10-06T10:00:00+05:30","status":"pending",'
        '"items":[{"sku":"B-2","quantity":2},'
        '{"sku":"B-1","quantity":1}]},'
        '"item_count":2,"total_quantity":3}\n'
    )


@pytest.mark.parametrize(
    "record_numbers",
    [(1, 1), (2, 1)],
    ids=["duplicate", "out-of-order"],
)
@pytest.mark.parametrize(
    "adapter_type",
    [StandardPythonAdapter, PolarsAdapter, DuckDBAdapter],
    ids=["python", "polars", "duckdb"],
)
def test_adapters_reject_non_increasing_record_numbers(
    tmp_path: Path,
    record_numbers: tuple[int, int],
    adapter_type: type[StandardPythonAdapter]
    | type[PolarsAdapter]
    | type[DuckDBAdapter],
) -> None:
    source = tmp_path / "source.ndjson"
    output = tmp_path / "output.ndjson.part"

    template = (GOLDEN_DIRECTORY / "accepted.ndjson").read_text(
        encoding="utf-8"
    ).strip()

    first, second = record_numbers
    source.write_text(
        template.replace('"record_number":3', f'"record_number":{first}') + "\n"
        + template.replace('"record_number":3', f'"record_number":{second}') + "\n",
        encoding="utf-8",
    )

    with pytest.raises(AdapterExecutionError):
        adapter_type().process(source, output)

    assert not output.exists()


@pytest.mark.parametrize(
    "invalid_line",
    [
        '{"record_number":1,"order":',
        (
            '{"record_number":1,"order":{"order_id":"A",'
            '"customer_email":"a@example.com","amount":"1.00",'
            '"ordered_at":"2026-10-06T09:00:00+05:30",'
            '"status":"pending","items":[{"sku":"A","quantity":1}]}}'
        ),
        (
            '{"record_number":1,"order":{"order_id":"A",'
            '"customer_email":"a@example.com","amount":1.0,"currency":"USD",'
            '"ordered_at":"2026-10-06T09:00:00+05:30",'
            '"status":"pending","items":[{"sku":"A","quantity":1}]}}'
        ),
        (
            '{"record_number":1,"order":{"order_id":"A",'
            '"customer_email":"a@example.com","amount":"1.00","currency":"USD",'
            '"ordered_at":"2026-10-06T09:00:00+05:30",'
            '"status":"pending","items":[{"sku":"A","quantity":true}]}}'
        ),
        (
            '{"record_number":true,"order":{"order_id":"A",'
            '"customer_email":"a@example.com","amount":"1.00","currency":"USD",'
            '"ordered_at":"2026-10-06T09:00:00+05:30",'
            '"status":"pending","items":[{"sku":"A","quantity":1}]}}'
        ),
        (
            '{"record_number":9223372036854775808,"order":{"order_id":"A",'
            '"customer_email":"a@example.com","amount":"1.00","currency":"USD",'
            '"ordered_at":"2026-10-06T09:00:00+05:30",'
            '"status":"pending","items":[{"sku":"A","quantity":1}]}}'
        ),
        (
            '{"record_number":1,"order":{"order_id":"A",'
            '"customer_email":"a@example.com","amount":"1.00","currency":"USD",'
            '"ordered_at":"2026-10-06T09:00:00+05:30",'
            '"status":"pending","items":[]}}'
        ),
        (
            '{"record_number":1,"order":{"order_id":"A",'
            '"customer_email":"a@example.com","amount":"1.00","currency":"USD",'
            '"ordered_at":"2026-10-06T09:00:00+05:30",'
            '"status":"pending","items":[{"sku":"A","quantity":0}]}}'
        ),
        (
            '{"record_number":1,"order":{"order_id":"A",'
            '"customer_email":"a@example.com","amount":"1.00","currency":"USD",'
            '"ordered_at":"2026-10-06T09:00:00+05:30",'
            '"status":"pending","items":[{"sku":"A","quantity":-1}]}}'
        ),
        (
            '{"record_number":1,"order":{"order_id":"A",'
            '"customer_email":"a@example.com","amount":"1.00","currency":"USD",'
            '"ordered_at":"2026-10-06T09:00:00+05:30",'
            '"status":"pending","items":['
            '{"sku":"A","quantity":9223372036854775807},'
            '{"sku":"B","quantity":1}]}}'
        ),
    ],
    ids=[
        "malformed-json",
        "missing-required-field",
        "amount-not-string",
        "quantity-not-strict-integer",
        "record-number-not-strict-integer",
        "record-number-over-int64",
        "empty-items",
        "zero-quantity",
        "negative-quantity",
        "total-quantity-over-int64",
    ],
)
@pytest.mark.parametrize(
    "adapter_type",
    [StandardPythonAdapter, PolarsAdapter, DuckDBAdapter],
    ids=["python", "polars", "duckdb"],
)
def test_adapters_fail_closed_without_partial_output(
    tmp_path: Path,
    invalid_line: str,
    adapter_type: type[StandardPythonAdapter]
    | type[PolarsAdapter]
    | type[DuckDBAdapter],
) -> None:
    source = tmp_path / "invalid.ndjson"
    output = tmp_path / "output.ndjson.part"
    source.write_text(invalid_line + "\n", encoding="utf-8")

    with pytest.raises(AdapterExecutionError):
        adapter_type().process(source, output)

    assert not output.exists()


def test_polars_adapter_rejects_missing_required_field(tmp_path: Path) -> None:
    source = tmp_path / "missing-currency.ndjson"
    output = tmp_path / "output.ndjson.part"
    source.write_text(
        '{"record_number":1,"order":{"order_id":"A",'
        '"customer_email":"a@example.com","amount":"1.00",'
        '"ordered_at":"2026-10-06T09:00:00+05:30",'
        '"status":"pending","items":[{"sku":"A","quantity":1}]}}\n',
        encoding="utf-8",
    )

    with pytest.raises(AdapterExecutionError):
        PolarsAdapter().process(source, output)

    assert not output.exists()


@pytest.mark.parametrize(
    "adapter_type",
    [StandardPythonAdapter, PolarsAdapter, DuckDBAdapter],
    ids=["python", "polars", "duckdb"],
)
def test_adapters_remove_valid_prefix_after_later_failure(
    tmp_path: Path,
    adapter_type: type[StandardPythonAdapter]
    | type[PolarsAdapter]
    | type[DuckDBAdapter],
) -> None:
    source = tmp_path / "partially-valid.ndjson"
    output = tmp_path / "output.ndjson.part"
    source.write_bytes(
        (GOLDEN_DIRECTORY / "accepted.ndjson").read_bytes()
        + b'{"record_number":4,"order":\n'
    )

    with pytest.raises(AdapterExecutionError):
        adapter_type().process(source, output)

    assert not output.exists()


@pytest.mark.parametrize(
    "adapter_type",
    [StandardPythonAdapter, PolarsAdapter, DuckDBAdapter],
    ids=["python", "polars", "duckdb"],
)
def test_adapters_accept_empty_input(
    tmp_path: Path,
    adapter_type: type[StandardPythonAdapter]
    | type[PolarsAdapter]
    | type[DuckDBAdapter],
) -> None:
    source = tmp_path / "empty.ndjson"
    output = tmp_path / "output.ndjson.part"
    source.write_bytes(b"")

    result = adapter_type().process(source, output)

    assert result.records_written == 0
    assert output.read_bytes() == b""


@pytest.mark.parametrize(
    "adapter_type",
    [StandardPythonAdapter, PolarsAdapter, DuckDBAdapter],
    ids=["python", "polars", "duckdb"],
)
def test_adapters_reject_a_canonical_record_over_the_byte_limit(
    tmp_path: Path,
    adapter_type: type[StandardPythonAdapter]
    | type[PolarsAdapter]
    | type[DuckDBAdapter],
) -> None:
    source = GOLDEN_DIRECTORY / "accepted.ndjson"
    output = tmp_path / "output.ndjson.part"

    with pytest.raises(AdapterExecutionError, match="record byte limit"):
        adapter_type(maximum_record_bytes=100).process(source, output)

    assert not output.exists()


@pytest.mark.parametrize(
    "adapter_type",
    [StandardPythonAdapter, PolarsAdapter, DuckDBAdapter],
    ids=["python", "polars", "duckdb"],
)
def test_adapters_do_not_overwrite_or_delete_existing_output(
    tmp_path: Path,
    adapter_type: type[StandardPythonAdapter]
    | type[PolarsAdapter]
    | type[DuckDBAdapter],
) -> None:
    source = GOLDEN_DIRECTORY / "accepted.ndjson"
    output = tmp_path / "existing.ndjson.part"
    output.write_text("existing output\n", encoding="utf-8")

    with pytest.raises(AdapterExecutionError):
        adapter_type().process(source, output)

    assert output.read_text(encoding="utf-8") == "existing output\n"
