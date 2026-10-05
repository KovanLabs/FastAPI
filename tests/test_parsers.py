from io import BytesIO

import pytest

from app.core.json_array_validation import summarize_json_array
from app.core.ndjson_validation import summarize_ndjson
from app.core.parsers import (
    InvalidJsonError,
    RecordTooLargeError,
    iter_file_chunks,
    iter_json_array,
    iter_ndjson,
)

def test_iter_ndjson_reports_invalid_json_and_continues() -> None:
    lines = BytesIO(b'{"id": 1}\n{"id": }\n{"id": 3}\n')

    outcomes = list(iter_ndjson(lines, maximum_record_bytes=100))

    assert [
        (outcome.line_number, outcome.record, outcome.error_code)
        for outcome in outcomes
    ] == [
        (1, {"id": 1}, None),
        (2, None, "invalid_json"),
        (3, {"id": 3}, None),
    ]


class TrackingBytesIO(BytesIO):
    def __init__(self, contents: bytes) -> None:
        super().__init__(contents)
        self.readline_sizes: list[int] = []


    def readline(self, size: int = -1) -> bytes:
        self.readline_sizes.append(size)
        return super().readline(size)


class TrackingReadBytesIO(BytesIO):
    def __init__(self, contents: bytes) -> None:
        super().__init__(contents)
        self.read_sizes: list[int] = []

    def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        return super().read(size)


def test_iter_ndjson_bounds_reads_and_recovers_from_mixed_records() -> None:
    maximum_record_bytes = 9
    oversized = b'{"id":"' + (b"x" * 30) + b'"}\n'
    source = TrackingBytesIO(
        b"\n"  # blank
        b" \t\r\n"  # whitespace-only blank
        b'{"id": 1}\r\n'  # exactly 9 record bytes; CRLF is excluded
        b"\xff\n"  # invalid UTF-8
        b'{"id": }\n'  # invalid JSON
        b"[]\n"  # valid JSON, but not an object
        + oversized  # oversized record
        + b'{"id":3}'  # valid final record without a trailing newline
    )

    outcomes = list(
        iter_ndjson(source, maximum_record_bytes=maximum_record_bytes)
    )

    assert [
        (
            outcome.line_number,
            outcome.record,
            outcome.error_code,
            outcome.skipped_blank,
        )
        for outcome in outcomes
    ] == [
        (1, None, None, True),
        (2, None, None, True),
        (3, {"id": 1}, None, False),
        (4, None, "invalid_utf8", False),
        (5, None, "invalid_json", False),
        (6, None, "not_object", False),
        (7, None, "record_too_large", False),
        (8, {"id": 3}, None, False),
    ]
    assert source.readline_sizes
    assert all(
        0 < size <= maximum_record_bytes + 2
        for size in source.readline_sizes
    )


def test_ndjson_summary_counts_every_failure_but_caps_samples() -> None:
    source = BytesIO(b'{}\n{"bad"}\n[]\n\xff\n \n')

    summary = summarize_ndjson(
        source,
        maximum_record_bytes=100,
        maximum_failure_samples=2,
    )

    assert summary.valid_records == 1
    assert summary.skipped_blank == 1
    assert summary.failures_by_code == {
        "invalid_json": 1,
        "not_object": 1,
        "invalid_utf8": 1,
    }
    assert summary.failure_count == 3
    assert summary.failure_samples == [
        {"line_number": 2, "error_code": "invalid_json"},
        {"line_number": 3, "error_code": "not_object"},
    ]


def test_json_array_parser_preserves_records_across_chunks() -> None:
    chunks = [
        b'[{"id":',
        b'1},',
        b'{"id":2}]',
    ]

    records = list(
        iter_json_array(chunks, maximum_record_bytes=100)
    )

    assert records == [{"id": 1}, {"id": 2}]


def test_json_array_parser_preserves_string_state_across_chunks() -> None:
    chunks = [
        b'[{"note":"use }',
        b' carefully"},{"note":"say \\',
        b'"hello\\""}]',
    ]

    records = list(
        iter_json_array(chunks, maximum_record_bytes=100)
    )

    assert records == [
        {"note": "use } carefully"},
        {"note": 'say "hello"'},
    ]


def test_json_array_parser_stops_immediately_when_record_exceeds_limit() -> None:
    def chunks():
        yield b'[{"id":"'
        yield b'xxxxxxxxxx'
        raise AssertionError(
            "Parser requested another chunk after the record exceeded its limit"
        )

    with pytest.raises(RecordTooLargeError):
        list(iter_json_array(chunks(), maximum_record_bytes=10))


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b'{"id":1}', "Top-level value must be a JSON array"),
        (b'[{"id":1}', "missing its closing bracket"),
        (b'[{"id":1},]', "Trailing comma"),
        (b'[{"id":1},,{"id":2}]', "Missing JSON array element"),
        (b'[{"id":1}] false', "Unexpected content"),
        (b'[{"id":1}{"id":2}]', "Expected ',' or ']'"),
    ],
)
def test_json_array_parser_rejects_malformed_document(
    payload: bytes,
    message: str,
) -> None:
    with pytest.raises(InvalidJsonError, match=message):
        list(iter_json_array([payload], maximum_record_bytes=100))


def test_file_chunks_use_only_the_configured_read_size() -> None:
    source = TrackingReadBytesIO(b"abcdefgh")

    assert list(iter_file_chunks(source, chunk_bytes=3)) == [b"abc", b"def", b"gh"]
    assert source.read_sizes == [3, 3, 3, 3]


def test_json_array_summary_continues_after_non_object_value() -> None:
    source = BytesIO(b'[{"id":1},3,{"id":2}]')

    summary = summarize_json_array(
        source,
        maximum_record_bytes=100,
        chunk_bytes=4,
    )

    assert summary.valid_records == 2
    assert summary.failure_count == 1
    assert summary.failures_by_code == {"not_object": 1}
    assert summary.failure_samples == [
        {"record_number": 2, "error_code": "not_object"}
    ]
