from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from json import JSONDecodeError
from typing import BinaryIO

from app.core.exceptions import ApplicationError


class RecordTooLargeError(ApplicationError):
    error_code = "record_too_large"
    status_code = 413

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message, details=details)


class InvalidJsonError(ApplicationError):
    error_code = "invalid_json"
    status_code = 400

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message, details=details or {})


class InvalidCsvError(ApplicationError):
    error_code = "invalid_csv"
    status_code = 400

    def __init__(self, message: str, *, details: dict | None = None) -> None:
        super().__init__(message, details=details or {})


@dataclass(frozen=True)
class ParseOutcome:

    line_number: int
    record: dict[str, object] | None = None
    error_code: str | None = None
    skipped_blank: bool = False


_NDJSON_DRAIN_CHUNK_BYTES = 64 * 1024


def iter_ndjson(
    source: BinaryIO,
    *,
    maximum_record_bytes: int,
) -> Iterator[ParseOutcome]:

    if maximum_record_bytes < 0:
        raise ValueError("maximum_record_bytes must not be negative")

    line_number = 0
    read_size = maximum_record_bytes + 2
    drain_size = min(_NDJSON_DRAIN_CHUNK_BYTES, read_size)

    while True:
        line = source.readline(read_size)
        if not line:
            break
        line_number += 1

        has_newline = line.endswith(b"\n")
        record_bytes = line[:-1] if has_newline else line
        if has_newline and record_bytes.endswith(b"\r"):
            record_bytes = record_bytes[:-1]

        if len(record_bytes) > maximum_record_bytes:
            while line and not line.endswith(b"\n"):
                line = source.readline(drain_size)
            yield ParseOutcome(
                line_number=line_number,
                error_code="record_too_large",
            )
            continue

        try:
            text = record_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            yield ParseOutcome(line_number=line_number, error_code="invalid_utf8")
            continue

        if not text.strip():
            yield ParseOutcome(line_number=line_number, skipped_blank=True)
            continue

        try:
            value = json.loads(text)
        except JSONDecodeError:
            yield ParseOutcome(line_number=line_number, error_code="invalid_json")
            continue

        if not isinstance(value, dict):
            yield ParseOutcome(line_number=line_number, error_code="not_object")
            continue

        yield ParseOutcome(line_number=line_number, record=value)


def iter_json_array(
    chunks: Iterable[bytes],
    *,
    maximum_record_bytes: int,
) -> Iterator[object]:

    parser = _IncrementalArrayParser(maximum_record_bytes=maximum_record_bytes)
    for chunk in chunks:
        for record in parser.feed(chunk):
            yield record
    for record in parser.finish():
        yield record


def iter_file_chunks(
    source: BinaryIO,
    *,
    chunk_bytes: int = 64 * 1024,
) -> Iterator[bytes]:
    """Yield bounded byte chunks from a binary file-like object."""
    if chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be positive")

    while chunk := source.read(chunk_bytes):
        yield chunk


class _IncrementalArrayParser:
    """Extract complete elements from one top-level JSON array."""

    def __init__(self, *, maximum_record_bytes: int) -> None:
        if maximum_record_bytes < 0:
            raise ValueError("maximum_record_bytes must not be negative")

        self._max = maximum_record_bytes
        self._buffer = bytearray()
        self._state = "before_array"
        self._position = 0
        self._start: int | None = None
        self._structured = False
        self._depth = 0
        self._in_string = False
        self._escape = False

    def feed(self, chunk: bytes) -> list[object]:
        if not isinstance(chunk, bytes):
            raise TypeError("JSON chunks must be bytes")
        self._buffer.extend(chunk)
        return self._scan()

    def finish(self) -> list[object]:
        records = self._scan()
        if self._state == "before_array":
            raise InvalidJsonError("Expected a top-level JSON array")
        if self._state == "in_element":
            raise InvalidJsonError("Incomplete JSON array element")
        if self._state != "done":
            raise InvalidJsonError("JSON array is missing its closing bracket")
        return records

    def _scan(self) -> list[object]:
        records: list[object] = []
        b = self._buffer
        i = self._position
        n = len(b)

        while i < n:
            ch = b[i]

            if self._state == "before_array":
                if ch in b" \t\r\n":
                    i += 1
                    continue
                if ch != ord(b"["):
                    raise InvalidJsonError("Top-level value must be a JSON array")
                self._state = "first_or_end"
                i += 1
                continue

            if self._state in {"first_or_end", "element_required"}:
                if ch in b" \t\r\n":
                    i += 1
                    continue
                if ch == ord(b"]"):
                    if self._state == "element_required":
                        raise InvalidJsonError("Trailing comma in JSON array")
                    self._state = "done"
                    i += 1
                    continue
                if ch == ord(b","):
                    raise InvalidJsonError("Missing JSON array element")
                self._start = i
                self._structured = ch in b"{["
                self._depth = 1 if self._structured else 0
                self._in_string = ch == ord(b'"')
                self._escape = False
                self._state = "in_element"
                self._check_size(1)
                i += 1
                continue

            if self._state == "separator_or_end":
                if ch in b" \t\r\n":
                    i += 1
                    continue
                if ch == ord(b","):
                    self._state = "element_required"
                    i += 1
                    continue
                if ch == ord(b"]"):
                    self._state = "done"
                    i += 1
                    continue
                raise InvalidJsonError("Expected ',' or ']' after array element")

            if self._state == "done":
                if ch not in b" \t\r\n":
                    raise InvalidJsonError("Unexpected content after JSON array")
                i += 1
                continue

            if self._state != "in_element" or self._start is None:
                raise RuntimeError("Invalid incremental parser state")

            if not self._structured and not self._in_string and ch in b",]":
                self._emit(records, i)
                self._state = "separator_or_end"
                continue

            self._check_size(i - self._start + 1)

            if self._in_string:
                if self._escape:
                    self._escape = False
                elif ch == ord(b"\\"):
                    self._escape = True
                elif ch == ord(b'"'):
                    self._in_string = False
                i += 1
                continue

            if ch == ord(b'"'):
                self._in_string = True
            elif self._structured and ch in b"{[":
                self._depth += 1
            elif self._structured and ch in b"}]":
                self._depth -= 1
                if self._depth == 0:
                    self._emit(records, i + 1)
                    self._state = "separator_or_end"
                    i += 1
                    continue
            i += 1

        self._position = i
        if self._state == "in_element" and self._start is not None:
            self._check_size(i - self._start)
            removed = self._start
            del b[:removed]
            self._start = 0
            self._position -= removed
        else:
            del b[: self._position]
            self._position = 0

        return records

    def _emit(self, records: list[object], end: int) -> None:
        start = self._start
        if start is None:
            raise RuntimeError("Cannot emit an element without a start position")

        record_bytes = bytes(self._buffer[start:end]).strip()
        if not record_bytes:
            raise InvalidJsonError("Empty array element")
        self._check_size(len(record_bytes))
        try:
            records.append(json.loads(record_bytes))
        except (JSONDecodeError, UnicodeDecodeError) as exc:
            raise InvalidJsonError(
                "Invalid JSON array element",
                details={"detail": str(exc)},
            ) from exc
        self._start = None
        self._structured = False
        self._depth = 0
        self._in_string = False
        self._escape = False

    def _check_size(self, size: int) -> None:
        if size > self._max:
            raise RecordTooLargeError(
                f"JSON array element exceeds configured maximum of "
                f"{self._max} bytes",
                details={"record_bytes": size},
            )
