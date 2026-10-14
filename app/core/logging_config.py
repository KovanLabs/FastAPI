"""Central structured logging, correlation, redaction, and file rotation."""

from __future__ import annotations

import json
import logging
import re
import sys
import traceback
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import TextIO
from zoneinfo import ZoneInfo

from app.core.log_context import current_log_context


_LOG_TIMEZONE = ZoneInfo("Asia/Kolkata")
_JSON_CONSOLE_HANDLER_NAME = "application-json-console"
_JSON_FILE_HANDLER_NAME = "application-json-file"
_SAFE_EVENT_FIELDS = (
    "environment",
    "configured_log_level",
    "request_id",
    "import_id",
    "engine",
    "batch_number",
    "attempt",
    "http_method",
    "status_code",
    "duration_ms",
    "records_processed",
    "total_records",
    "accepted_records",
    "rejected_records",
    "batches_committed",
    "batches_skipped",
    "import_state",
    "retryable",
)


class CorrelationFilter(logging.Filter):
    """Copy task-local correlation values onto each log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        for field, value in current_log_context().items():
            if not hasattr(record, field):
                setattr(record, field, value)
        return True


_CORRELATION_FILTER = CorrelationFilter()

_SENSITIVE_ASSIGNMENT = re.compile(
    r'''(?ix)
    (?P<label>
        (?<![\w-])["']?
        (?:password|passwd|secret|token|api[_-]?key|database[_-]?url|db[_-]?url)
        ["']?\s*[:=]\s*
    )
    (?P<value>
        "(?:\\.|[^"\\])*"
        | '(?:\\.|[^'\\])*'
        | [^\s,;}]+
    )
    ''',
)
_AUTHORIZATION_ASSIGNMENT = re.compile(
    r'''(?ix)
    (?P<label>(?<![\w-])["']?authorization["']?\s*[:=]\s*)
    (?P<value>
        "(?:\\.|[^"\\])*"
        | '(?:\\.|[^'\\])*'
        | [^\r\n,;}]+
    )
    ''',
)
_DATABASE_URL = re.compile(
    r"(?i)\b(?:postgres(?:ql)?|mysql|mariadb|mongodb(?:\+srv)?|redis|"
    r"sqlite|duckdb)://[^\s]+"
)
_EMAIL_ADDRESS = re.compile(
    r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b"
)
_BEARER_TOKEN = re.compile(r"(?i)\bBearer\s+[^\s,;]+")


def _redact_message(message: str) -> str:
    message = _AUTHORIZATION_ASSIGNMENT.sub(
        lambda match: f"{match.group('label')}[REDACTED]",
        message,
    )
    message = _SENSITIVE_ASSIGNMENT.sub(
        lambda match: f"{match.group('label')}[REDACTED]",
        message,
    )
    message = _DATABASE_URL.sub("[REDACTED_DATABASE_URL]", message)
    message = _BEARER_TOKEN.sub("Bearer [REDACTED]", message)
    return _EMAIL_ADDRESS.sub("[REDACTED_EMAIL]", message)


class JsonFormatter(logging.Formatter):
    """Serialize one protected ``LogRecord`` as one compact JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(
            record.created,
            tz=_LOG_TIMEZONE,
        ).isoformat(timespec="milliseconds")
        payload: dict[str, object] = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "event": getattr(record, "event", "log.message"),
            "message": _redact_message(record.getMessage()),
        }
        for field in _SAFE_EVENT_FIELDS:
            if hasattr(record, field):
                payload[field] = getattr(record, field)
        if record.exc_info is not None:
            exception_type, _, exception_traceback = record.exc_info
            if exception_type is not None and exception_traceback is not None:
                payload["exception"] = {
                    "type": exception_type.__name__,
                    "frames": [
                        {
                            "file": Path(frame.filename).name,
                            "line": frame.lineno,
                            "function": frame.name,
                        }
                        for frame in traceback.extract_tb(exception_traceback)
                    ],
                }
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def configure_logging(
    level: str,
    *,
    stream: TextIO | None = None,
    file_path: Path | None = None,
    file_backup_count: int = 30,
) -> None:
    """Configure JSON console output and optional daily file rotation."""

    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"unsupported log level: {level}")

    application_logger = logging.getLogger("app")
    application_logger.setLevel(numeric_level)
    application_logger.propagate = False

    target_stream = stream if stream is not None else sys.stdout
    handler = next(
        (
            candidate
            for candidate in application_logger.handlers
            if candidate.get_name() == _JSON_CONSOLE_HANDLER_NAME
        ),
        None,
    )
    for candidate in tuple(application_logger.handlers):
        is_console_handler = isinstance(
            candidate,
            logging.StreamHandler,
        ) and not isinstance(candidate, logging.FileHandler)
        if is_console_handler and candidate is not handler:
            application_logger.removeHandler(candidate)
            candidate.close()

    if handler is None:
        handler = logging.StreamHandler(target_stream)
        handler.set_name(_JSON_CONSOLE_HANDLER_NAME)
        application_logger.addHandler(handler)
    elif isinstance(handler, logging.StreamHandler):
        handler.setStream(target_stream)

    handler.setLevel(numeric_level)
    handler.setFormatter(JsonFormatter())
    if _CORRELATION_FILTER not in handler.filters:
        handler.addFilter(_CORRELATION_FILTER)

    file_handler = next(
        (
            candidate
            for candidate in application_logger.handlers
            if candidate.get_name() == _JSON_FILE_HANDLER_NAME
        ),
        None,
    )
    if file_path is None:
        if file_handler is not None:
            application_logger.removeHandler(file_handler)
            file_handler.close()
        return

    file_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_file_path = file_path.resolve()
    if not isinstance(file_handler, TimedRotatingFileHandler) or (
        Path(file_handler.baseFilename) != resolved_file_path
    ):
        if file_handler is not None:
            application_logger.removeHandler(file_handler)
            file_handler.close()
        file_handler = TimedRotatingFileHandler(
            resolved_file_path,
            when="midnight",
            interval=1,
            backupCount=file_backup_count,
            encoding="utf-8",
            delay=True,
            utc=False,
        )
        file_handler.set_name(_JSON_FILE_HANDLER_NAME)
        application_logger.addHandler(file_handler)
    else:
        file_handler.backupCount = file_backup_count

    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(JsonFormatter())
    if _CORRELATION_FILTER not in file_handler.filters:
        file_handler.addFilter(_CORRELATION_FILTER)
