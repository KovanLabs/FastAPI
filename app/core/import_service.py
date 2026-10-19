from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from app.core.config import Settings
from app.core.exceptions import (
    EmptyUploadError,
    UploadStorageError,
    UploadTooLargeError,
)
from app.core.log_context import bind_log_context


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ImportResult:
    import_id: str
    checksum: str
    stored_filename: str
    bytes_received: int


class AsyncReadable(Protocol):
    async def read(self, size: int = -1) -> bytes: ...


async def store_upload(
    source: AsyncReadable,
    settings: Settings,
    *,
    suffix: str,
) -> ImportResult:
    import_id = uuid.uuid4().hex
    filename = f"{import_id}{suffix}"
    destination = settings.upload_directory / filename
    temporary = settings.upload_directory / f".{import_id}.part"
    sha256 = hashlib.sha256()
    total = 0
    observed_bytes = 0

    with bind_log_context(import_id=import_id):
        logger.info(
            "Upload storage started",
            extra={
                "event": "upload.started",
                "maximum_bytes": settings.maximum_upload_bytes,
                "upload_chunk_bytes": settings.upload_chunk_bytes,
            },
        )
        try:
            settings.upload_directory.mkdir(parents=True, exist_ok=True)
            with temporary.open("xb") as handle:
                while True:
                    chunk = await source.read(settings.upload_chunk_bytes)
                    if not chunk:
                        break
                    chunk_len = len(chunk)
                    observed_bytes = total + chunk_len
                    if total + chunk_len > settings.maximum_upload_bytes:
                        raise UploadTooLargeError(
                            f"Upload exceeds configured maximum of "
                            f"{settings.maximum_upload_bytes} bytes",
                            details={
                                "maximum_bytes": settings.maximum_upload_bytes
                            },
                        )
                    await asyncio.to_thread(handle.write, chunk)
                    sha256.update(chunk)
                    total += chunk_len
            if total == 0:
                raise EmptyUploadError("Uploaded file must not be empty")
            os.replace(temporary, destination)
        except UploadTooLargeError:
            _discard_partial_upload(temporary)
            logger.warning(
                "Upload rejected because it exceeded the configured limit",
                extra={
                    "event": "upload.rejected",
                    "reason": "maximum_upload_size_exceeded",
                    "maximum_bytes": settings.maximum_upload_bytes,
                    "bytes_received": observed_bytes,
                },
            )
            raise
        except EmptyUploadError:
            _discard_partial_upload(temporary)
            logger.warning(
                "Upload rejected because it was empty",
                extra={
                    "event": "upload.rejected",
                    "reason": "empty_upload",
                    "bytes_received": 0,
                },
            )
            raise
        except OSError as exc:
            _discard_partial_upload(temporary)
            logger.exception(
                "Upload storage failed",
                extra={
                    "event": "upload.failed",
                    "reason": "storage_error",
                    "bytes_received": total,
                },
            )
            raise UploadStorageError("Could not store uploaded file") from exc
        except BaseException:
            _discard_partial_upload(temporary)
            logger.exception(
                "Upload failed",
                extra={
                    "event": "upload.failed",
                    "reason": "unexpected_error",
                    "bytes_received": total,
                },
            )
            raise

        checksum = sha256.hexdigest()
        logger.info(
            "Upload stored",
            extra={
                "event": "upload.completed",
                "bytes_received": total,
                "checksum_sha256": checksum,
            },
        )

    return ImportResult(
        import_id=import_id,
        checksum=checksum,
        stored_filename=filename,
        bytes_received=total,
    )


def _discard_partial_upload(temporary: Path) -> None:
    """Remove a partial upload without hiding the original failure."""

    try:
        temporary.unlink(missing_ok=True)
    except (FileNotFoundError, NotADirectoryError):
        return
    except OSError:
        logger.exception(
            "Partial upload cleanup failed",
            extra={
                "event": "upload.cleanup_failed",
                "reason": "cleanup_error",
            },
        )
