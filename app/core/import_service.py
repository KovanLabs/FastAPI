from __future__ import annotations

import asyncio
import hashlib
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

    try:
        settings.upload_directory.mkdir(parents=True, exist_ok=True)
        with temporary.open("xb") as handle:
            while True:
                chunk = await source.read(settings.upload_chunk_bytes)
                if not chunk:
                    break
                chunk_len = len(chunk)
                if total + chunk_len > settings.maximum_upload_bytes:
                    raise UploadTooLargeError(
                        f"Upload exceeds configured maximum of "
                        f"{settings.maximum_upload_bytes} bytes",
                        details={"maximum_bytes": settings.maximum_upload_bytes},
                    )
                await asyncio.to_thread(handle.write, chunk)
                sha256.update(chunk)
                total += chunk_len
        if total == 0:
            raise EmptyUploadError("Uploaded file must not be empty")
        os.replace(temporary, destination)
    except UploadTooLargeError:
        temporary.unlink(missing_ok=True)
        raise
    except EmptyUploadError:
        temporary.unlink(missing_ok=True)
        raise
    except OSError as exc:
        temporary.unlink(missing_ok=True)
        raise UploadStorageError("Could not store uploaded file") from exc
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise

    return ImportResult(
        import_id=import_id,
        checksum=sha256.hexdigest(),
        stored_filename=filename,
        bytes_received=total,
    )
