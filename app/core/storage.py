from app.core.config import Settings
from app.core.exceptions import StorageDirectoryUnavailableError


def ensure_storage_directories(settings: Settings) -> None:
    for directory in (settings.upload_directory, settings.output_directory):
        directory.mkdir(parents=True, exist_ok=True)
        if not directory.exists() or not directory.is_dir():
            raise StorageDirectoryUnavailableError(str(directory))
        if not (directory.stat().st_mode & 0o222):
            raise StorageDirectoryUnavailableError(str(directory))
