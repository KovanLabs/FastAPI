from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "Vendor Order Import API"
    environment: str = "development"
    upload_directory: Path = Path("data/uploads")
    output_directory: Path = Path("data/output")
    maximum_upload_bytes: int = 100 * 1024 * 1024
    upload_chunk_bytes: int = 1024 * 1024
    maximum_record_bytes: int = 1024 * 1024
    import_batch_size: int = 5_000
    progress_log_interval: int = 10_000
    otel_exporter_endpoint: str = "http://localhost:4317"
    telemetry_enabled: bool = True
    supported_currencies: frozenset[str] = frozenset({"EUR", "INR", "USD"})
    model_config = SettingsConfigDict(env_file=".env", env_prefix="APP_")


@lru_cache
def get_settings() -> Settings:
    return Settings()
