from functools import lru_cache
from enum import StrEnum
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProcessingEngine(StrEnum):
    python = "python"
    polars = "polars"
    duckdb = "duckdb"


class LogLevel(StrEnum):
    debug = "DEBUG"
    info = "INFO"
    warning = "WARNING"
    error = "ERROR"
    critical = "CRITICAL"


class Settings(BaseSettings):
    app_name: str = "Vendor Order Import API"
    environment: str = "development"
    upload_directory: Path = Path("data/uploads")
    output_directory: Path = Path("data/output")
    maximum_upload_bytes: int = Field(default=100 * 1024 * 1024, gt=0)
    upload_chunk_bytes: int = Field(default=1024 * 1024, gt=0)
    maximum_record_bytes: int = Field(default=1024 * 1024, gt=0)
    import_batch_size: int = Field(default=5_000, gt=0)
    import_batch_bytes: int = Field(default=16 * 1024 * 1024, gt=0)
    maximum_items_per_order: int = Field(default=1_000, gt=0)
    processing_engine: ProcessingEngine = ProcessingEngine.python
    log_level: LogLevel = LogLevel.info
    log_file_path: Path = Path("logs/application.log")
    log_retention_days: int = Field(default=30, ge=1)
    progress_log_interval: int = Field(default=100_000, ge=100_000)
    otel_exporter_endpoint: str = "http://localhost:4317"
    telemetry_enabled: bool = True
    supported_currencies: frozenset[str] = frozenset({"EUR", "INR", "USD"})
    model_config = SettingsConfigDict(env_file=".env", env_prefix="APP_")

    @model_validator(mode="after")
    def validate_batch_byte_budget(self) -> "Settings":
        if self.import_batch_bytes < self.maximum_record_bytes + 2:
            raise ValueError(
                "import_batch_bytes must be at least maximum_record_bytes plus 2 "
                "bytes for a line ending"
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
