from datetime import datetime

from pydantic import BaseModel

from app.core.import_jobs import ImportState


class ImportResponse(BaseModel):
    import_id: str
    checksum: str
    stored_filename: str
    bytes_received: int
    state: str


class ImportStageResponse(BaseModel):
    import_id: str
    state: ImportState


class ImportStatusResponse(BaseModel):
    import_id: str
    state: ImportState
    bytes_received: int
    total_records: int
    accepted_records: int
    rejected_records: int
    created_at: datetime
    updated_at: datetime
    failure_code: str | None
    failure_message: str | None
    failed_stage: str | None
    stage_attempt: int
    processing_engine: str | None


class ImportSummaryResponse(BaseModel):
    import_id: str
    state: ImportState
    total_records: int
    accepted_records: int
    rejected_records: int
    failures_by_code: dict[str, int]


class ImportErrorsResponse(BaseModel):
    import_id: str
    total_errors: int
    returned_errors: int
    errors: list[dict[str, object]]


class NdjsonRecordFailure(BaseModel):
    line_number: int
    error_code: str


class NdjsonValidationResponse(BaseModel):
    import_id: str
    checksum: str
    stored_filename: str
    bytes_received: int
    state: str
    valid_records: int
    skipped_blank: int
    failures_by_code: dict[str, int]
    failure_count: int
    failure_samples: list[NdjsonRecordFailure]


class JsonArrayRecordFailure(BaseModel):
    record_number: int
    error_code: str


class JsonArrayValidationResponse(BaseModel):
    import_id: str
    checksum: str
    stored_filename: str
    bytes_received: int
    state: str
    valid_records: int
    failures_by_code: dict[str, int]
    failure_count: int
    failure_samples: list[JsonArrayRecordFailure]
