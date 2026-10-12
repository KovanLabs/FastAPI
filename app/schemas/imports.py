from pydantic import BaseModel


class ImportResponse(BaseModel):
    import_id: str
    checksum: str
    stored_filename: str
    bytes_received: int
    state: str


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
