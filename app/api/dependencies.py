from typing import Annotated
from _thread import LockType

from fastapi import Depends, Request

from app.core.config import Settings, get_settings
from app.core.import_jobs import ImportJobRepository
from app.services.order_validation import OrderValidationService


SettingsDependency = Annotated[Settings, Depends(get_settings)]


def get_order_validation_service(
    settings: SettingsDependency,
) -> OrderValidationService:
    return OrderValidationService(settings.supported_currencies)


OrderValidationServiceDependency = Annotated[
    OrderValidationService,
    Depends(get_order_validation_service),
]


def get_import_job_repository(request: Request) -> ImportJobRepository:
    return request.app.state.import_job_repository


ImportJobRepositoryDependency = Annotated[
    ImportJobRepository,
    Depends(get_import_job_repository),
]


def get_import_writer_lock(request: Request) -> LockType:
    return request.app.state.import_writer_lock


ImportWriterLockDependency = Annotated[
    LockType,
    Depends(get_import_writer_lock),
]
