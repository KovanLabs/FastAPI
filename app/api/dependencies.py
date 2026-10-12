from typing import Annotated

from fastapi import Depends

from app.core.config import Settings, get_settings
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
