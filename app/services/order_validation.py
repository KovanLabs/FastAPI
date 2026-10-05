from collections.abc import Iterable

from app.core.exceptions import UnsupportedCurrencyError
from app.schemas.orders import OrderCreate, OrderResponse


class OrderValidationService:
    def __init__(self, supported_currencies: Iterable[str]) -> None:
        self._supported_currencies = frozenset(supported_currencies)

    def validate(self, order: OrderCreate) -> OrderResponse:
        if order.currency not in self._supported_currencies:
            raise UnsupportedCurrencyError(order.currency)
        return OrderResponse(**order.model_dump())
