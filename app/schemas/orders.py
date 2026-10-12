from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, EmailStr, Field, StringConstraints


MAX_ITEMS_PER_ORDER = 1_000


class OrderStatus(StrEnum):
    pending = "pending"
    completed = "completed"
    cancelled = "cancelled"


Sku = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=80),
]


class OrderItem(BaseModel):
    sku: Sku
    quantity: int = Field(gt=0, le=10_000)


class OrderCreate(BaseModel):
    order_id: str = Field(min_length=1, max_length=100)
    customer_email: EmailStr
    amount: Decimal = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    ordered_at: datetime
    status: OrderStatus
    items: list[OrderItem] = Field(
        min_length=1,
        max_length=MAX_ITEMS_PER_ORDER,
    )


class ShippingAddress(BaseModel):
    street: str = Field(min_length=1, max_length=200)
    city: str = Field(min_length=1, max_length=80)
    postal_code: str = Field(min_length=3, max_length=12)
    country: str = Field(min_length=2, max_length=2)


class OrderResponse(OrderCreate):
    accepted: bool = True


class CheckoutOrderRequest(BaseModel):
    customer_email: EmailStr
    currency: str = Field(min_length=3, max_length=3)
    shipping_address: ShippingAddress
    items: list[OrderItem] = Field(
        min_length=1,
        max_length=MAX_ITEMS_PER_ORDER,
    )
