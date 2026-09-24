from decimal import Decimal 
from enum import StrEnum   

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, EmailStr, Field 
 
app = FastAPI(title="Vendor Order Import API") 

class OrderStatus(StrEnum): 
    pending = "pending" 
    completed = "completed" 
    cancelled = "cancelled" 


class OrderItem(BaseModel): 
    sku: str = Field(min_length=1, max_length=80) 
    quantity: int = Field(gt=0, le=10_000) 


class OrderCreate(BaseModel): 
    order_id: str = Field(min_length=1, max_length=100) 
    customer_email: EmailStr 
    amount: Decimal = Field(gt=0) 
    currency: str = Field(min_length=3, max_length=3) 
    status: OrderStatus 
    items: list[OrderItem] = Field(min_length=1) 


class ShippingAddress(BaseModel):
    street: str = Field(min_length=1, max_length=200)
    city: str = Field(min_length=1, max_length=80)
    postal_code: str = Field(min_length=3, max_length=12)
    country: str = Field(min_length=2, max_length=2)


class OrderResponse(OrderCreate): 
    accepted: bool = True 
ORDERS: dict[str, OrderResponse] = {} 


class CheckoutOrderRequest(BaseModel):
    customer_email: EmailStr
    currency: str = Field(min_length=3, max_length=3)
    shipping_address: ShippingAddress
    items: list[OrderItem] = Field(min_length=1)


@app.get("/health/live") 
def liveness() -> dict[str, str]: 
    return {"status": "ok"} 

@app.post( 
    "/orders", 
    response_model=OrderResponse, 
    status_code=status.HTTP_201_CREATED, 
) 
def create_order(order: OrderCreate) -> OrderResponse: 
    if order.order_id in ORDERS: 
        raise HTTPException(status_code=409, detail="Order already exists") 
    response = OrderResponse(**order.model_dump()) 
    ORDERS[order.order_id] = response 
    return response 

@app.get("/orders/{order_id}", response_model=OrderResponse) 
def get_order(order_id: str) -> OrderResponse: 
    order = ORDERS.get(order_id) 
    if order is None: 
        raise HTTPException(status_code=404, detail="Order not found") 
    return order 

@app.get("/orders", response_model=list[OrderResponse]) 
def list_orders( 
    order_status: OrderStatus | None = Query(default=None, alias="status"), 
) -> list[OrderResponse]: 

    values = list(ORDERS.values()) 
    if order_status is None: 
        return values 
    return [order for order in values if order.status == order_status] 

@app.post(
    "/orders/checkout",
    response_model=CheckoutOrderRequest,
    status_code=status.HTTP_201_CREATED,)
def create_checkout_order(payload: CheckoutOrderRequest):
    return payload