import pytest
from fastapi.testclient import TestClient

from app.api.dependencies import get_order_validation_service
from app.main import app
from app.schemas.orders import OrderCreate, OrderResponse
from app.services.order_validation import OrderValidationService


def valid_order() -> dict:
    return {
        "order_id": "ORD-2001",
        "customer_email": "buyer@example.com",
        "amount": "1299.50",
        "currency": "INR",
        "ordered_at": "2026-09-22T10:30:00+05:30",
        "status": "completed",
        "items": [{"sku": "LAPTOP-01", "quantity": 2}],
    }


def test_validate_order_returns_declared_contract() -> None:
    with TestClient(app) as client:
        response = client.post("/orders/validate", json=valid_order())

    assert response.status_code == 200
    assert response.json() == {
        **valid_order(),
        "amount": "1299.50",
        "ordered_at": "2026-09-22T10:30:00+05:30",
        "accepted": True,
    }


def test_validate_order_returns_stable_domain_error() -> None:
    payload = valid_order()
    payload["currency"] = "JPY"

    with TestClient(app) as client:
        response = client.post(
            "/orders/validate",
            json=payload,
            headers={"X-Request-ID": "req-domain-test"},
        )

    assert response.status_code == 422
    assert response.json() == {
        "error": {
            "code": "unsupported_currency",
            "message": "Currency JPY is not supported",
            "details": {"currency": "JPY"},
        },
        "request_id": "req-domain-test",
    }


def test_order_validation_service_is_callable_without_fastapi() -> None:
    service = OrderValidationService({"INR"})
    order = OrderCreate.model_validate(valid_order())

    result = service.validate(order)

    assert result.order_id == "ORD-2001"
    assert result.accepted is True


def test_validate_order_dependency_can_be_overridden() -> None:
    class FakeOrderValidationService:
        def validate(self, order: OrderCreate) -> OrderResponse:
            return OrderResponse(**order.model_dump(), accepted=False)

    app.dependency_overrides[get_order_validation_service] = (
        lambda: FakeOrderValidationService()
    )
    try:
        with TestClient(app) as client:
            response = client.post("/orders/validate", json=valid_order())
    finally:
        app.dependency_overrides.pop(get_order_validation_service, None)

    assert response.status_code == 200
    assert response.json()["accepted"] is False


def test_response_model_filters_internal_service_data() -> None:
    class FakeOrderValidationService:
        def validate(self, order: OrderCreate) -> dict:
            return {
                **order.model_dump(),
                "accepted": True,
                "internal_note": "must not cross the HTTP boundary",
            }

    app.dependency_overrides[get_order_validation_service] = (
        lambda: FakeOrderValidationService()
    )
    try:
        with TestClient(app) as client:
            response = client.post("/orders/validate", json=valid_order())
    finally:
        app.dependency_overrides.pop(get_order_validation_service, None)

    assert response.status_code == 200
    assert "internal_note" not in response.json()


@pytest.mark.parametrize(
    ("change", "expected_field"),
    [
        ({"customer_email": None}, "customer_email"),
        ({"customer_email": "not-an-email"}, "customer_email"),
        ({"items": []}, "items"),
        ({"items": [{"sku": "", "quantity": 1}]}, "sku"),
        ({"items": [{"sku": "SKU-1", "quantity": "many"}]}, "quantity"),
    ],
)
def test_validate_order_rejects_invalid_request_data(
    change: dict,
    expected_field: str,
) -> None:
    payload = valid_order()
    payload.update(change)

    with TestClient(app) as client:
        response = client.post("/orders/validate", json=payload)

    assert response.status_code == 422
    assert any(
        error["loc"][-1] == expected_field
        for error in response.json()["detail"]
    )


def test_request_id_header_is_read_and_returned() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/orders/validate",
            json=valid_order(),
            headers={"X-Request-ID": "req-location-test"},
        )

    assert response.headers["X-Request-ID"] == "req-location-test"


def test_missing_order_uses_stable_application_error() -> None:
    with TestClient(app) as client:
        response = client.get(
            "/orders/UNKNOWN",
            headers={"X-Request-ID": "req-missing-order"},
        )

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "order_not_found",
            "message": "Order UNKNOWN was not found",
            "details": {"order_id": "UNKNOWN"},
        },
        "request_id": "req-missing-order",
    }


def test_validate_order_rejects_missing_field() -> None:
    payload = valid_order()
    del payload["order_id"]

    with TestClient(app) as client:
        response = client.post("/orders/validate", json=payload)

    assert response.status_code == 422
    assert any(
        error["loc"][-1] == "order_id"
        for error in response.json()["detail"]
    )


def test_list_orders_reads_status_from_query_string() -> None:
    pending = valid_order()
    pending["order_id"] = "ORD-QUERY-PENDING"
    pending["status"] = "pending"
    completed = valid_order()
    completed["order_id"] = "ORD-QUERY-COMPLETED"

    with TestClient(app) as client:
        assert client.post("/orders", json=pending).status_code == 201
        assert client.post("/orders", json=completed).status_code == 201
        response = client.get("/orders", params={"status": "pending"})

    assert response.status_code == 200
    returned_ids = {order["order_id"] for order in response.json()}
    assert "ORD-QUERY-PENDING" in returned_ids
    assert "ORD-QUERY-COMPLETED" not in returned_ids


def test_duplicate_order_uses_stable_application_error() -> None:
    payload = valid_order()
    payload["order_id"] = "ORD-DUPLICATE"

    with TestClient(app) as client:
        assert client.post("/orders", json=payload).status_code == 201
        response = client.post(
            "/orders",
            json=payload,
            headers={"X-Request-ID": "req-duplicate-order"},
        )

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "order_already_exists",
            "message": "Order ORD-DUPLICATE already exists",
            "details": {"order_id": "ORD-DUPLICATE"},
        },
        "request_id": "req-duplicate-order",
    }


def test_openapi_contains_health_and_validation_routes() -> None:
    with TestClient(app) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 200
    paths = response.json()["paths"]
    assert "/health/live" in paths
    assert "/orders/validate" in paths
