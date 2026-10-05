from fastapi.testclient import TestClient

from app.main import app


def valid_order() -> dict:
    return {
        "order_id": "ORD-1001",
        "customer_email": "user@example.com",
        "amount": "1299.50",
        "currency": "INR",
        "ordered_at": "2026-09-22T10:30:00+05:30",
        "status": "completed",
        "items": [{"sku": "LAPTOP-01", "quantity": 2}],
    }


def test_create_order() -> None:
    with TestClient(app) as client:
        response = client.post("/orders", json=valid_order())
    assert response.status_code == 201
    assert response.json()["order_id"] == "ORD-1001"
    assert response.json()["accepted"] is True


def test_rejects_non_positive_amount() -> None:
    payload = valid_order()
    payload["order_id"] = "ORD-BAD-AMOUNT"
    payload["amount"] = "0"
    with TestClient(app) as client:
        response = client.post("/orders", json=payload)
    assert response.status_code == 422
    errors = response.json()["detail"]
    assert any(error["loc"][-1] == "amount" for error in errors)


def test_missing_order_returns_404() -> None:
    with TestClient(app) as client:
        response = client.get("/orders/DOES-NOT-EXIST")
    assert response.status_code == 404


def test_readiness_endpoint_returns_ready() -> None:
    with TestClient(app) as client:
        response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"
