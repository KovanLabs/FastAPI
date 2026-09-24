from fastapi.testclient import TestClient 
from app.main import app 

client = TestClient(app) 

def valid_order() -> dict: 
    return { 
        "order_id": "ORD-1001", 
        "customer_email": "akshay@example.com", 
        "amount": "1299.50", 
        "currency": "INR", 
        "status": "completed", 
        "items": [{"sku": "LAPTOP-01", "quantity": 2}], 
    } 

def test_create_order() -> None: 
    response = client.post("/orders", json=valid_order()) 
    assert response.status_code == 201 
    assert response.json()["order_id"] == "ORD-1001" 
    assert response.json()["accepted"] is True 

def test_rejects_non_positive_amount() -> None: 
    payload = valid_order() 
    payload["order_id"] = "ORD-BAD-AMOUNT" 
    payload["amount"] = "0" 
    response = client.post("/orders", json=payload) 
    assert response.status_code == 422 
    errors = response.json()["detail"] 
    assert any(error["loc"][-1] == "amount" for error in errors) 

def test_missing_order_returns_404() -> None: 
    response = client.get("/orders/DOES-NOT-EXIST") 
    assert response.status_code == 404 