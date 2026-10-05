import json

from scripts.generate_orders_json import make_order


def test_generated_order_has_expected_nested_shape() -> None:
    order = make_order(1)

    assert set(order) == {
        "order_id",
        "customer_email",
        "amount",
        "currency",
        "ordered_at",
        "status",
        "items",
    }
    assert isinstance(order["items"], list)
    assert {"sku", "quantity"} <= set(order["items"][0])


def test_cleaning_anomalies_remain_valid_json_values() -> None:
    for index in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9):
        order = make_order(index)
        decoded = json.loads(json.dumps(order))
        assert decoded == order

    assert make_order(0)["customer_email"] == " NOT-AN-EMAIL "
    assert make_order(1)["amount"] == "N/A"
    assert make_order(2)["amount"] < 0
    assert make_order(3)["currency"] == " inr "
    assert make_order(4)["status"] == "PENDING"
    assert make_order(5)["items"][0]["sku"] == " "
    assert make_order(6)["items"][0]["quantity"] == 0
    assert make_order(7)["customer_email"] == ""
    assert make_order(8)["customer_email"] is None
    assert make_order(9)["order_id"] == make_order(8)["order_id"]
