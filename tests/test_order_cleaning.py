
from copy import deepcopy
from decimal import Decimal, localcontext

import pytest

from app.core.order_cleaning import clean_order


@pytest.fixture
def valid_order() -> dict[str, object]:
    return {
        "order_id": "ORD-1",
        "customer_email": "user@example.com",
        "amount": "10.00",
        "currency": "INR",
        "ordered_at": "2026-10-05T10:00:00+05:30",
        "status": "pending",
        "items": [{"sku": "SKU-1", "quantity": 2}],
    }


@pytest.mark.parametrize(
    ("field", "raw_value", "expected"),
    [
        ("order_id", " ord-1 ", "ORD-1"),
        ("customer_email", " USER@EXAMPLE.COM ", "user@example.com"),
        ("amount", " 10.00 ", Decimal("10.00")),
        ("currency", " inr ", "INR"),
        ("status", " PENDING ", "pending"),
    ],
)
def test_clean_order_normalizes_fields(
    valid_order: dict[str, object],
    field: str,
    raw_value: object,
    expected: object,
) -> None:
    valid_order[field] = raw_value

    result = clean_order(valid_order)

    assert result.issues == ()
    assert result.value[field] == expected


@pytest.mark.parametrize(
    ("raw_value", "expected_code"),
    [
        ("ten", "invalid_amount"),
        (True, "invalid_amount"),
        ("NaN", "invalid_amount"),
        ("Infinity", "invalid_amount"),
        ("0", "invalid_amount"),
        ("-1", "invalid_amount"),
    ],
)
def test_clean_order_rejects_invalid_amount(
    valid_order: dict[str, object],
    raw_value: object,
    expected_code: str,
) -> None:
    valid_order["amount"] = raw_value

    result = clean_order(valid_order)

    assert expected_code in {issue.code for issue in result.issues}


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("1", Decimal("1.00")),
        ("1.234", Decimal("1.23")),
        ("1.235", Decimal("1.24")),
        (10, Decimal("10.00")),
    ],
)
def test_amount_uses_two_place_half_up_rounding(
    valid_order: dict[str, object],
    raw_value: object,
    expected: Decimal,
) -> None:
    valid_order["amount"] = raw_value

    result = clean_order(valid_order)

    assert result.issues == ()
    assert result.value["amount"] == expected


@pytest.mark.parametrize("ambient_precision", [2, 3, 4])
def test_amount_rounding_is_independent_of_ambient_decimal_context(
    valid_order: dict[str, object],
    ambient_precision: int,
) -> None:
    valid_order["amount"] = "123.455"

    with localcontext() as context:
        context.prec = ambient_precision
        result = clean_order(valid_order)

    assert result.issues == ()
    assert result.value["amount"] == Decimal("123.46")


@pytest.mark.parametrize(
    ("raw_value", "should_be_valid"),
    [
        (2, True),
        ("2", False),
        (2.0, False),
        (True, False),
    ],
)
def test_quantity_requires_strict_integer(
    valid_order: dict[str, object],
    raw_value: object,
    should_be_valid: bool,
) -> None:
    valid_order["items"][0]["quantity"] = raw_value

    result = clean_order(valid_order)

    quantity_errors = [
        issue for issue in result.issues
        if issue.code == "invalid_quantity"
    ]
    assert bool(quantity_errors) is not should_be_valid


def test_sku_trims_but_preserves_case(
    valid_order: dict[str, object],
) -> None:
    valid_order["items"][0]["sku"] = " sku-AbC "

    result = clean_order(valid_order)

    assert result.value["items"][0]["sku"] == "sku-AbC"


@pytest.mark.parametrize(
    ("raw_datetime", "expected_iso"),
    [
        ("2026-10-05T10:00:00", "2026-10-05T10:00:00+05:30"),
        ("2026-10-05T04:30:00+00:00", "2026-10-05T10:00:00+05:30"),
        ("2026-10-05T10:00:00+05:30", "2026-10-05T10:00:00+05:30"),
    ],
)
def test_datetime_is_normalized_to_asia_kolkata(
    valid_order: dict[str, object],
    raw_datetime: str,
    expected_iso: str,
) -> None:
    valid_order["ordered_at"] = raw_datetime

    result = clean_order(valid_order)

    assert result.value["ordered_at"].isoformat() == expected_iso


def test_clean_order_is_idempotent(
    valid_order: dict[str, object],
) -> None:
    first = clean_order(valid_order)
    second = clean_order(first.value)

    assert first.issues == ()
    assert second.issues == ()
    assert second.value == first.value


def test_clean_order_does_not_mutate_input(
    valid_order: dict[str, object],
) -> None:
    original = deepcopy(valid_order)

    clean_order(valid_order)

    assert valid_order == original


@pytest.mark.parametrize(
    ("condition", "expected_code"),
    [
        ("missing", "field_missing"),
        ("null", "null_not_allowed"),
        ("blank", "blank_not_allowed"),
    ],
)
def test_required_text_states_have_stable_codes(
    valid_order: dict[str, object],
    condition: str,
    expected_code: str,
) -> None:
    if condition == "missing":
        del valid_order["order_id"]
    elif condition == "null":
        valid_order["order_id"] = None
    else:
        valid_order["order_id"] = "   "

    result = clean_order(valid_order)

    assert any(
        issue.code == expected_code and issue.location == ("order_id",)
        for issue in result.issues
    )


def test_clean_order_collects_multiple_nested_issues(
    valid_order: dict[str, object],
) -> None:
    valid_order["items"] = [{"sku": "   ", "quantity": True}]

    result = clean_order(valid_order)

    assert [(issue.code, issue.location) for issue in result.issues] == [
        ("blank_not_allowed", ("items", 0, "sku")),
        ("invalid_quantity", ("items", 0, "quantity")),
    ]


def test_clean_order_omits_extra_fields(
    valid_order: dict[str, object],
) -> None:
    valid_order["top_level_extra"] = "omit me"
    valid_order["items"][0]["item_extra"] = "omit me"  # type: ignore[index]

    result = clean_order(valid_order)

    assert result.issues == ()
    assert "top_level_extra" not in result.value
    assert "item_extra" not in result.value["items"][0]  # type: ignore[index]


@pytest.mark.parametrize(
    ("field", "condition", "expected_code"),
    [
        ("order_id", "missing", "field_missing"),
        ("order_id", None, "null_not_allowed"),
        ("order_id", "   ", "blank_not_allowed"),
        ("customer_email", "missing", "field_missing"),
        ("customer_email", None, "null_not_allowed"),
        ("customer_email", "   ", "blank_not_allowed"),
        ("amount", "missing", "field_missing"),
        ("amount", None, "null_not_allowed"),
        ("amount", "   ", "blank_not_allowed"),
        ("currency", "missing", "field_missing"),
        ("currency", None, "null_not_allowed"),
        ("currency", "   ", "blank_not_allowed"),
        ("ordered_at", "missing", "field_missing"),
        ("ordered_at", None, "null_not_allowed"),
        ("ordered_at", "   ", "blank_not_allowed"),
        ("status", "missing", "field_missing"),
        ("status", None, "null_not_allowed"),
        ("status", "   ", "blank_not_allowed"),
        ("items", "missing", "field_missing"),
        ("items", None, "null_not_allowed"),
        ("items", [], "invalid_items"),
        ("items", {}, "invalid_items"),
    ],
)
def test_top_level_rejection_catalogue_has_stable_codes(
    valid_order: dict[str, object],
    field: str,
    condition: object,
    expected_code: str,
) -> None:
    if condition == "missing":
        valid_order.pop(field)
    else:
        valid_order[field] = condition

    result = clean_order(valid_order)

    assert any(
        issue.code == expected_code and issue.location == (field,)
        for issue in result.issues
    )


@pytest.mark.parametrize(
    ("field", "condition", "expected_code"),
    [
        ("sku", "missing", "field_missing"),
        ("sku", None, "null_not_allowed"),
        ("sku", "   ", "blank_not_allowed"),
        ("quantity", "missing", "field_missing"),
        ("quantity", None, "null_not_allowed"),
        ("quantity", 0, "invalid_quantity"),
        ("quantity", 10_001, "invalid_quantity"),
        ("quantity", "2", "invalid_quantity"),
        ("quantity", 2.0, "invalid_quantity"),
        ("quantity", True, "invalid_quantity"),
    ],
)
def test_nested_rejection_catalogue_has_stable_codes(
    valid_order: dict[str, object],
    field: str,
    condition: object,
    expected_code: str,
) -> None:
    item = valid_order["items"][0]  # type: ignore[index]
    if condition == "missing":
        item.pop(field)
    else:
        item[field] = condition

    result = clean_order(valid_order)

    assert any(
        issue.code == expected_code
        and issue.location == ("items", 0, field)
        for issue in result.issues
    )
