from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from decimal import Context, Decimal, DecimalException, ROUND_HALF_UP, localcontext
from typing import TypeAlias
from zoneinfo import ZoneInfo


LocationPart: TypeAlias = str | int
_MISSING = object()
_MONEY_SCALE = Decimal("0.01")
_ORDER_TIME_ZONE = ZoneInfo("Asia/Kolkata")


@dataclass(frozen=True)
class CleaningIssue:
    code: str
    location: tuple[LocationPart, ...]
    message: str


@dataclass(frozen=True)
class CleaningResult:
    value: dict[str, object]
    issues: tuple[CleaningIssue, ...]


def clean_order(record: Mapping[str, object]) -> CleaningResult:
    """Return a canonical copy plus every cleaning issue found in one order.

    The file pipeline supplies JSON fractional numbers as ``Decimal`` values,
    preserving their exact value. A direct Python ``float`` input is accepted
    from its shortest decimal representation; its original JSON lexeme cannot
    be recovered after a float has already been created.
    """
    source = deepcopy(dict(record))
    cleaned: dict[str, object] = {}
    issues: list[CleaningIssue] = []

    _clean_text_field(
        source,
        cleaned,
        issues,
        field="order_id",
        transform=str.upper,
        invalid_code="invalid_order_id",
    )
    _clean_text_field(
        source,
        cleaned,
        issues,
        field="customer_email",
        transform=str.lower,
        invalid_code="invalid_email",
    )
    _clean_amount(source, cleaned, issues)
    _clean_text_field(
        source,
        cleaned,
        issues,
        field="currency",
        transform=str.upper,
        invalid_code="invalid_currency",
    )
    _clean_datetime(source, cleaned, issues)
    _clean_text_field(
        source,
        cleaned,
        issues,
        field="status",
        transform=str.lower,
        invalid_code="invalid_status",
    )
    _clean_items(source, cleaned, issues)

    return CleaningResult(value=cleaned, issues=tuple(issues))


def _clean_text_field(
    source: Mapping[str, object],
    cleaned: dict[str, object],
    issues: list[CleaningIssue],
    *,
    field: str,
    transform: Callable[[str], str],
    invalid_code: str,
) -> None:
    raw = source.get(field, _MISSING)
    location = (field,)

    if raw is _MISSING:
        issues.append(_issue("field_missing", location, f"{field} is required."))
    elif raw is None:
        issues.append(_issue("null_not_allowed", location, f"{field} cannot be null."))
    elif not isinstance(raw, str):
        issues.append(_issue(invalid_code, location, f"{field} must be a string."))
    else:
        stripped = raw.strip()
        if not stripped:
            issues.append(
                _issue("blank_not_allowed", location, f"{field} cannot be blank.")
            )
        else:
            cleaned[field] = transform(stripped)


def _clean_amount(
    source: Mapping[str, object],
    cleaned: dict[str, object],
    issues: list[CleaningIssue],
) -> None:
    raw = source.get("amount", _MISSING)
    location = ("amount",)

    if raw is _MISSING:
        issues.append(_issue("field_missing", location, "amount is required."))
        return
    if raw is None:
        issues.append(_issue("null_not_allowed", location, "amount cannot be null."))
        return
    if isinstance(raw, str) and not raw.strip():
        issues.append(_issue("blank_not_allowed", location, "amount cannot be blank."))
        return
    if isinstance(raw, bool) or not isinstance(raw, (Decimal, int, float, str)):
        issues.append(_issue("invalid_amount", location, "amount must be a number."))
        return

    try:
        amount = raw if isinstance(raw, Decimal) else Decimal(str(raw).strip())
        if not amount.is_finite() or amount <= 0:
            raise ValueError
        integer_places = max(amount.adjusted() + 1, 1)
        decimal_context = Context(
            prec=max(28, integer_places + 2),
            rounding=ROUND_HALF_UP,
        )
        with localcontext(decimal_context):
            cleaned["amount"] = amount.quantize(
                _MONEY_SCALE,
                rounding=ROUND_HALF_UP,
            )
    except (DecimalException, ValueError):
        issues.append(
            _issue(
                "invalid_amount",
                location,
                "amount must be a positive finite decimal number.",
            )
        )


def _clean_datetime(
    source: Mapping[str, object],
    cleaned: dict[str, object],
    issues: list[CleaningIssue],
) -> None:
    raw = source.get("ordered_at", _MISSING)
    location = ("ordered_at",)

    if raw is _MISSING:
        issues.append(_issue("field_missing", location, "ordered_at is required."))
        return
    if raw is None:
        issues.append(
            _issue("null_not_allowed", location, "ordered_at cannot be null.")
        )
        return
    if isinstance(raw, str) and not raw.strip():
        issues.append(
            _issue("blank_not_allowed", location, "ordered_at cannot be blank.")
        )
        return

    try:
        if isinstance(raw, datetime):
            parsed = raw
        elif isinstance(raw, str):
            parsed = datetime.fromisoformat(raw.strip())
        else:
            raise ValueError

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_ORDER_TIME_ZONE)
        else:
            parsed = parsed.astimezone(_ORDER_TIME_ZONE)
        cleaned["ordered_at"] = parsed
    except ValueError:
        issues.append(
            _issue("invalid_datetime", location, "ordered_at must be an ISO datetime.")
        )


def _clean_items(
    source: Mapping[str, object],
    cleaned: dict[str, object],
    issues: list[CleaningIssue],
) -> None:
    raw = source.get("items", _MISSING)
    location = ("items",)

    if raw is _MISSING:
        issues.append(_issue("field_missing", location, "items is required."))
        return
    if raw is None:
        issues.append(_issue("null_not_allowed", location, "items cannot be null."))
        return
    if not isinstance(raw, list) or not raw:
        issues.append(
            _issue("invalid_items", location, "items must be a non-empty array.")
        )
        return

    cleaned_items: list[dict[str, object]] = []
    for index, raw_item in enumerate(raw):
        item_location = ("items", index)
        if not isinstance(raw_item, Mapping):
            issues.append(
                _issue("invalid_items", item_location, "Each item must be an object.")
            )
            continue

        item: dict[str, object] = {}
        _clean_item_sku(raw_item, item, issues, index=index)
        _clean_item_quantity(raw_item, item, issues, index=index)
        cleaned_items.append(item)

    cleaned["items"] = cleaned_items


def _clean_item_sku(
    source: Mapping[str, object],
    cleaned: dict[str, object],
    issues: list[CleaningIssue],
    *,
    index: int,
) -> None:
    raw = source.get("sku", _MISSING)
    location = ("items", index, "sku")

    if raw is _MISSING:
        issues.append(_issue("field_missing", location, "sku is required."))
    elif raw is None:
        issues.append(_issue("null_not_allowed", location, "sku cannot be null."))
    elif not isinstance(raw, str):
        issues.append(_issue("invalid_sku", location, "sku must be a string."))
    else:
        stripped = raw.strip()
        if not stripped:
            issues.append(_issue("blank_not_allowed", location, "sku cannot be blank."))
        else:
            cleaned["sku"] = stripped


def _clean_item_quantity(
    source: Mapping[str, object],
    cleaned: dict[str, object],
    issues: list[CleaningIssue],
    *,
    index: int,
) -> None:
    raw = source.get("quantity", _MISSING)
    location = ("items", index, "quantity")

    if raw is _MISSING:
        issues.append(_issue("field_missing", location, "quantity is required."))
    elif raw is None:
        issues.append(
            _issue("null_not_allowed", location, "quantity cannot be null.")
        )
    elif type(raw) is not int or not 1 <= raw <= 10_000:
        issues.append(
            _issue(
                "invalid_quantity",
                location,
                "quantity must be an integer from 1 through 10000.",
            )
        )
    else:
        cleaned["quantity"] = raw


def _issue(
    code: str,
    location: tuple[LocationPart, ...],
    message: str,
) -> CleaningIssue:
    return CleaningIssue(code=code, location=location, message=message)
