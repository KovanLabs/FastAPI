"""Generate a deterministic, Polars-cleaning-friendly JSON order fixture.

The output is one valid top-level JSON array of order objects. Sparse rows have
syntactically valid but semantically messy values so they can be cleaned later
without making the complete JSON document unparsable.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TextIO


def make_order(index: int) -> dict[str, object]:
    """Return one deterministic order; ``index`` is zero-based."""
    sequence = index + 1
    order_id = f"ORD-{sequence:08d}"
    email = f"customer{sequence:08d}@example.com"
    amount: int | float | str = ((sequence * 379) % 250_000 + 100) / 100
    currency = ("INR", "USD", "EUR")[sequence % 3]
    status = ("pending", "completed", "cancelled")[sequence % 3]
    item_count = 1 + int(sequence % 5 == 0)
    items: list[dict[str, object]] = [
        {
            "sku": f"SKU-{(sequence + item_number) % 100_000:06d}",
            "quantity": (sequence + item_number) % 5 + 1,
        }
        for item_number in range(item_count)
    ]

    # Every 10,000th row has one controlled semantic quality issue. Keep the
    # JSON itself valid so an incremental parser can still reach later rows.
    anomaly = sequence % 10_000
    if anomaly == 1:
        email = " NOT-AN-EMAIL "
    elif anomaly == 2:
        amount = "N/A"
    elif anomaly == 3:
        amount = -abs(float(amount))
    elif anomaly == 4:
        currency = " inr "
    elif anomaly == 5:
        status = "PENDING"
    elif anomaly == 6:
        items[0]["sku"] = " "
    elif anomaly == 7:
        items[0]["quantity"] = 0
    elif anomaly == 8:
        email = ""
    elif anomaly == 9:
        email = None  # type: ignore[assignment]
    elif anomaly == 10:
        order_id = f"ORD-{sequence - 1:08d}"  # duplicate previous ID

    day_of_month = (sequence - 1) % 28 + 1
    hour = (sequence // 3) % 24
    minute = (sequence // 7) % 60
    second = (sequence // 11) % 60
    ordered_at = (
        f"2026-09-{day_of_month:02d}T{hour:02d}:{minute:02d}:"
        f"{second:02d}.000Z"
    )

    return {
        "order_id": order_id,
        "customer_email": email,
        "amount": amount,
        "currency": currency,
        "ordered_at": ordered_at,
        "status": status,
        "items": items,
    }


def write_orders(output: TextIO, count: int) -> None:
    output.write("[\n")
    for index in range(count):
        if index:
            output.write(",\n")
        output.write(
            json.dumps(
                make_order(index),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        if (index + 1) % 1_000_000 == 0:
            print(f"Generated {index + 1:,} / {count:,} records")
    output.write("\n]\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=10_000_000)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/samples/orders_10m.json"),
    )
    args = parser.parse_args()
    if args.records < 0:
        parser.error("--records must not be negative")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", buffering=1024 * 1024) as output:
        write_orders(output, args.records)

    print(
        f"Wrote {args.records:,} JSON order records to {args.output} "
        f"({args.output.stat().st_size:,} bytes)"
    )


if __name__ == "__main__":
    main()
