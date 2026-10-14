from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import TextIO


def make_canonical_envelope(record_number: int) -> dict[str, object]:
    sequence = record_number
    return {
        "record_number": sequence,
        "order": {
            "order_id": f"ORD-{sequence:08d}",
            "customer_email": f"customer{sequence:08d}@example.com",
            "amount": f"{((sequence * 379) % 250_000 + 100) / 100:.2f}",
            "currency": ("INR", "USD", "EUR")[sequence % 3],
            "ordered_at": "2026-10-06T09:00:00+05:30",
            "status": ("pending", "completed", "cancelled")[sequence % 3],
            "items": [
                {
                    "sku": f"SKU-{sequence % 100_000:06d}",
                    "quantity": sequence % 5 + 1,
                }
            ],
        },
    }


def write_canonical_orders(output: TextIO, count: int) -> None:
    for record_number in range(1, count + 1):
        output.write(
            json.dumps(
                make_canonical_envelope(record_number),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        output.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.records < 0:
        parser.error("--records must not be negative")
    if args.output.exists():
        parser.error(f"output already exists: {args.output}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8", newline="\n") as output:
        write_canonical_orders(output, args.records)

    print(
        f"Wrote {args.records:,} canonical records to {args.output} "
        f"({args.output.stat().st_size:,} bytes)"
    )


if __name__ == "__main__":
    main()
