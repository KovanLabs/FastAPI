"""Generate a deterministic CSV fixture equivalent to the JSON fixture."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import TextIO

from scripts.generate_orders_json import make_order


CSV_COLUMNS = (
    "order_id",
    "customer_email",
    "amount",
    "currency",
    "ordered_at",
    "status",
    "items",
)
CSV_NULL = r"\N"


def write_orders_csv(output: TextIO, count: int) -> None:
    writer = csv.DictWriter(
        output,
        fieldnames=CSV_COLUMNS,
        lineterminator="\n",
    )
    writer.writeheader()
    for index in range(count):
        order = make_order(index)
        writer.writerow(
            {
                field: (
                    CSV_NULL
                    if order[field] is None
                    else json.dumps(
                        order[field],
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    if field == "items"
                    else order[field]
                )
                for field in CSV_COLUMNS
            }
        )
        if (index + 1) % 1_000_000 == 0:
            print(f"Generated {index + 1:,} / {count:,} records")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=10_000_000)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/samples/orders_10m.csv"),
    )
    args = parser.parse_args()
    if args.records < 0:
        parser.error("--records must not be negative")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open(
        "w",
        encoding="utf-8",
        newline="",
        buffering=1024 * 1024,
    ) as output:
        write_orders_csv(output, args.records)

    print(
        f"Wrote {args.records:,} CSV order records to {args.output} "
        f"({args.output.stat().st_size:,} bytes)"
    )


if __name__ == "__main__":
    main()
