# Equivalent engines need one observable transformation contract

Day 06 now asks Standard Python, Polars, and DuckDB to perform the same useful
work on clean NDJSON: calculate `item_count` and `total_quantity` from each
order's nested items. The adapters are not considered equivalent merely
because they complete successfully; their serialized output must match the
same Day 06 golden file byte for byte.

The loader recalculates both metrics before storing them. This creates a trust
boundary between processing and persistence and prevents inconsistent derived
values from entering DuckDB. It also showed why edge cases must be shared:
empty items, non-positive quantities, and a sum outside signed 64-bit range now
fail the same way in every engine.

The benchmark reinforced that faster does not mean lower memory. On the current
100,000-record transformation, Polars and DuckDB were faster than the Python
reference, while Python retained the lowest peak RSS. Engine selection should
therefore follow the workload and resource budget, not a general reputation.
