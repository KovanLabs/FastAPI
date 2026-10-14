# Day 06 technical journal

## Decisions and observations

- Kept raw malformed-record recovery in the Day 05 pipeline and gave engines
  only structurally valid canonical NDJSON.
- Removed global sorting by strengthening the staged-input contract to require
  strictly increasing record numbers. This changed the plan from a blocking
  sort to scan, projection, and sink, reducing avoidable memory risk.
- Added a meaningful equivalent transformation in every engine: derive
  top-level `item_count` and `total_quantity` from the nested items. A distinct
  Day 06 golden fixture now describes the enriched output instead of changing
  the Day 05 accepted-record fixture.
- Made empty item lists, non-positive quantities, and signed-64-bit total
  overflow explicit cross-engine failures. The DuckDB loader recalculates the
  derived values before storing them rather than trusting the adapter output.
- Added a small schema upgrade so a database created with the Day 05
  `accepted_orders` shape gains the two Day 06 metric columns.
- Retained Standard Python as the default request engine because it had the
  lowest measured memory for the current enrichment transformation. Polars and
  DuckDB were faster in the current 100,000-record run and remain configurable
  alternatives and regression/benchmark tools.
- Treated streaming as an execution property to inspect and measure, not as a
  constant-memory guarantee.
- Added independent record-count, encoded-byte, record-size, and nested-item
  limits because a row-count limit alone does not bound adversarial batches.
- Kept database transactions independent of parser and engine batch sizes.
- Made retryability part of the batch error contract while leaving actual retry
  ownership to a future durable worker.
- Demonstrated that in-memory status and in-process tasks are lost on restart,
  and that the writer lock solves only same-process concurrency.

## Learner reflection

Add your own explanation of the engine choice, one failure you diagnosed, and
what you would change before production. This section is intentionally not
completed on the learner's behalf.
