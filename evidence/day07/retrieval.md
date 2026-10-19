# Day 07 Retrieval

## Operational questions

The logs must answer when an upload started and completed, when parsing began
and ended, which import and engine performed the work, which batch failed,
whether a retry occurred, whether the database was healthy, and how the import
reached its terminal state.

## Correlation problem

Events from concurrent imports are interleaved by timestamp. A stable
`request_id` follows one HTTP request, while `import_id` follows the complete
job across upload, cleaning, processing, loading, and retries. `engine`,
`batch_number`, and `attempt` provide bounded context at narrower execution
boundaries.

## Safety and volume

Normal logs must exclude source records, customer email addresses, credentials,
tokens, database URLs, and unrestricted exception messages. Large imports emit
progress only when a configured record threshold is crossed rather than once
per record.
