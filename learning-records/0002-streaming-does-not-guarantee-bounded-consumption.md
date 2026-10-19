# Streaming does not guarantee bounded consumption

## Context

Day 4 compared `json.load()`, a custom incremental JSON-array iterator, and
`ijson.items()` on the same generated order data.

## Learning

An incremental parser can yield one record at a time, but its caller controls
whether memory stays bounded. Iterating and releasing each record kept peak RSS
low in the focused comparison. Calling `list(...)` retained every record and
used memory comparable to a full `json.load()`.

## Consequence

Future validation and Polars cleaning must consume records incrementally or in
explicitly bounded batches. Avoid `list(parser)` for production-sized imports.

## Revisit when

The project chooses its Polars batch size or moves parsing into a background
worker with a defined memory budget.
