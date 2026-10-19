# An ordered-input contract can remove a global sort

The learner changed the Day 6 adapter contract test-first: canonical Day 5
records must have strictly increasing `record_number` values, while duplicates
and decreasing values fail closed. They then replaced the Standard Python
adapter's retained list and sort with an incremental generator and confirmed
all 29 adapter tests pass; future lessons can build on the distinction between
repairing unordered input and validating an upstream ordering guarantee.
