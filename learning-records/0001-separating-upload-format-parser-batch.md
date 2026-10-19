# Separate the upload path from the record-processing path

The learner distinguished upload (how bytes arrive), format (how records are arranged), parser (how bytes/text become Python values), and batch (how many records are processed together), and explained that FastAPI's `yield` JSON Lines example is outbound rather than an upload parser. This means future lessons can build on HTTP direction and format vocabulary; the current challenge is now enforcing a true per-record memory bound before decoding or parsing.
