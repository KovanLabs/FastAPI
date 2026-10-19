"""Shared bounded progress-event threshold tracking."""

from __future__ import annotations


class ProgressEventGate:
    """Allow at most one event for each crossed record-count interval."""

    def __init__(self, interval: int) -> None:
        if interval < 100_000:
            raise ValueError("progress interval must be at least 100000")
        self.interval = interval
        self._next_threshold = interval

    def crossed(self, records_processed: int) -> bool:
        """Return true once when a monotonic count crosses a threshold."""

        if records_processed < self._next_threshold:
            return False
        self._next_threshold = (
            records_processed // self.interval + 1
        ) * self.interval
        return True
