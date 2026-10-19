import pytest

from app.core.progress_logging import ProgressEventGate


def test_progress_gate_bounds_ten_million_records_to_one_hundred_events() -> None:
    gate = ProgressEventGate(100_000)

    emitted_at = [
        records_processed
        for records_processed in range(5_000, 10_000_001, 5_000)
        if gate.crossed(records_processed)
    ]

    assert len(emitted_at) == 100
    assert emitted_at[0] == 100_000
    assert emitted_at[-1] == 10_000_000


def test_progress_gate_collapses_multiple_crossed_thresholds_into_one_event() -> None:
    gate = ProgressEventGate(100_000)

    assert gate.crossed(350_000) is True
    assert gate.crossed(399_999) is False
    assert gate.crossed(400_000) is True


def test_progress_gate_rejects_an_unbounded_interval() -> None:
    with pytest.raises(ValueError, match="at least 100000"):
        ProgressEventGate(99_999)
