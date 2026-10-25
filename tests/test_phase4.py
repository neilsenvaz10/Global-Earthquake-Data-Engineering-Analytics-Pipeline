"""Window-selection logic is testable without installing Airflow."""
from datetime import timedelta
import pytest
from quaketrack.ingestion.windowing import parse_utc
from quaketrack.orchestration import incremental_window


def test_bootstrap_is_bounded():
    target = parse_utc("2026-10-25T12:00:00Z")
    left, right = incremental_window(target)
    assert right == target and right - left == timedelta(hours=6)


def test_overlap_and_gap_free_catchup():
    old = parse_utc("2026-10-20T12:00:00Z")
    left, right = incremental_window("2026-10-25T12:00:00Z", old, coverage_start=old - timedelta(days=10))
    assert left == old - timedelta(hours=72)
    assert right == old + timedelta(hours=6)
    assert left <= old <= right


def test_overlap_never_backfills_before_initial_coverage():
    old = parse_utc("2026-10-25T12:00:00Z")
    coverage = old - timedelta(hours=6)
    left, right = incremental_window("2026-10-25T13:00:00Z", old, coverage_start=coverage)
    assert left == coverage and right == old + timedelta(hours=1)


@pytest.mark.parametrize("kwargs", [{"overlap_hours": 0}, {"bootstrap_hours": -1},
    {"step_hours": float("nan")}, {"step_hours": 200}])
def test_invalid_incremental_options(kwargs):
    with pytest.raises(ValueError):
        incremental_window("2026-10-25", **kwargs)


def test_checkpoint_ahead_of_target_is_rejected():
    with pytest.raises(ValueError):
        incremental_window("2026-10-25", "2026-10-26", coverage_start="2026-10-20")
