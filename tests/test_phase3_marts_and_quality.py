from __future__ import annotations

import pytest

from spark.jobs.quality_check import QualityGateError
from spark.utils.backfill_runner import generate_source_hours_range


def test_generate_source_hours_range():
    # 1. Same day, 3 hours
    hours = generate_source_hours_range("2024-01-01-0", "2024-01-01-2")
    assert hours == ["2024-01-01-0", "2024-01-01-1", "2024-01-01-2"]

    # 2. Across midnight boundary
    cross_hours = generate_source_hours_range("2024-01-01-22", "2024-01-02-1")
    assert cross_hours == [
        "2024-01-01-22",
        "2024-01-01-23",
        "2024-01-02-0",
        "2024-01-02-1",
    ]

    # 3. Invalid start > end raises ValueError
    with pytest.raises(ValueError, match="cannot be after end datetime"):
        generate_source_hours_range("2024-01-02-0", "2024-01-01-0")

    # 4. Malformed source hour format
    with pytest.raises(ValueError, match="Invalid source_hour format"):
        generate_source_hours_range("invalid-hour", "2024-01-01-2")


def test_quality_gate_error_exception():
    err = QualityGateError("Duplicate keys detected in gold mart")
    assert isinstance(err, Exception)
    assert str(err) == "Duplicate keys detected in gold mart"
