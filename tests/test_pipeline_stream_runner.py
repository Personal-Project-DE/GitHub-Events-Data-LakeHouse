from __future__ import annotations

from unittest.mock import MagicMock

from spark.jobs.pipeline_stream_runner import (
    get_dates_in_range,
    is_hour_already_completed,
    prune_oldest_day_if_exceeds,
)


def test_get_dates_in_range():
    dates = get_dates_in_range("2026-08-01", "2026-08-03")
    assert dates == ["2026-08-01", "2026-08-02", "2026-08-03"]


def test_get_dates_in_range_single_day():
    dates = get_dates_in_range("2026-08-01", "2026-08-01")
    assert dates == ["2026-08-01"]


def test_is_hour_already_completed_true():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.collect.return_value = [{"status": "BRONZE_COMPLETE"}]
    mock_spark.sql.return_value = mock_df

    assert is_hour_already_completed(mock_spark, "2026-08-01-0") is True


def test_is_hour_already_completed_false():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.collect.return_value = []
    mock_spark.sql.return_value = mock_df

    assert is_hour_already_completed(mock_spark, "2026-08-01-0") is False


def test_prune_oldest_day_within_window():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    # 2 dates, retention limit is 5 -> should not prune
    mock_df.collect.return_value = [
        {"metric_date": "2026-08-01"},
        {"metric_date": "2026-08-02"},
    ]
    mock_spark.sql.return_value = mock_df

    result = prune_oldest_day_if_exceeds(
        mock_spark, retention_days=5, current_date="2026-08-02"
    )
    assert result == []
    # No delete executed
    assert not any("DELETE" in str(c) for c in mock_spark.sql.call_args_list)


def test_prune_oldest_day_exceeds_window():
    mock_spark = MagicMock()
    mock_df = MagicMock()
    # 3 dates, retention limit is 2 -> oldest date '2026-08-01' should be pruned
    mock_df.collect.return_value = [
        {"metric_date": "2026-08-01"},
        {"metric_date": "2026-08-02"},
        {"metric_date": "2026-08-03"},
    ]
    mock_spark.sql.return_value = mock_df

    result = prune_oldest_day_if_exceeds(
        mock_spark, retention_days=2, current_date="2026-08-03"
    )
    assert result == ["2026-08-01"]
    # Verify DELETE queries and expire snapshots were called
    executed_sqls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
    assert any(
        "DELETE FROM iceberg.gold.mart_trending_repos_daily" in s for s in executed_sqls
    )
    assert any(
        "DELETE FROM iceberg.silver.fact_engagements" in s for s in executed_sqls
    )
    assert any("DELETE FROM iceberg.bronze.github_events" in s for s in executed_sqls)
    assert any("CALL iceberg.system.expire_snapshots" in s for s in executed_sqls)
