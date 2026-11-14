from __future__ import annotations

from spark.jobs.iceberg_maintenance import (
    TABLE_MAINTENANCE_PROFILES,
    build_expire_snapshots_sql,
    build_remove_orphan_files_sql,
    build_rewrite_data_files_sql,
    build_rewrite_manifests_sql,
    canonicalize_table_name,
    parse_args,
)


def test_canonicalize_table_name():
    assert (
        canonicalize_table_name("gold.mart_trending_repos_daily")
        == "iceberg.gold.mart_trending_repos_daily"
    )
    assert (
        canonicalize_table_name("iceberg.gold.mart_trending_repos_daily")
        == "iceberg.gold.mart_trending_repos_daily"
    )
    assert (
        canonicalize_table_name("bronze.github_events")
        == "iceberg.bronze.github_events"
    )


def test_build_rewrite_data_files_sql_default_binpack():
    sql = build_rewrite_data_files_sql("gold.mart_trending_repos_daily")
    assert "CALL iceberg.system.rewrite_data_files" in sql
    assert "table => 'iceberg.gold.mart_trending_repos_daily'" in sql
    assert "strategy => 'binpack'" in sql
    assert "target-file-size-bytes" in sql
    assert "134217728" in sql


def test_build_rewrite_data_files_sql_sort_strategy():
    sql = build_rewrite_data_files_sql(
        table="iceberg.bronze.github_events",
        strategy="sort",
        sort_order="created_at ASC, event_type ASC",
        target_file_size_bytes=67108864,
        where_clause="created_at >= '2024-01-01'",
    )
    assert "strategy => 'sort'" in sql
    assert "sort_order => 'created_at ASC, event_type ASC'" in sql
    assert "target-file-size-bytes', '67108864'" in sql
    assert "where => 'created_at >= '2024-01-01''" in sql


def test_build_expire_snapshots_sql():
    sql = build_expire_snapshots_sql(
        table="silver.dim_users",
        older_than_ts="2026-11-10 12:00:00",
        retain_last=7,
    )
    assert "CALL iceberg.system.expire_snapshots" in sql
    assert "table => 'iceberg.silver.dim_users'" in sql
    assert "older_than => TIMESTAMP '2026-11-10 12:00:00'" in sql
    assert "retain_last => 7" in sql


def test_build_remove_orphan_files_sql():
    sql = build_remove_orphan_files_sql(
        table="gold.mart_language_trends_daily",
        older_than_ts="2026-11-10 00:00:00",
    )
    assert "CALL iceberg.system.remove_orphan_files" in sql
    assert "table => 'iceberg.gold.mart_language_trends_daily'" in sql
    assert "older_than => TIMESTAMP '2026-11-10 00:00:00'" in sql


def test_build_rewrite_manifests_sql():
    sql = build_rewrite_manifests_sql("bronze.github_events")
    assert (
        sql
        == "CALL iceberg.system.rewrite_manifests(table => 'iceberg.bronze.github_events')"
    )


def test_table_maintenance_profiles_completeness():
    # Verify core medallion tables are configured
    assert "iceberg.bronze.github_events" in TABLE_MAINTENANCE_PROFILES
    assert "iceberg.silver.fact_engagements" in TABLE_MAINTENANCE_PROFILES
    assert (
        "iceberg.silver.fact_prs" not in TABLE_MAINTENANCE_PROFILES
        or "iceberg.silver.fact_pull_requests" in TABLE_MAINTENANCE_PROFILES
    )
    assert "iceberg.silver.fact_commits" in TABLE_MAINTENANCE_PROFILES
    assert "iceberg.gold.mart_trending_repos_daily" in TABLE_MAINTENANCE_PROFILES
    assert "iceberg.gold.mart_language_trends_daily" in TABLE_MAINTENANCE_PROFILES
    assert (
        "iceberg.gold.mart_developer_work_patterns_daily" in TABLE_MAINTENANCE_PROFILES
    )


def test_parse_args_all_tables():
    args = parse_args(
        ["--all-tables", "--dry-run", "--older-than-days", "5", "--retain-last", "10"]
    )
    assert args.all_tables is True
    assert args.dry_run is True
    assert args.older_than_days == 5
    assert args.retain_last == 10


def test_parse_args_fifo_retention():
    args = parse_args(["--fifo-retention-days", "90"])
    assert args.fifo_retention_days == 90

    args_prune = parse_args(["--prune-date", "2026-08-01", "--dry-run"])
    assert args_prune.prune_date == "2026-08-01"
    assert args_prune.dry_run is True


def test_fifo_retention_prune_logic():
    from unittest.mock import MagicMock

    from spark.jobs.iceberg_maintenance import run_fifo_retention_prune

    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_df.collect.return_value = [
        {"metric_date": "2026-08-01"},
        {"metric_date": "2026-08-02"},
        {"metric_date": "2026-08-03"},
    ]
    mock_spark.sql.return_value = mock_df

    pruned = run_fifo_retention_prune(
        mock_spark, retention_days=2, current_date="2026-08-03"
    )
    assert pruned == ["2026-08-01"]

    calls = [str(c[0][0]) for c in mock_spark.sql.call_args_list]
    assert any("DELETE FROM iceberg.gold.mart_trending_repos_daily" in s for s in calls)
    assert any("DELETE FROM iceberg.bronze.github_events" in s for s in calls)
    assert any("CALL iceberg.system.expire_snapshots" in s for s in calls)
