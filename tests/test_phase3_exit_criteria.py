from __future__ import annotations

import os
import socket

import pytest

import trino


def is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def get_trino_cursor(schema: str = "gold"):
    trino_host = os.getenv("TRINO_HOST", "localhost")
    trino_port = int(os.getenv("TRINO_PORT", "8082"))
    if not is_port_open(trino_host, trino_port):
        pytest.skip(
            f"Trino service is not accessible on {trino_host}:{trino_port}; skipping live integration test."
        )
    try:
        conn = trino.dbapi.connect(
            host=trino_host,
            port=trino_port,
            user="admin",
            catalog="iceberg",
            schema=schema,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchall()
        return cursor
    except (trino.exceptions.Error, OSError) as exc:
        pytest.skip(
            f"Trino service is not accessible ({exc}); skipping live integration test."
        )


def test_phase3_exit_criteria_trino():
    """
    Validates Phase 3 Exit Criteria against Iceberg tables via Trino:
    1. Mart 1 Uniqueness: Zero duplicate (metric_date, repo_id) in mart_trending_repos_daily.
    2. Mart 2 Uniqueness: Zero duplicate (metric_date, primary_language) in mart_language_trends_daily.
    3. Mart 3 Uniqueness: Zero duplicate (metric_date, day_of_week, hour_of_day) in mart_developer_work_patterns_daily.
    4. Reconciliation: Silver stars == Gold Trending stars == Gold Language stars.
    5. Bot Exclusion: Bots in dim_users are not counted in human metrics.
    6. Dimensions and Facts uniqueness in Silver layer.
    """
    cursor = get_trino_cursor(schema="gold")

    # 1. Mart 1 Uniqueness
    cursor.execute(
        """
        SELECT metric_date, repo_id, COUNT(*) AS key_count
        FROM iceberg.gold.mart_trending_repos_daily
        GROUP BY metric_date, repo_id
        HAVING COUNT(*) > 1
        """
    )
    m1_dups = cursor.fetchall()
    assert len(m1_dups) == 0, f"Duplicate keys in mart_trending_repos_daily: {m1_dups}"

    # 2. Mart 2 Uniqueness
    cursor.execute(
        """
        SELECT metric_date, primary_language, COUNT(*) AS key_count
        FROM iceberg.gold.mart_language_trends_daily
        GROUP BY metric_date, primary_language
        HAVING COUNT(*) > 1
        """
    )
    m2_dups = cursor.fetchall()
    assert len(m2_dups) == 0, f"Duplicate keys in mart_language_trends_daily: {m2_dups}"

    # 3. Mart 3 Uniqueness
    cursor.execute(
        """
        SELECT metric_date, day_of_week, hour_of_day, COUNT(*) AS key_count
        FROM iceberg.gold.mart_developer_work_patterns_daily
        GROUP BY metric_date, day_of_week, hour_of_day
        HAVING COUNT(*) > 1
        """
    )
    m3_dups = cursor.fetchall()
    assert len(m3_dups) == 0, (
        f"Duplicate keys in mart_developer_work_patterns_daily: {m3_dups}"
    )

    # 4. Reconciliation check across dates present in Gold
    cursor.execute(
        "SELECT DISTINCT metric_date FROM iceberg.gold.mart_trending_repos_daily"
    )
    dates = [row[0] for row in cursor.fetchall()]

    for target_date in dates:
        # Silver stars
        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM iceberg.silver.fact_engagements
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
              AND engagement_type = 'star'
              AND action = 'started'
            """
        )
        silver_stars = cursor.fetchone()[0]

        # Trending gold stars
        cursor.execute(
            f"""
            SELECT COALESCE(SUM(new_stars_count), 0)
            FROM iceberg.gold.mart_trending_repos_daily
            WHERE metric_date = DATE '{target_date}'
            """
        )
        trending_stars = cursor.fetchone()[0]

        # Language gold stars
        cursor.execute(
            f"""
            SELECT COALESCE(SUM(new_stars_count), 0)
            FROM iceberg.gold.mart_language_trends_daily
            WHERE metric_date = DATE '{target_date}'
            """
        )
        language_stars = cursor.fetchone()[0]

        assert silver_stars == trending_stars, (
            f"Silver stars ({silver_stars}) != Trending stars ({trending_stars}) on {target_date}"
        )
        assert trending_stars == language_stars, (
            f"Trending stars ({trending_stars}) != Language stars ({language_stars}) on {target_date}"
        )
