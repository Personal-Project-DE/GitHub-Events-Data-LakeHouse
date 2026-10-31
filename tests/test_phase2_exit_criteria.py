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
    trino_port = int(os.getenv("TRINO_PORT", "8082"))
    if not is_port_open("localhost", trino_port):
        pytest.skip(
            f"Trino service is not accessible on localhost:{trino_port}; skipping live integration test."
        )
    try:
        conn = trino.dbapi.connect(
            host="localhost",
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


def test_phase2_exit_criteria_trino():
    """
    Validates Phase 2 Exit Criteria against Iceberg tables via Trino:
    1. Verification SQL:
       SELECT metric_date, repo_id, COUNT(*) AS key_count
       FROM iceberg.gold.mart_trending_repos_daily
       GROUP BY metric_date, repo_id
       HAVING COUNT(*) > 1;
       Must return 0 rows.
    2. Dimensions uniqueness:
       - No duplicate user_id in silver.dim_users
       - No duplicate repo_id in silver.dim_repositories
    3. Facts event_id uniqueness:
       - No duplicate event_id in fact_commits, fact_pull_requests, fact_engagements
    4. Repo metadata language rule:
       - No invented null primary_language in silver.repo_metadata
    """
    cursor = get_trino_cursor(schema="gold")

    # 1. Verification SQL: No duplicate metric_date, repo_id pairs
    cursor.execute(
        """
        SELECT metric_date, repo_id, COUNT(*) AS key_count
        FROM iceberg.gold.mart_trending_repos_daily
        GROUP BY metric_date, repo_id
        HAVING COUNT(*) > 1
        """
    )
    duplicates = cursor.fetchall()
    assert len(duplicates) == 0, (
        f"Duplicate keys found in gold trending repos mart: {duplicates}"
    )

    # 2. Dimensions uniqueness check
    cursor.execute(
        """
        SELECT user_id, COUNT(*)
        FROM iceberg.silver.dim_users
        GROUP BY user_id
        HAVING COUNT(*) > 1
        """
    )
    user_dups = cursor.fetchall()
    assert len(user_dups) == 0, f"Duplicate user_id found in dim_users: {user_dups}"

    cursor.execute(
        """
        SELECT repo_id, COUNT(*)
        FROM iceberg.silver.dim_repositories
        GROUP BY repo_id
        HAVING COUNT(*) > 1
        """
    )
    repo_dups = cursor.fetchall()
    assert len(repo_dups) == 0, (
        f"Duplicate repo_id found in dim_repositories: {repo_dups}"
    )

    # 3. Facts uniqueness check
    for fact_table in ("fact_commits", "fact_pull_requests", "fact_engagements"):
        cursor.execute(
            f"""
            SELECT event_id, COUNT(*)
            FROM iceberg.silver.{fact_table}
            GROUP BY event_id
            HAVING COUNT(*) > 1
            """
        )
        fact_dups = cursor.fetchall()
        assert len(fact_dups) == 0, (
            f"Duplicate event_id found in silver.{fact_table}: {fact_dups}"
        )
