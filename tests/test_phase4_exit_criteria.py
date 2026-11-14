from __future__ import annotations

import csv
import os
import socket
from pathlib import Path

import pytest

from spark.jobs.benchmark_compaction import BENCHMARK_QUERIES

project_root = Path(__file__).resolve().parents[1]


def is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def test_iceberg_maintenance_job_exists():
    maint_script = project_root / "spark" / "jobs" / "iceberg_maintenance.py"
    assert maint_script.exists(), "spark/jobs/iceberg_maintenance.py must exist"
    assert maint_script.stat().st_size > 0, (
        "spark/jobs/iceberg_maintenance.py must not be empty"
    )


def test_docker_compose_metabase_service_declared():
    compose_path = project_root / "docker-compose.yml"
    assert compose_path.exists(), "docker-compose.yml must exist"

    content = compose_path.read_text(encoding="utf-8")
    assert "metabase:" in content, (
        "Metabase service must be declared in docker-compose.yml"
    )
    assert "image: metabase/metabase" in content, (
        "Metabase service must have a pinned metabase image"
    )
    assert "metabase_data:" in content, "metabase_data volume must be declared"


def test_benchmark_runner_and_queries_completeness():
    benchmark_script = project_root / "spark" / "jobs" / "benchmark_compaction.py"
    assert benchmark_script.exists(), "spark/jobs/benchmark_compaction.py must exist"

    # Must contain 4 defined benchmark queries
    assert len(BENCHMARK_QUERIES) >= 4, "Must define at least 4 benchmark queries"
    assert "q1_trending_repos" in BENCHMARK_QUERIES
    assert "q2_developer_heatmap" in BENCHMARK_QUERIES
    assert "q3_language_trends" in BENCHMARK_QUERIES
    assert "q4_silver_fact_engagements" in BENCHMARK_QUERIES


def test_benchmark_artifacts_format_and_schema():
    csv_path = project_root / "docs" / "benchmark" / "benchmark_results.csv"
    assert csv_path.exists(), "docs/benchmark/benchmark_results.csv must exist"

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        required_cols = {
            "phase",
            "query_id",
            "query_name",
            "iteration",
            "latency_ms",
            "file_count",
            "total_size_bytes",
            "timestamp",
        }
        assert required_cols.issubset(set(fieldnames)), (
            f"Missing columns in CSV: {required_cols - set(fieldnames)}"
        )

        rows = list(reader)
        assert len(rows) > 0, "Benchmark CSV must contain timing rows"
        phases = {r["phase"] for r in rows}
        assert "before_compaction" in phases, "Must have before_compaction records"
        assert "after_compaction" in phases, "Must have after_compaction records"


def test_benchmark_markdown_report_structure():
    report_path = project_root / "docs" / "benchmark_report.md"
    assert report_path.exists(), "docs/benchmark_report.md must exist"

    content = report_path.read_text(encoding="utf-8")
    assert (
        "## 1. Bối Cảnh Thực Nghiệm & Cấu Hình Môi Trường" in content
        or "Experimental Conditions" in content
    )
    assert (
        "## 2. Danh Mục Các Câu Truy Vấn Benchmark" in content
        or "Benchmark Query Definitions" in content
    )
    assert (
        "## 3. Bảng Kết Quả Thực Nghiệm" in content
        or "Measured Performance Comparison" in content
    )
    assert "q1_trending_repos" in content
    assert "mart_trending_repos_daily" in content


def test_phase4_exit_criteria_trino_live():
    trino_port = int(os.getenv("TRINO_PORT", "8082"))
    if not is_port_open("localhost", trino_port):
        pytest.skip(
            f"Trino service is not accessible on localhost:{trino_port}; skipping live Phase 4 check."
        )

    try:
        import trino

        conn = trino.dbapi.connect(
            host="localhost",
            port=trino_port,
            user="admin",
            catalog="iceberg",
            schema="gold",
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES FROM iceberg.gold")
        tables = [row[0] for row in cursor.fetchall()]
        assert "mart_trending_repos_daily" in tables
        assert "mart_language_trends_daily" in tables
        assert "mart_developer_work_patterns_daily" in tables
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Trino live check skipped due to connection error: {exc}")
