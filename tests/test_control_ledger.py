from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from spark.utils.control_ledger import (
    acquire_run_lock,
    advance_run_status,
    complete_run,
    ensure_control_table,
    fail_run,
    get_ingestion_run,
)


def test_ensure_control_table():
    spark = MagicMock()
    ensure_control_table(spark)
    assert spark.sql.call_count == 2
    create_namespace_call = spark.sql.call_args_list[0][0][0]
    create_table_call = spark.sql.call_args_list[1][0][0]
    assert "CREATE NAMESPACE IF NOT EXISTS iceberg.control" in create_namespace_call
    assert (
        "CREATE TABLE IF NOT EXISTS iceberg.control.ingestion_runs" in create_table_call
    )


def test_get_ingestion_run_not_found():
    spark = MagicMock()
    mock_df = MagicMock()
    mock_df.collect.return_value = []
    spark.sql.return_value = mock_df

    res = get_ingestion_run(spark, "2024-01-01-0")
    assert res is None


def test_get_ingestion_run_found():
    spark = MagicMock()
    mock_row = MagicMock()
    mock_row.asDict.return_value = {
        "source_hour": "2024-01-01-0",
        "source_checksum": "abc123",
        "status": "BRONZE_COMPLETE",
    }
    mock_df = MagicMock()
    mock_df.collect.return_value = [mock_row]
    spark.sql.return_value = mock_df

    res = get_ingestion_run(spark, "2024-01-01-0")
    assert res is not None
    assert res["source_hour"] == "2024-01-01-0"
    assert res["status"] == "BRONZE_COMPLETE"


def test_acquire_run_lock_new_record():
    spark = MagicMock()
    with patch("spark.utils.control_ledger.get_ingestion_run", return_value=None):
        acquire_run_lock(
            spark=spark,
            source_hour="2024-01-01-0",
            source_path="s3://lakehouse/landing/test.json.gz",
            source_checksum="sha_val",
            run_id="run-101",
            force=False,
        )
        assert spark.sql.call_count == 1
        executed_sql = spark.sql.call_args[0][0]
        assert "MERGE INTO iceberg.control.ingestion_runs" in executed_sql
        assert "2024-01-01-0" in executed_sql
        assert "run-101" in executed_sql
        assert "BRONZE_PROCESSING" in executed_sql


def test_acquire_run_lock_checksum_conflict_without_force():
    spark = MagicMock()
    existing_record = {
        "source_hour": "2024-01-01-0",
        "source_checksum": "old_checksum",
        "status": "BRONZE_COMPLETE",
    }
    with (
        patch(
            "spark.utils.control_ledger.get_ingestion_run", return_value=existing_record
        ),
        pytest.raises(ValueError, match="Source version conflict"),
    ):
        acquire_run_lock(
            spark=spark,
            source_hour="2024-01-01-0",
            source_path="s3://lakehouse/landing/test.json.gz",
            source_checksum="new_checksum",
            run_id="run-102",
            force=False,
        )


def test_acquire_run_lock_checksum_conflict_with_force():
    spark = MagicMock()
    existing_record = {
        "source_hour": "2024-01-01-0",
        "source_checksum": "old_checksum",
        "status": "BRONZE_COMPLETE",
    }
    with patch(
        "spark.utils.control_ledger.get_ingestion_run", return_value=existing_record
    ):
        acquire_run_lock(
            spark=spark,
            source_hour="2024-01-01-0",
            source_path="s3://lakehouse/landing/test.json.gz",
            source_checksum="new_checksum",
            run_id="run-102",
            force=True,
        )
        assert spark.sql.call_count == 1
        executed_sql = spark.sql.call_args[0][0]
        assert "MERGE INTO iceberg.control.ingestion_runs" in executed_sql


def test_acquire_run_lock_concurrency_conflict_without_force():
    spark = MagicMock()
    existing_record = {
        "source_hour": "2024-01-01-0",
        "source_checksum": "same_checksum",
        "status": "BRONZE_PROCESSING",
        "run_id": "active-worker-9",
        "started_at": "2024-01-01 00:00:00",
    }
    with (
        patch(
            "spark.utils.control_ledger.get_ingestion_run", return_value=existing_record
        ),
        pytest.raises(RuntimeError, match="Concurrency conflict"),
    ):
        acquire_run_lock(
            spark=spark,
            source_hour="2024-01-01-0",
            source_path="s3://lakehouse/landing/test.json.gz",
            source_checksum="same_checksum",
            run_id="run-103",
            force=False,
        )


def test_acquire_run_lock_concurrency_conflict_with_force():
    spark = MagicMock()
    existing_record = {
        "source_hour": "2024-01-01-0",
        "source_checksum": "same_checksum",
        "status": "BRONZE_PROCESSING",
        "run_id": "active-worker-9",
        "started_at": "2024-01-01 00:00:00",
    }
    with patch(
        "spark.utils.control_ledger.get_ingestion_run", return_value=existing_record
    ):
        acquire_run_lock(
            spark=spark,
            source_hour="2024-01-01-0",
            source_path="s3://lakehouse/landing/test.json.gz",
            source_checksum="same_checksum",
            run_id="run-103",
            force=True,
        )
        assert spark.sql.call_count == 1
        executed_sql = spark.sql.call_args[0][0]
        assert "MERGE INTO iceberg.control.ingestion_runs" in executed_sql


def test_advance_run_status():
    spark = MagicMock()
    advance_run_status(
        spark=spark,
        source_hour="2024-01-01-0",
        run_id="run-200",
        new_status="SILVER_COMPLETE",
    )
    assert spark.sql.call_count == 1
    sql = spark.sql.call_args[0][0]
    assert "UPDATE iceberg.control.ingestion_runs" in sql
    assert "SET status = 'SILVER_COMPLETE'" in sql
    assert "source_hour = '2024-01-01-0'" in sql


def test_advance_run_status_gold_complete_records_completion():
    spark = MagicMock()
    advance_run_status(
        spark=spark,
        source_hour="2024-01-01-0",
        run_id="run-201",
        new_status="GOLD_COMPLETE",
    )
    assert spark.sql.call_count == 1
    sql = spark.sql.call_args[0][0]
    assert "completed_at = TIMESTAMP" in sql


def test_fail_run():
    spark = MagicMock()
    fail_run(
        spark=spark,
        source_hour="2024-01-01-0",
        run_id="run-202",
        error_message="OOM: Java heap space error on 'node-3'",
    )
    assert spark.sql.call_count == 1
    sql = spark.sql.call_args[0][0]
    assert "SET status = 'FAILED'" in sql
    assert "error_message = 'OOM: Java heap space error on ''node-3'''" in sql


def test_complete_run():
    spark = MagicMock()
    complete_run(
        spark=spark,
        source_hour="2024-01-01-0",
        run_id="run-203",
        raw_count=1000,
        bronze_count=980,
        quarantine_count=20,
    )
    assert spark.sql.call_count == 1
    sql = spark.sql.call_args[0][0]
    assert "status = 'BRONZE_COMPLETE'" in sql
    assert "raw_record_count = 1000" in sql
    assert "bronze_record_count = 980" in sql
    assert "quarantine_record_count = 20" in sql
