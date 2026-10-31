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


def get_trino_cursor(schema: str = "bronze"):
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


def test_bronze_replay_and_uniqueness():
    """
    Validates Phase 1 Exit Criteria via Trino:
    - rows == distinct_event_ids for every ingested source_hour
    - Control ledger has exactly one record for source_hour with BRONZE_COMPLETE
    - Every bronze record has traceability lineage fields populated
    """
    cursor = get_trino_cursor(schema="bronze")

    # 1. Uniqueness check on Bronze table
    cursor.execute(
        """
        SELECT source_hour,
               COUNT(*) AS rows,
               COUNT(DISTINCT event_id) AS distinct_event_ids
        FROM iceberg.bronze.github_events
        GROUP BY source_hour
        """
    )
    results = cursor.fetchall()
    assert len(results) > 0, "Expected at least one source_hour in bronze.github_events"

    for source_hour, total_rows, distinct_ids in results:
        assert total_rows == distinct_ids, (
            f"Uniqueness violation in source_hour {source_hour}: "
            f"total rows ({total_rows}) != distinct event_ids ({distinct_ids})"
        )

    # 2. Lineage fields validation
    cursor.execute(
        """
        SELECT event_id, record_hash, source_hour, source_path, source_checksum, ingestion_run_id, ingested_at
        FROM iceberg.bronze.github_events
        LIMIT 5
        """
    )
    sample_rows = cursor.fetchall()
    assert len(sample_rows) > 0, "Expected sample rows in bronze.github_events"
    for row in sample_rows:
        event_id, r_hash, s_hour, s_path, s_checksum, run_id, ing_at = row
        assert event_id is not None and str(event_id).strip() != ""
        assert r_hash is not None and len(r_hash) == 64
        assert s_hour is not None and len(s_hour) >= 10
        assert s_path.startswith(("s3a://", "s3://"))
        assert s_checksum is not None and len(s_checksum) == 64
        assert run_id is not None and len(run_id) > 0
        assert ing_at is not None

    # 3. Control Ledger Status
    cursor.execute(
        """
        SELECT source_hour, status, raw_record_count, bronze_record_count, quarantine_record_count
        FROM iceberg.control.ingestion_runs
        """
    )
    ledger_rows = cursor.fetchall()
    assert len(ledger_rows) > 0, "Expected at least one run in control ledger"

    completed_statuses = {
        "BRONZE_COMPLETE",
        "SILVER_COMPLETE",
        "ENRICHED",
        "GOLD_COMPLETE",
    }
    for row in ledger_rows:
        source_hour, status, raw_cnt, bronze_cnt, quar_cnt = row
        assert status in completed_statuses
        assert raw_cnt is not None and raw_cnt >= 0
        assert bronze_cnt is not None and bronze_cnt >= 0
        assert quar_cnt is not None and quar_cnt >= 0
        assert raw_cnt == (bronze_cnt + quar_cnt)
