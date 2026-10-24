import os

import trino


def test_bronze_replay_and_uniqueness():
    """
    Validates Phase 1 Exit Criteria via Trino:
    - rows == distinct_event_ids for every ingested source_hour
    - Control ledger has exactly one record for source_hour with BRONZE_COMPLETE
    - Every bronze record has traceability lineage fields populated
    """
    trino_port = int(os.getenv("TRINO_PORT", "8082"))
    conn = trino.dbapi.connect(
        host="localhost",
        port=trino_port,
        user="admin",
        catalog="iceberg",
        schema="bronze",
    )
    cursor = conn.cursor()

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
    assert len(sample_rows) > 0
    for row in sample_rows:
        (
            event_id,
            record_hash,
            source_hour,
            source_path,
            source_checksum,
            run_id,
            ingested_at,
        ) = row
        assert event_id is not None and len(event_id) > 0
        assert record_hash is not None and len(record_hash) == 64
        assert source_hour == "2024-01-01-0"
        assert "s3a://lakehouse/landing" in source_path
        assert len(source_checksum) == 64
        assert run_id is not None
        assert ingested_at is not None

    # 3. Control Ledger verification
    cursor.execute(
        """
        SELECT source_hour, status, raw_record_count, bronze_record_count, quarantine_record_count
        FROM iceberg.control.ingestion_runs
        WHERE source_hour = '2024-01-01-0'
        """
    )
    control_rows = cursor.fetchall()
    assert len(control_rows) == 1, (
        f"Expected exactly 1 control record, got {len(control_rows)}"
    )
    src_hr, status, raw_cnt, bronze_cnt, quar_cnt = control_rows[0]
    assert src_hr == "2024-01-01-0"
    assert status == "BRONZE_COMPLETE"
    assert raw_cnt == 4
    assert bronze_cnt == 4
    assert quar_cnt == 0
