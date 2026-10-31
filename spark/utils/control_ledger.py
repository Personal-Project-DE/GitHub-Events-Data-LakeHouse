from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def ensure_control_table(spark: SparkSession) -> None:
    """Creates the control namespace and ingestion_runs table in Iceberg if they do not exist."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.control")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.control.ingestion_runs (
            source_hour STRING NOT NULL,
            source_path STRING NOT NULL,
            source_checksum STRING NOT NULL,
            run_id STRING NOT NULL,
            status STRING NOT NULL,
            raw_record_count BIGINT,
            bronze_record_count BIGINT,
            quarantine_record_count BIGINT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            error_message STRING
        )
        USING iceberg
        """
    )


def get_ingestion_run(spark: SparkSession, source_hour: str) -> dict[str, Any] | None:
    """Retrieves the latest state of an ingestion run for a given source_hour."""
    ensure_control_table(spark)
    df = spark.sql(
        f"SELECT * FROM iceberg.control.ingestion_runs WHERE source_hour = '{source_hour}'"
    )
    rows = df.collect()
    if not rows:
        return None
    row = rows[0]
    return row.asDict()


def acquire_run_lock(
    spark: SparkSession,
    source_hour: str,
    source_path: str,
    source_checksum: str,
    run_id: str,
    force: bool = False,
) -> None:
    """
    Acquires an ingestion lock for the source hour:
    - Enforces source version history: raises error if checksum changes without force.
    - Enforces concurrency lock: raises error if another run is currently processing without force.
    - Upserts the control record with status 'BRONZE_PROCESSING'.
    """
    existing = get_ingestion_run(spark, source_hour)
    if existing:
        if existing["source_checksum"] != source_checksum and not force:
            raise ValueError(
                f"Source version conflict for source_hour '{source_hour}': "
                f"Existing checksum '{existing['source_checksum']}' does not match "
                f"incoming checksum '{source_checksum}'. Reprocessing changed versions requires explicit force flag."
            )
        active_statuses = {"BRONZE_PROCESSING", "SILVER_PROCESSING", "GOLD_PROCESSING"}
        if existing["status"] in active_statuses and not force:
            raise RuntimeError(
                f"Concurrency conflict: source_hour '{source_hour}' is currently locked with status "
                f"'{existing['status']}' by run_id '{existing['run_id']}' started at {existing['started_at']}."
            )

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # noqa: UP017

    # Upsert control record via MERGE INTO
    merge_sql = f"""
    MERGE INTO iceberg.control.ingestion_runs AS target
    USING (
        SELECT
            '{source_hour}' AS source_hour,
            '{source_path}' AS source_path,
            '{source_checksum}' AS source_checksum,
            '{run_id}' AS run_id,
            'BRONZE_PROCESSING' AS status,
            CAST(NULL AS BIGINT) AS raw_record_count,
            CAST(NULL AS BIGINT) AS bronze_record_count,
            CAST(NULL AS BIGINT) AS quarantine_record_count,
            TIMESTAMP '{now_iso}' AS started_at,
            CAST(NULL AS TIMESTAMP) AS completed_at,
            CAST(NULL AS STRING) AS error_message
    ) AS source
    ON target.source_hour = source.source_hour
    WHEN MATCHED THEN
        UPDATE SET
            target.source_path = source.source_path,
            target.source_checksum = source.source_checksum,
            target.run_id = source.run_id,
            target.status = source.status,
            target.started_at = source.started_at,
            target.completed_at = NULL,
            target.error_message = NULL
    WHEN NOT MATCHED THEN
        INSERT (
            source_hour, source_path, source_checksum, run_id, status,
            raw_record_count, bronze_record_count, quarantine_record_count,
            started_at, completed_at, error_message
        )
        VALUES (
            source.source_hour, source.source_path, source.source_checksum, source.run_id, source.status,
            source.raw_record_count, source.bronze_record_count, source.quarantine_record_count,
            source.started_at, source.completed_at, source.error_message
        )
    """
    spark.sql(merge_sql)


def advance_run_status(
    spark: SparkSession,
    source_hour: str,
    run_id: str,
    new_status: str,
    error_message: str | None = None,
) -> None:
    """Advances the status of the ingestion run in the control table."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # noqa: UP017
    if error_message:
        safe_msg = error_message.replace("'", "''")[:1000]
        err_clause = f"error_message = '{safe_msg}'"
    else:
        err_clause = "error_message = NULL"

    if new_status in ("GOLD_COMPLETE", "FAILED"):
        completed_clause = f"completed_at = TIMESTAMP '{now_iso}'"
    else:
        completed_clause = "completed_at = completed_at"

    update_sql = f"""
    UPDATE iceberg.control.ingestion_runs
    SET status = '{new_status}',
        {completed_clause},
        {err_clause}
    WHERE source_hour = '{source_hour}' AND run_id = '{run_id}'
    """
    spark.sql(update_sql)


def complete_run(
    spark: SparkSession,
    source_hour: str,
    run_id: str,
    raw_count: int,
    bronze_count: int,
    quarantine_count: int,
) -> None:
    """Marks the ingestion run as BRONZE_COMPLETE with record stage metrics."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # noqa: UP017
    update_sql = f"""
    UPDATE iceberg.control.ingestion_runs
    SET status = 'BRONZE_COMPLETE',
        completed_at = TIMESTAMP '{now_iso}',
        raw_record_count = {raw_count},
        bronze_record_count = {bronze_count},
        quarantine_record_count = {quarantine_count},
        error_message = NULL
    WHERE source_hour = '{source_hour}' AND run_id = '{run_id}'
    """
    spark.sql(update_sql)


def fail_run(
    spark: SparkSession,
    source_hour: str,
    run_id: str,
    error_message: str,
) -> None:
    """Marks the ingestion run as FAILED with an error explanation."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # noqa: UP017
    safe_error = error_message.replace("'", "''")[:1000]
    update_sql = f"""
    UPDATE iceberg.control.ingestion_runs
    SET status = 'FAILED',
        completed_at = TIMESTAMP '{now_iso}',
        error_message = '{safe_error}'
    WHERE source_hour = '{source_hour}' AND run_id = '{run_id}'
    """
    spark.sql(update_sql)
