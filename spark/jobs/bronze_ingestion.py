from __future__ import annotations

import argparse
import hashlib
import sys
import uuid
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from spark.utils.control_ledger import (
    acquire_run_lock,
    complete_run,
    ensure_control_table,
    fail_run,
)
from spark.utils.path_utils import (
    compute_sha256,
    get_landing_s3_key,
    parse_source_hour,
)
from spark.utils.spark_session import get_spark_session


def ensure_bronze_tables(spark: SparkSession) -> None:
    """Creates bronze namespace and target Iceberg tables if not exist."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

    # Main Bronze events table, partitioned by hours(created_at)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.github_events (
            event_id STRING NOT NULL,
            record_hash STRING NOT NULL,
            event_type STRING,
            actor_id BIGINT,
            actor_login STRING,
            repo_id BIGINT,
            repo_name STRING,
            created_at TIMESTAMP,
            raw_payload STRING,
            source_hour STRING NOT NULL,
            source_path STRING NOT NULL,
            source_checksum STRING NOT NULL,
            ingestion_run_id STRING NOT NULL,
            ingested_at TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (hours(created_at))
        """
    )

    # Quarantine table for invalid or unparseable records
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.bronze.github_events_quarantine (
            event_id STRING,
            raw_record STRING,
            quarantine_reason STRING NOT NULL,
            source_hour STRING NOT NULL,
            source_path STRING NOT NULL,
            source_checksum STRING NOT NULL,
            ingestion_run_id STRING NOT NULL,
            quarantined_at TIMESTAMP NOT NULL
        )
        USING iceberg
        """
    )


def resolve_landing_metadata(
    source_hour: str,
    override_path: str | None = None,
    override_checksum: str | None = None,
) -> tuple[str, str]:
    """
    Resolves S3 source path and SHA-256 checksum for the landing object.
    Checks candidate local file locations or generates deterministic key hash.
    """
    s3_key = get_landing_s3_key(source_hour)
    bucket = "lakehouse"
    source_path = override_path or f"s3a://{bucket}/{s3_key}"

    if override_checksum:
        return source_path, override_checksum

    candidates = [
        Path(__file__).resolve().parents[2]
        / "data"
        / "landing"
        / f"{source_hour}.json.gz",
        Path("/opt/spark/work-dir/data/landing") / f"{source_hour}.json.gz",
        Path("data/landing") / f"{source_hour}.json.gz",
    ]
    for c in candidates:
        if c.exists():
            return source_path, compute_sha256(c)

    return source_path, hashlib.sha256(source_path.encode("utf-8")).hexdigest()


def ingest_bronze_hour(
    source_hour: str,
    run_id: str | None = None,
    force: bool = False,
    override_path: str | None = None,
    override_checksum: str | None = None,
) -> dict:
    """
    Executes idempotent Bronze ingestion for a single source hour:
    1. Validates landing object & acquires control lock.
    2. Reads raw gzipped NDJSON lines.
    3. Computes deterministic record_hash (SHA-256 of raw line).
    4. Routes records without valid event_id to quarantine table.
    5. Deduplicates valid records in-batch by event_id.
    6. MERGE INTO iceberg.bronze.github_events on event_id.
    7. Updates control.ingestion_runs with status BRONZE_COMPLETE.
    """
    parse_source_hour(source_hour)  # Validate format
    run_id = run_id or str(uuid.uuid4())
    print(
        f"=== Starting Bronze Ingestion for source_hour: {source_hour} (Run ID: {run_id}) ==="
    )

    source_path, source_checksum = resolve_landing_metadata(
        source_hour, override_path, override_checksum
    )
    print(f"Source Landing Path: {source_path}")
    print(f"Source SHA-256 Checksum: {source_checksum}")

    spark = get_spark_session(f"BronzeIngest-{source_hour}-{run_id[:8]}")

    try:
        ensure_control_table(spark)
        ensure_bronze_tables(spark)

        # Acquire lock & register state
        print("Acquiring ingestion lock in iceberg.control.ingestion_runs...")
        acquire_run_lock(
            spark=spark,
            source_hour=source_hour,
            source_path=source_path,
            source_checksum=source_checksum,
            run_id=run_id,
            force=force,
        )

        # Read raw lines
        print(f"Reading raw data from {source_path}...")
        raw_lines_df = spark.read.text(source_path).filter(
            F.length(F.trim(F.col("value"))) > 0
        )
        raw_count = raw_lines_df.count()
        print(f"Total raw lines read: {raw_count}")

        if raw_count == 0:
            print("Warning: Source file contains 0 valid lines.")
            complete_run(spark, source_hour, run_id, 0, 0, 0)
            return {"raw_count": 0, "bronze_count": 0, "quarantine_count": 0}

        # Parse JSON and compute deterministic record_hash
        parsed_df = (
            raw_lines_df.withColumn("record_hash", F.sha2(F.col("value"), 256))
            .withColumn("event_id", F.get_json_object(F.col("value"), "$.id"))
            .withColumn("event_type", F.get_json_object(F.col("value"), "$.type"))
            .withColumn(
                "actor_id",
                F.get_json_object(F.col("value"), "$.actor.id").cast("bigint"),
            )
            .withColumn(
                "actor_login", F.get_json_object(F.col("value"), "$.actor.login")
            )
            .withColumn(
                "repo_id", F.get_json_object(F.col("value"), "$.repo.id").cast("bigint")
            )
            .withColumn("repo_name", F.get_json_object(F.col("value"), "$.repo.name"))
            .withColumn(
                "created_at",
                F.to_timestamp(F.get_json_object(F.col("value"), "$.created_at")),
            )
            .withColumn("raw_payload", F.col("value"))
            .withColumn("source_hour", F.lit(source_hour))
            .withColumn("source_path", F.lit(source_path))
            .withColumn("source_checksum", F.lit(source_checksum))
            .withColumn("ingestion_run_id", F.lit(run_id))
            .withColumn("ingested_at", F.current_timestamp())
        )

        # Split into Valid and Quarantine records
        invalid_filter = F.col("event_id").isNull() | (
            F.length(F.trim(F.col("event_id"))) == 0
        )
        quarantine_df = parsed_df.filter(invalid_filter).select(
            F.col("event_id"),
            F.col("raw_payload").alias("raw_record"),
            F.lit("NULL_OR_EMPTY_EVENT_ID").alias("quarantine_reason"),
            F.col("source_hour"),
            F.col("source_path"),
            F.col("source_checksum"),
            F.col("ingestion_run_id"),
            F.current_timestamp().alias("quarantined_at"),
        )
        quarantine_count = quarantine_df.count()

        if quarantine_count > 0:
            print(f"Quarantining {quarantine_count} invalid records...")
            quarantine_df.writeTo("iceberg.bronze.github_events_quarantine").append()

        valid_df = parsed_df.filter(~invalid_filter)

        # In-batch Deduplication on event_id
        window_spec = Window.partitionBy("event_id").orderBy(
            F.col("created_at").desc_nulls_last()
        )
        deduped_staged_df = (
            valid_df.withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        staged_view = f"staged_{source_hour.replace('-', '_')}_{run_id[:8]}"
        deduped_staged_df.createOrReplaceTempView(staged_view)

        # Execute MERGE INTO Bronze table
        print(
            f"Merging staged events into iceberg.bronze.github_events using view '{staged_view}'..."
        )
        merge_sql = f"""
        MERGE INTO iceberg.bronze.github_events AS target
        USING {staged_view} AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN
            UPDATE SET
                target.record_hash = source.record_hash,
                target.event_type = source.event_type,
                target.actor_id = source.actor_id,
                target.actor_login = source.actor_login,
                target.repo_id = source.repo_id,
                target.repo_name = source.repo_name,
                target.created_at = source.created_at,
                target.raw_payload = source.raw_payload,
                target.source_hour = source.source_hour,
                target.source_path = source.source_path,
                target.source_checksum = source.source_checksum,
                target.ingestion_run_id = source.ingestion_run_id,
                target.ingested_at = source.ingested_at
        WHEN NOT MATCHED THEN
            INSERT (
                event_id, record_hash, event_type, actor_id, actor_login,
                repo_id, repo_name, created_at, raw_payload,
                source_hour, source_path, source_checksum,
                ingestion_run_id, ingested_at
            )
            VALUES (
                source.event_id, source.record_hash, source.event_type, source.actor_id, source.actor_login,
                source.repo_id, source.repo_name, source.created_at, source.raw_payload,
                source.source_hour, source.source_path, source.source_checksum,
                source.ingestion_run_id, source.ingested_at
            )
        """
        spark.sql(merge_sql)

        # Verification query
        bronze_count_df = spark.sql(
            f"SELECT count(*) as count FROM iceberg.bronze.github_events WHERE source_hour = '{source_hour}'"
        )
        bronze_count = bronze_count_df.collect()[0]["count"]

        # Mark complete in control ledger
        complete_run(
            spark=spark,
            source_hour=source_hour,
            run_id=run_id,
            raw_count=raw_count,
            bronze_count=bronze_count,
            quarantine_count=quarantine_count,
        )

        print(f"=== Bronze Ingestion Completed Successfully for {source_hour} ===")
        print(
            f"Raw: {raw_count} | Bronze: {bronze_count} | Quarantine: {quarantine_count}"
        )
        return {
            "source_hour": source_hour,
            "run_id": run_id,
            "raw_count": raw_count,
            "bronze_count": bronze_count,
            "quarantine_count": quarantine_count,
        }

    except Exception as exc:
        print(f"Error during bronze ingestion: {exc}", file=sys.stderr)
        try:
            fail_run(spark, source_hour, run_id, str(exc))
        except Exception as e:  # noqa: BLE001
            print(
                f"Failed to record run failure in control table: {e}", file=sys.stderr
            )
        raise
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion Job")
    parser.add_argument(
        "--source-hour",
        type=str,
        default="2024-01-01-0",
        help="Source hour to ingest (format: YYYY-MM-DD-H)",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Unique identifier for this ingestion run",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reprocessing even if checksum or concurrency lock exists",
    )
    parser.add_argument(
        "--source-path",
        type=str,
        default=None,
        help="Override S3 landing path",
    )
    parser.add_argument(
        "--source-checksum",
        type=str,
        default=None,
        help="Override source SHA-256 checksum",
    )
    args = parser.parse_args()

    ingest_bronze_hour(
        source_hour=args.source_hour,
        run_id=args.run_id,
        force=args.force,
        override_path=args.source_path,
        override_checksum=args.source_checksum,
    )


if __name__ == "__main__":
    main()
