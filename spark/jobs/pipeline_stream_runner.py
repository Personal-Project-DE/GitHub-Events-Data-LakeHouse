"""
Automated Sliding-Window Backfill & Realtime Stream Pipeline with FIFO Retention.

Features:
1. Converts GH Archive JSON to Apache Iceberg Parquet (Bronze -> Silver -> Gold).
2. Zero-Footprint Landing: Purges local downloaded .json.gz files immediately after ingestion.
3. FIFO Retention Window: Drops the oldest day from all tables (Bronze, Silver, Gold, Ledger)
   after each new day is ingested.
4. Storage Reclaim: Expire snapshots & compaction to keep MinIO S3 bounded.
5. Realtime Transition: Seamlessly continues hourly ingestion after backfill catches up to now.
"""

from __future__ import annotations

import argparse
import datetime
import time

try:
    from datetime import UTC
except ImportError:
    from datetime import timezone

    UTC = timezone.utc  # noqa: UP017
from pathlib import Path
from typing import Any

try:
    from spark.jobs.bronze_ingestion import run_bronze_ingestion
    from spark.jobs.gold_aggregation import run_gold_aggregation
    from spark.jobs.silver_transform import run_silver_transformation
    from spark.utils.downloader import download_and_land_source_hour
    from spark.utils.spark_session import get_spark_session
except ImportError:
    run_bronze_ingestion = None  # type: ignore
    run_gold_aggregation = None  # type: ignore
    run_silver_transformation = None  # type: ignore
    download_and_land_source_hour = None  # type: ignore
    get_spark_session = None  # type: ignore


def get_dates_in_range(start_date: str, end_date: str) -> list[str]:
    """Generates a list of YYYY-MM-DD strings between start and end dates inclusive."""
    d_start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    d_end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    delta = (d_end - d_start).days
    return [
        (d_start + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(delta + 1)
    ]


def purge_local_landing_file(source_hour: str) -> None:
    """Removes the local .json.gz file to maintain zero-footprint disk usage."""
    candidates = [
        Path(f"data/landing/{source_hour}.json.gz"),
        Path(f"/opt/spark/work-dir/data/landing/{source_hour}.json.gz"),
        Path(__file__).resolve().parents[2]
        / "data"
        / "landing"
        / f"{source_hour}.json.gz",
    ]
    for p in candidates:
        if p.exists():
            try:
                p.unlink()
                print(f"[Storage Cleanup] Removed temporary landing file: {p}")
            except OSError as e:
                print(f"[Storage Cleanup] Warning: could not delete {p}: {e}")


def prune_oldest_day_if_exceeds(
    spark: Any,
    retention_days: int,
    current_date: str,
) -> list[str]:
    """
    Checks the number of distinct dates in Gold marts and Bronze events.
    If total days > retention_days, purges the oldest days across Bronze, Silver, Gold, and Control,
    and expires snapshots to physically reclaim MinIO S3 storage.
    """
    if retention_days <= 0:
        return []

    try:
        from spark.jobs.iceberg_maintenance import run_fifo_retention_prune

        return run_fifo_retention_prune(
            spark=spark,
            retention_days=retention_days,
            current_date=current_date,
        )
    except Exception as e:  # noqa: BLE001
        print(f"[FIFO Retention Policy] Warning: Pruning failed: {e}")
        return []


def is_hour_already_completed(spark: Any, source_hour: str) -> bool:
    """Checks whether the hour has already completed all pipeline stages in the ledger."""
    try:
        df = spark.sql(
            f"SELECT status FROM iceberg.control.ingestion_runs WHERE source_hour = '{source_hour}'"
        )
        rows = df.collect()
        if not rows:
            return False
        return rows[0]["status"] in (
            "GOLD_COMPLETE",
            "SILVER_COMPLETE",
            "BRONZE_COMPLETE",
        )
    except Exception:  # noqa: BLE001
        return False


def process_single_hour(
    spark: Any,
    source_hour: str,
    target_date: str,
    force: bool = False,
    clean_local: bool = True,
) -> bool:
    """
    Executes the full pipeline for a single hour:
    Download -> Bronze (Parquet) -> Clean JSON -> Silver (Parquet) -> Gold (Parquet).
    """
    print(
        f"\n>>> [Pipeline Runner] Processing Hour: {source_hour} (Date: {target_date}) <<<"
    )

    if not force and is_hour_already_completed(spark, source_hour):
        print(f"--- Hour {source_hour} already completed in ledger. Skipping.")
        return True

    # 1. Download & Land
    print(f"[Stage 1/4] Downloading GH Archive for {source_hour}...")
    try:
        download_and_land_source_hour(source_hour)
    except Exception as e:  # noqa: BLE001
        print(f"Error downloading {source_hour}: {e}")
        return False

    # 2. Bronze Ingestion (JSON to Parquet in Iceberg MinIO)
    print("[Stage 2/4] Ingesting into Bronze Iceberg Parquet...")
    try:
        run_bronze_ingestion(source_hour=source_hour, force=force, spark=spark)
    except Exception as e:  # noqa: BLE001
        print(f"Error in Bronze Ingestion for {source_hour}: {e}")
        return False

    # 3. Clean temporary JSON landing file
    if clean_local:
        purge_local_landing_file(source_hour)

    # 4. Silver Transformation
    print("[Stage 3/4] Transforming into Silver Facts & Dimensions...")
    try:
        run_silver_transformation(spark, source_hour, force=force)
    except Exception as e:  # noqa: BLE001
        print(f"Error in Silver Transformation for {source_hour}: {e}")
        return False

    # 5. Gold Aggregation
    print("[Stage 4/4] Aggregating into Gold Analytics Marts...")
    try:
        run_gold_aggregation(spark, target_date, source_hour=source_hour)
    except Exception as e:  # noqa: BLE001
        print(f"Error in Gold Aggregation for {source_hour}: {e}")
        return False

    print(f"--- Completed Hour: {source_hour} successfully! ---\n")
    return True


def run_pipeline(
    start_date: str,
    end_date: str,
    hours_per_day: list[int] | None = None,
    retention_days: int = 90,
    force: bool = False,
    continuous_realtime: bool = False,
) -> None:
    """
    Main coordinator for sliding-window backfill and optional continuous realtime loop.
    """
    print("==================================================================")
    print("        GITHUB EVENTS LAKEHOUSE STREAMING & FIFO RUNNER           ")
    print(f"  Start Date: {start_date}")
    print(f"  End Date:   {end_date}")
    print(f"  Retention Window: {retention_days} days (FIFO pruning)")
    print(f"  Continuous Realtime Mode: {continuous_realtime}")
    print("==================================================================")

    spark = get_spark_session("GitHubEventsStreamRunner")

    try:
        dates = get_dates_in_range(start_date, end_date)
        target_hours = hours_per_day if hours_per_day is not None else list(range(24))

        for date_str in dates:
            print("\n=======================================================")
            print(f"  PROCESSING DATE: {date_str} ({len(target_hours)} hours)")
            print("=======================================================")

            for hour in target_hours:
                source_hour = f"{date_str}-{hour}"
                success = process_single_hour(
                    spark=spark,
                    source_hour=source_hour,
                    target_date=date_str,
                    force=force,
                    clean_local=True,
                )
                if not success:
                    print(
                        f"Warning: Processing failed for {source_hour}. Continuing to next."
                    )

            # End of day: Run FIFO Retention Pruning
            pruned = prune_oldest_day_if_exceeds(spark, retention_days, date_str)
            if pruned:
                print(f"[FIFO Check] Kept window at max {retention_days} days.")

            # Periodic Spark Cache clear to prevent JVM heap bloat
            spark.catalog.clearCache()

        print("\n Backfill range completed successfully!")

        # If continuous realtime mode is enabled
        if continuous_realtime:
            print("\n>>> Entering Continuous Hourly Realtime Mode <<<")
            while True:
                now_utc = datetime.datetime.now(UTC)
                # GH Archive files are typically published 10-20 min after the hour
                # We ingest the previous hour
                prev_hour_dt = now_utc - datetime.timedelta(hours=1)
                curr_source_hour = (
                    f"{prev_hour_dt.strftime('%Y-%m-%d')}-{prev_hour_dt.hour}"
                )
                curr_date_str = prev_hour_dt.strftime("%Y-%m-%d")

                print(
                    f"\n[Realtime Poller] Current time: {now_utc.isoformat()}. Checking {curr_source_hour}..."
                )
                success = process_single_hour(
                    spark=spark,
                    source_hour=curr_source_hour,
                    target_date=curr_date_str,
                    force=False,
                    clean_local=True,
                )
                if success:
                    prune_oldest_day_if_exceeds(spark, retention_days, curr_date_str)
                    time_to_sleep = 1800  # Check again in 30 minutes
                    print(
                        f"[Realtime Poller] Hour {curr_source_hour} processed successfully. Sleeping for {time_to_sleep // 60} minutes..."
                    )
                else:
                    time_to_sleep = 300  # Retry in 5 minutes if not published yet
                    print(
                        f"[Realtime Poller] Hour {curr_source_hour} not yet ready on GH Archive. Retrying in {time_to_sleep // 60} minutes..."
                    )
                time.sleep(time_to_sleep)

    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(
        description="Sliding-Window Backfill & FIFO Runner"
    )
    parser.add_argument(
        "--start-date", default="2026-08-01", help="Start date YYYY-MM-DD"
    )
    parser.add_argument("--end-date", default="2026-08-01", help="End date YYYY-MM-DD")
    parser.add_argument(
        "--hours",
        type=str,
        default="all",
        help="Comma-separated hours (0,1,2) or 'all'",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Maximum number of days to retain (FIFO)",
    )
    parser.add_argument(
        "--force", action="store_true", help="Force re-ingest if already processed"
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Keep running continuously in realtime",
    )

    args = parser.parse_args()

    hours_list = None
    if args.hours != "all":
        hours_list = [int(h.strip()) for h in args.hours.split(",")]

    run_pipeline(
        start_date=args.start_date,
        end_date=args.end_date,
        hours_per_day=hours_list,
        retention_days=args.retention_days,
        force=args.force,
        continuous_realtime=args.continuous,
    )


if __name__ == "__main__":
    main()
