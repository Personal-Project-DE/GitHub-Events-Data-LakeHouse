from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

try:
    from datetime import UTC
except ImportError:
    UTC = timezone.utc  # type: ignore # noqa: UP017
from pathlib import Path
from typing import Any

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = Any  # type: ignore

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Default maintenance configurations
DEFAULT_TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB
DEFAULT_MIN_FILE_SIZE_BYTES = 32 * 1024 * 1024  # 32 MB
DEFAULT_MAX_FILE_SIZE_BYTES = 256 * 1024 * 1024  # 256 MB
DEFAULT_RETAIN_LAST_SNAPSHOTS = 5
DEFAULT_OLDER_THAN_DAYS = 3

TABLE_MAINTENANCE_PROFILES: dict[str, dict[str, Any]] = {
    "iceberg.bronze.github_events": {
        "strategy": "sort",
        "sort_order": "created_at ASC, event_type ASC",
    },
    "iceberg.silver.dim_users": {
        "strategy": "sort",
        "sort_order": "user_id ASC",
    },
    "iceberg.silver.dim_repositories": {
        "strategy": "sort",
        "sort_order": "repo_id ASC",
    },
    "iceberg.silver.repo_metadata": {
        "strategy": "sort",
        "sort_order": "repo_id ASC",
    },
    "iceberg.silver.fact_commits": {
        "strategy": "sort",
        "sort_order": "created_at ASC, repo_id ASC",
    },
    "iceberg.silver.fact_pull_requests": {
        "strategy": "sort",
        "sort_order": "created_at ASC, repo_id ASC",
    },
    "iceberg.silver.fact_engagements": {
        "strategy": "sort",
        "sort_order": "created_at ASC, repo_id ASC",
    },
    "iceberg.gold.mart_trending_repos_daily": {
        "strategy": "sort",
        "sort_order": "metric_date DESC, new_stars_count DESC",
    },
    "iceberg.gold.mart_language_trends_daily": {
        "strategy": "sort",
        "sort_order": "metric_date DESC, active_repos_count DESC",
    },
    "iceberg.gold.mart_developer_work_patterns_daily": {
        "strategy": "sort",
        "sort_order": "metric_date DESC, day_of_week ASC, hour_of_day ASC",
    },
}

FIFO_RETENTION_TABLES: list[str] = [
    "iceberg.gold.mart_trending_repos_daily",
    "iceberg.gold.mart_language_trends_daily",
    "iceberg.gold.mart_developer_work_patterns_daily",
    "iceberg.silver.fact_engagements",
    "iceberg.silver.fact_commits",
    "iceberg.silver.fact_pull_requests",
    "iceberg.bronze.github_events",
    "iceberg.bronze.github_events_quarantine",
    "iceberg.control.ingestion_runs",
]


def get_existing_dates(spark: SparkSession) -> list[str]:
    """Retrieves all distinct dates currently present across Gold marts or Bronze events."""
    try:
        df_dates = spark.sql(
            "SELECT DISTINCT metric_date FROM iceberg.gold.mart_trending_repos_daily ORDER BY metric_date ASC"
        )
        dates = [str(r["metric_date"]) for r in df_dates.collect() if r["metric_date"]]
        if dates:
            return sorted(set(dates))
    except Exception:  # noqa: S110, BLE001
        pass

    try:
        df_dates = spark.sql(
            "SELECT DISTINCT to_date(created_at) AS metric_date FROM iceberg.bronze.github_events WHERE created_at IS NOT NULL ORDER BY metric_date ASC"
        )
        dates = [str(r["metric_date"]) for r in df_dates.collect() if r["metric_date"]]
        if dates:
            return sorted(set(dates))
    except Exception:  # noqa: S110, BLE001
        pass

    try:
        df_dates = spark.sql(
            "SELECT DISTINCT substring(source_hour, 1, 10) AS metric_date FROM iceberg.control.ingestion_runs ORDER BY metric_date ASC"
        )
        dates = [str(r["metric_date"]) for r in df_dates.collect() if r["metric_date"]]
        return sorted(set(dates))
    except Exception:  # noqa: BLE001
        return []


def prune_date_from_all_tiers(
    spark: SparkSession,
    target_date: str,
    reclaim_storage: bool = True,
    dry_run: bool = False,
) -> bool:
    """
    Cascades deletion of target_date across Bronze, Silver, Gold, and Ledger,
    followed by snapshot expiration to physically reclaim MinIO S3 storage.
    """
    print("\n=======================================================")
    print(f" [FIFO PRUNING] Purging data for date: {target_date} ")
    print("=======================================================")

    delete_stmts = [
        # Gold
        f"DELETE FROM iceberg.gold.mart_trending_repos_daily WHERE metric_date = DATE '{target_date}'",
        f"DELETE FROM iceberg.gold.mart_language_trends_daily WHERE metric_date = DATE '{target_date}'",
        f"DELETE FROM iceberg.gold.mart_developer_work_patterns_daily WHERE metric_date = DATE '{target_date}'",
        # Silver
        f"DELETE FROM iceberg.silver.fact_engagements WHERE to_date(created_at) = DATE '{target_date}'",
        f"DELETE FROM iceberg.silver.fact_commits WHERE to_date(committed_at) = DATE '{target_date}'",
        f"DELETE FROM iceberg.silver.fact_pull_requests WHERE to_date(created_at) = DATE '{target_date}'",
        # Bronze
        f"DELETE FROM iceberg.bronze.github_events WHERE to_date(created_at) = DATE '{target_date}'",
        f"DELETE FROM iceberg.bronze.github_events_quarantine WHERE to_date(created_at) = DATE '{target_date}'",
        # Control Ledger
        f"DELETE FROM iceberg.control.ingestion_runs WHERE source_hour LIKE '{target_date}-%'",
    ]

    for stmt in delete_stmts:
        print(f"  Executing: {stmt}")
        if not dry_run:
            try:
                spark.sql(stmt)
            except Exception as e:  # noqa: BLE001
                print(f"  [Warning] Statement failed: {stmt} -> {e}")

    if reclaim_storage and not dry_run:
        print("\n  [Storage Reclaim] Expiring snapshots to release MinIO S3 space...")
        for tbl in FIFO_RETENTION_TABLES:
            try:
                expire_sql = f"CALL iceberg.system.expire_snapshots(table => '{tbl}', retain_last => 1)"
                spark.sql(expire_sql)
            except Exception:  # noqa: S110, BLE001
                # Table might be empty or not yet have multiple snapshots
                pass

    print(f" Successfully pruned date: {target_date} across all Lakehouse tiers.\n")
    return True


def run_fifo_retention_prune(
    spark: SparkSession,
    retention_days: int = 90,
    current_date: str | None = None,
    dry_run: bool = False,
) -> list[str]:
    """
    FIFO Sliding-Window Retention:
    If total stored dates exceed retention_days, purges the oldest days until window <= retention_days.
    Returns the list of pruned dates.
    """
    if retention_days <= 0:
        return []

    dates = get_existing_dates(spark)
    if not dates:
        print("[FIFO Retention] No historical dates found in Lakehouse.")
        return []

    pruned: list[str] = []
    while len(dates) > retention_days:
        oldest_date = dates[0]
        if oldest_date == current_date:
            break
        prune_date_from_all_tiers(
            spark=spark,
            target_date=oldest_date,
            reclaim_storage=True,
            dry_run=dry_run,
        )
        pruned.append(oldest_date)
        dates.pop(0)

    if pruned:
        print(
            f"[FIFO Retention] Pruned {len(pruned)} expired dates: {pruned}. Active dates remaining: {len(dates)}"
        )
    else:
        print(
            f"[FIFO Retention] Current dates count ({len(dates)}) is within retention limit ({retention_days})."
        )

    return pruned


def canonicalize_table_name(table_name: str) -> str:
    """Ensures table name includes catalog prefix 'iceberg.'"""
    if not table_name.startswith("iceberg."):
        return f"iceberg.{table_name}"
    return table_name


def build_rewrite_data_files_sql(
    table: str,
    strategy: str = "binpack",
    sort_order: str | None = None,
    target_file_size_bytes: int = DEFAULT_TARGET_FILE_SIZE_BYTES,
    min_file_size_bytes: int = DEFAULT_MIN_FILE_SIZE_BYTES,
    max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES,
    where_clause: str | None = None,
) -> str:
    """Constructs Iceberg Spark SQL procedure for rewrite_data_files."""
    full_table = canonicalize_table_name(table)
    params = [f"table => '{full_table}'", f"strategy => '{strategy}'"]

    if strategy.lower() == "sort" and sort_order:
        params.append(f"sort_order => '{sort_order}'")

    options_map = (
        f"map('target-file-size-bytes', '{target_file_size_bytes}', "
        f"'min-file-size-bytes', '{min_file_size_bytes}', "
        f"'max-file-size-bytes', '{max_file_size_bytes}')"
    )
    params.append(f"options => {options_map}")

    if where_clause:
        params.append(f"where => '{where_clause}'")

    return f"CALL iceberg.system.rewrite_data_files({', '.join(params)})"


def build_expire_snapshots_sql(
    table: str,
    older_than_ts: str | None = None,
    retain_last: int = DEFAULT_RETAIN_LAST_SNAPSHOTS,
) -> str:
    """Constructs Iceberg Spark SQL procedure for expire_snapshots."""
    full_table = canonicalize_table_name(table)
    params = [f"table => '{full_table}'"]
    if older_than_ts:
        params.append(f"older_than => TIMESTAMP '{older_than_ts}'")
    if retain_last > 0:
        params.append(f"retain_last => {retain_last}")

    return f"CALL iceberg.system.expire_snapshots({', '.join(params)})"


def build_remove_orphan_files_sql(
    table: str,
    older_than_ts: str | None = None,
) -> str:
    """Constructs Iceberg Spark SQL procedure for remove_orphan_files."""
    full_table = canonicalize_table_name(table)
    params = [f"table => '{full_table}'"]
    if older_than_ts:
        params.append(f"older_than => TIMESTAMP '{older_than_ts}'")

    return f"CALL iceberg.system.remove_orphan_files({', '.join(params)})"


def build_rewrite_manifests_sql(table: str) -> str:
    """Constructs Iceberg Spark SQL procedure for rewrite_manifests."""
    full_table = canonicalize_table_name(table)
    return f"CALL iceberg.system.rewrite_manifests(table => '{full_table}')"


def run_compaction(
    spark: SparkSession,
    table: str,
    strategy: str = "binpack",
    sort_order: str | None = None,
    target_file_size_bytes: int = DEFAULT_TARGET_FILE_SIZE_BYTES,
    min_file_size_bytes: int = DEFAULT_MIN_FILE_SIZE_BYTES,
    max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES,
    where_clause: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Runs data file rewriting (compaction / bin-pack / sort) on the specified Iceberg table."""
    full_table = canonicalize_table_name(table)
    sql_stmt = build_rewrite_data_files_sql(
        table=full_table,
        strategy=strategy,
        sort_order=sort_order,
        target_file_size_bytes=target_file_size_bytes,
        min_file_size_bytes=min_file_size_bytes,
        max_file_size_bytes=max_file_size_bytes,
        where_clause=where_clause,
    )
    print(f"\n[Maintenance - Compaction] Table: {full_table}")
    print(f"  Executing SQL: {sql_stmt}")

    if dry_run:
        print("  [DRY RUN] Skipping actual execution.")
        return {"status": "dry_run", "sql": sql_stmt}

    try:
        result_rows = spark.sql(sql_stmt).collect()
        summary = {}
        if result_rows:
            row_dict = result_rows[0].asDict()
            summary = row_dict
            print(f"  -> Compaction result: {summary}")
        return {"status": "success", "summary": summary, "sql": sql_stmt}
    except Exception as exc:  # noqa: BLE001
        print(f"  [ERROR] Compaction failed for {full_table}: {exc}")
        return {"status": "failed", "error": str(exc), "sql": sql_stmt}


def run_expire_snapshots(
    spark: SparkSession,
    table: str,
    older_than_ts: str | None = None,
    retain_last: int = DEFAULT_RETAIN_LAST_SNAPSHOTS,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Expires snapshots for an Iceberg table based on age and count thresholds."""
    full_table = canonicalize_table_name(table)
    sql_stmt = build_expire_snapshots_sql(
        table=full_table,
        older_than_ts=older_than_ts,
        retain_last=retain_last,
    )
    print(f"\n[Maintenance - Expire Snapshots] Table: {full_table}")
    print(f"  Executing SQL: {sql_stmt}")

    if dry_run:
        print("  [DRY RUN] Skipping actual execution.")
        return {"status": "dry_run", "sql": sql_stmt}

    try:
        result_rows = spark.sql(sql_stmt).collect()
        deleted_count = 0
        if result_rows:
            deleted_count = result_rows[0][0] if len(result_rows[0]) > 0 else 0
        print(f"  -> Expired snapshots: {deleted_count}")
        return {
            "status": "success",
            "deleted_snapshots": deleted_count,
            "sql": sql_stmt,
        }
    except Exception as exc:  # noqa: BLE001
        print(f"  [ERROR] Expire snapshots failed for {full_table}: {exc}")
        return {"status": "failed", "error": str(exc), "sql": sql_stmt}


def run_remove_orphan_files(
    spark: SparkSession,
    table: str,
    older_than_ts: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Removes orphan data files for an Iceberg table."""
    full_table = canonicalize_table_name(table)
    sql_stmt = build_remove_orphan_files_sql(
        table=full_table, older_than_ts=older_than_ts
    )
    print(f"\n[Maintenance - Remove Orphan Files] Table: {full_table}")
    print(f"  Executing SQL: {sql_stmt}")

    if dry_run:
        print("  [DRY RUN] Skipping actual execution.")
        return {"status": "dry_run", "sql": sql_stmt}

    try:
        result_rows = spark.sql(sql_stmt).collect()
        orphan_count = len(result_rows)
        print(f"  -> Removed orphan files count: {orphan_count}")
        return {"status": "success", "removed_count": orphan_count, "sql": sql_stmt}
    except Exception as exc:  # noqa: BLE001
        print(f"  [ERROR] Remove orphan files failed for {full_table}: {exc}")
        return {"status": "failed", "error": str(exc), "sql": sql_stmt}


def run_rewrite_manifests(
    spark: SparkSession,
    table: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Rewrites manifest files to optimize query planning."""
    full_table = canonicalize_table_name(table)
    sql_stmt = build_rewrite_manifests_sql(table=full_table)
    print(f"\n[Maintenance - Rewrite Manifests] Table: {full_table}")
    print(f"  Executing SQL: {sql_stmt}")

    if dry_run:
        print("  [DRY RUN] Skipping actual execution.")
        return {"status": "dry_run", "sql": sql_stmt}

    try:
        spark.sql(sql_stmt).collect()
        print("  -> Manifest rewriting completed successfully.")
        return {"status": "success", "sql": sql_stmt}
    except Exception as exc:  # noqa: BLE001
        print(f"  [ERROR] Rewrite manifests failed for {full_table}: {exc}")
        return {"status": "failed", "error": str(exc), "sql": sql_stmt}


def maintain_table(
    spark: SparkSession,
    table: str,
    strategy: str | None = None,
    sort_order: str | None = None,
    target_file_size_mb: int = 128,
    retain_last: int = DEFAULT_RETAIN_LAST_SNAPSHOTS,
    older_than_days: int = DEFAULT_OLDER_THAN_DAYS,
    skip_expire_snapshots: bool = False,
    skip_orphan_cleanup: bool = False,
    skip_manifest_rewrite: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Runs a complete maintenance cycle for an individual Iceberg table."""
    full_table = canonicalize_table_name(table)
    profile = TABLE_MAINTENANCE_PROFILES.get(full_table, {})

    chosen_strategy = strategy or profile.get("strategy", "binpack")
    chosen_sort_order = (
        sort_order if sort_order is not None else profile.get("sort_order")
    )

    target_bytes = target_file_size_mb * 1024 * 1024
    min_bytes = max(1, target_bytes // 4)
    max_bytes = target_bytes * 2

    cutoff_dt = datetime.now(UTC) - timedelta(days=older_than_days)
    older_than_ts = cutoff_dt.strftime("%Y-%m-%d %H:%M:%S")

    results: dict[str, Any] = {"table": full_table}

    # 1. Compaction / rewrite_data_files
    compaction_res = run_compaction(
        spark=spark,
        table=full_table,
        strategy=chosen_strategy,
        sort_order=chosen_sort_order,
        target_file_size_bytes=target_bytes,
        min_file_size_bytes=min_bytes,
        max_file_size_bytes=max_bytes,
        dry_run=dry_run,
    )
    results["compaction"] = compaction_res

    # 2. Rewrite Manifests
    if not skip_manifest_rewrite:
        results["manifests"] = run_rewrite_manifests(spark, full_table, dry_run=dry_run)

    # 3. Expire Snapshots
    if not skip_expire_snapshots:
        results["expire_snapshots"] = run_expire_snapshots(
            spark=spark,
            table=full_table,
            older_than_ts=older_than_ts,
            retain_last=retain_last,
            dry_run=dry_run,
        )

    # 4. Remove Orphan Files
    if not skip_orphan_cleanup:
        results["orphan_files"] = run_remove_orphan_files(
            spark=spark,
            table=full_table,
            older_than_ts=older_than_ts,
            dry_run=dry_run,
        )

    return results


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apache Iceberg Table Maintenance (Compaction, Expire Snapshots, Clean Orphans)"
    )
    parser.add_argument(
        "--table",
        type=str,
        help="Target table name (e.g. iceberg.gold.mart_trending_repos_daily)",
    )
    parser.add_argument(
        "--all-tables",
        action="store_true",
        help="Run maintenance across all Bronze, Silver, and Gold tables",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        choices=["binpack", "sort"],
        default=None,
        help="Compaction strategy ('binpack' or 'sort')",
    )
    parser.add_argument(
        "--sort-order",
        type=str,
        default=None,
        help="Sort order columns when strategy is 'sort' (e.g. 'metric_date DESC, repo_id ASC')",
    )
    parser.add_argument(
        "--target-file-size-mb",
        type=int,
        default=128,
        help="Target compacted file size in Megabytes (default: 128 MB)",
    )
    parser.add_argument(
        "--retain-last",
        type=int,
        default=DEFAULT_RETAIN_LAST_SNAPSHOTS,
        help="Minimum number of latest snapshots to retain (default: 5)",
    )
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=DEFAULT_OLDER_THAN_DAYS,
        help="Retention window in days for snapshots and orphan files (default: 3)",
    )
    parser.add_argument(
        "--skip-expire-snapshots",
        action="store_true",
        help="Skip snapshot expiration",
    )
    parser.add_argument(
        "--skip-orphan-cleanup",
        action="store_true",
        help="Skip orphan files cleanup",
    )
    parser.add_argument(
        "--skip-manifest-rewrite",
        action="store_true",
        help="Skip manifest rewriting",
    )
    parser.add_argument(
        "--fifo-retention-days",
        type=int,
        default=None,
        help="Enforce FIFO sliding window retention: prune oldest date(s) if count > N",
    )
    parser.add_argument(
        "--prune-date",
        type=str,
        default=None,
        help="Explicitly prune a specific date YYYY-MM-DD from all medallion tables",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned SQL procedure calls without executing them",
    )
    return parser.parse_args(args)


def main(cli_args: list[str] | None = None) -> None:
    args = parse_args(cli_args)

    if (
        not args.table
        and not args.all_tables
        and not args.fifo_retention_days
        and not args.prune_date
    ):
        print(
            "[ERROR] Please provide --table <name>, --all-tables, --fifo-retention-days <N>, or --prune-date <YYYY-MM-DD>."
        )
        sys.exit(1)

    if not args.dry_run:
        from spark.utils.spark_session import get_spark_session

        spark = get_spark_session(app_name="IcebergTableMaintenance")
    else:
        spark = None

    # Handle explicit date pruning if requested
    if args.prune_date:
        prune_date_from_all_tiers(
            spark=spark,
            target_date=args.prune_date,
            reclaim_storage=True,
            dry_run=args.dry_run,
        )

    # Handle FIFO retention sliding window if requested
    if args.fifo_retention_days is not None:
        run_fifo_retention_prune(
            spark=spark,
            retention_days=args.fifo_retention_days,
            dry_run=args.dry_run,
        )

    tables_to_maintain: list[str] = []
    if args.all_tables:
        tables_to_maintain = list(TABLE_MAINTENANCE_PROFILES.keys())
    elif args.table:
        tables_to_maintain = [canonicalize_table_name(args.table)]

    if tables_to_maintain:
        print("==================================================================")
        print("      APACHE ICEBERG TABLE MAINTENANCE & OPTIMIZATION PIPELINE    ")
        print(f"  Target Tables: {len(tables_to_maintain)}")
        print(f"  Target File Size: {args.target_file_size_mb} MB")
        print(f"  Retain Last Snapshots: {args.retain_last}")
        print(f"  Snapshot Retention Window: {args.older_than_days} days")
        print(f"  Dry Run: {args.dry_run}")
        print("==================================================================")

        maintenance_summary = []
        for tbl in tables_to_maintain:
            res = maintain_table(
                spark=spark,
                table=tbl,
                strategy=args.strategy,
                sort_order=args.sort_order,
                target_file_size_mb=args.target_file_size_mb,
                retain_last=args.retain_last,
                older_than_days=args.older_than_days,
                skip_expire_snapshots=args.skip_expire_snapshots,
                skip_orphan_cleanup=args.skip_orphan_cleanup,
                skip_manifest_rewrite=args.skip_manifest_rewrite,
                dry_run=args.dry_run,
            )
            maintenance_summary.append(res)

    print("\n==================================================================")
    print("             MAINTENANCE CYCLE FINISHED SUCCESSFULLY             ")
    print("==================================================================")


if __name__ == "__main__":
    main()
