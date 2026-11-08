from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None  # type: ignore

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


class QualityGateError(Exception):
    """Raised when a data quality gate check fails."""


def validate_bronze_gate(spark: SparkSession, source_hour: str) -> None:
    """Bronze Quality Gate: Zero null event_id and zero duplicate event_id."""
    print(f"[Quality Gate] Validating Bronze for source_hour: {source_hour}")
    df = spark.sql(
        f"""
        SELECT
            COUNT(*) AS total_count,
            COUNT(DISTINCT event_id) AS distinct_keys,
            COUNT(CASE WHEN event_id IS NULL THEN 1 END) AS null_keys
        FROM iceberg.bronze.github_events
        WHERE source_hour = '{source_hour}'
        """
    ).collect()[0]

    total_count = df["total_count"]
    distinct_keys = df["distinct_keys"]
    null_keys = df["null_keys"]

    if null_keys > 0:
        raise QualityGateError(
            f"Bronze Gate Failed: Found {null_keys} null event_id values for source_hour {source_hour}"
        )

    if total_count != distinct_keys:
        raise QualityGateError(
            f"Bronze Gate Failed: Total records ({total_count}) != distinct keys ({distinct_keys}) for source_hour {source_hour}"
        )

    print(
        f"  -> Bronze Gate Passed: {total_count} records, {distinct_keys} unique keys, 0 nulls."
    )


def validate_silver_gate(spark: SparkSession, source_hour: str) -> None:
    """Silver Quality Gate: Unique dimension keys, unique fact event_ids, required non-null fields."""
    print(f"[Quality Gate] Validating Silver for source_hour: {source_hour}")

    # 1. Dim Users uniqueness
    user_dups = spark.sql(
        """
        SELECT user_id, COUNT(*) AS cnt
        FROM iceberg.silver.dim_users
        GROUP BY user_id
        HAVING COUNT(*) > 1
        """
    ).count()
    if user_dups > 0:
        raise QualityGateError(
            f"Silver Gate Failed: Found {user_dups} duplicate user_id in dim_users"
        )

    # 2. Dim Repositories uniqueness
    repo_dups = spark.sql(
        """
        SELECT repo_id, COUNT(*) AS cnt
        FROM iceberg.silver.dim_repositories
        GROUP BY repo_id
        HAVING COUNT(*) > 1
        """
    ).count()
    if repo_dups > 0:
        raise QualityGateError(
            f"Silver Gate Failed: Found {repo_dups} duplicate repo_id in dim_repositories"
        )

    # 3. Facts uniqueness and required fields for source_hour
    for fact_table in ["fact_commits", "fact_pull_requests", "fact_engagements"]:
        fact_check = spark.sql(
            f"""
            SELECT
                COUNT(*) AS total_count,
                COUNT(DISTINCT event_id) AS distinct_keys,
                COUNT(CASE WHEN event_id IS NULL THEN 1 END) AS null_events,
                COUNT(CASE WHEN repo_id IS NULL THEN 1 END) AS null_repos,
                COUNT(CASE WHEN actor_id IS NULL THEN 1 END) AS null_actors,
                COUNT(CASE WHEN created_at IS NULL THEN 1 END) AS null_timestamps
            FROM iceberg.silver.{fact_table}
            WHERE source_hour = '{source_hour}'
            """
        ).collect()[0]

        if (
            fact_check["null_events"] > 0
            or fact_check["null_repos"] > 0
            or fact_check["null_actors"] > 0
            or fact_check["null_timestamps"] > 0
        ):
            raise QualityGateError(
                f"Silver Gate Failed: Found null required fields in {fact_table} for source_hour {source_hour}: {fact_check.asDict()}"
            )

        if fact_check["total_count"] != fact_check["distinct_keys"]:
            raise QualityGateError(
                f"Silver Gate Failed: Duplicate event_id in {fact_table} for source_hour {source_hour}: total={fact_check['total_count']}, distinct={fact_check['distinct_keys']}"
            )

    print(
        "  -> Silver Gate Passed: Dimensions and facts are unique with all required fields present."
    )


def validate_gold_gate(spark: SparkSession, target_date: str) -> None:
    """Gold Quality Gate: Unique grains across all 3 marts & Silver-to-Gold reconciliations."""
    print(f"[Quality Gate] Validating Gold for metric_date: {target_date}")

    # 1. Uniqueness check for mart_trending_repos_daily: (metric_date, repo_id)
    trending_dups = spark.sql(
        f"""
        SELECT metric_date, repo_id, COUNT(*) AS cnt
        FROM iceberg.gold.mart_trending_repos_daily
        WHERE metric_date = DATE '{target_date}'
        GROUP BY metric_date, repo_id
        HAVING COUNT(*) > 1
        """
    ).count()
    if trending_dups > 0:
        raise QualityGateError(
            f"Gold Gate Failed: Found {trending_dups} duplicate (metric_date, repo_id) in mart_trending_repos_daily"
        )

    # 2. Uniqueness check for mart_language_trends_daily: (metric_date, primary_language)
    lang_dups = spark.sql(
        f"""
        SELECT metric_date, primary_language, COUNT(*) AS cnt
        FROM iceberg.gold.mart_language_trends_daily
        WHERE metric_date = DATE '{target_date}'
        GROUP BY metric_date, primary_language
        HAVING COUNT(*) > 1
        """
    ).count()
    if lang_dups > 0:
        raise QualityGateError(
            f"Gold Gate Failed: Found {lang_dups} duplicate (metric_date, primary_language) in mart_language_trends_daily"
        )

    # 3. Uniqueness check for mart_developer_work_patterns_daily: (metric_date, day_of_week, hour_of_day)
    pattern_dups = spark.sql(
        f"""
        SELECT metric_date, day_of_week, hour_of_day, COUNT(*) AS cnt
        FROM iceberg.gold.mart_developer_work_patterns_daily
        WHERE metric_date = DATE '{target_date}'
        GROUP BY metric_date, day_of_week, hour_of_day
        HAVING COUNT(*) > 1
        """
    ).count()
    if pattern_dups > 0:
        raise QualityGateError(
            f"Gold Gate Failed: Found {pattern_dups} duplicate grains in mart_developer_work_patterns_daily"
        )

    # 4. Silver-to-Gold Reconciliation: Stars count
    silver_stars = spark.sql(
        f"""
        SELECT COUNT(*) AS total_stars
        FROM iceberg.silver.fact_engagements
        WHERE CAST(created_at AS DATE) = DATE '{target_date}'
          AND engagement_type = 'star'
          AND action = 'started'
        """
    ).collect()[0]["total_stars"]

    trending_gold_stars = spark.sql(
        f"""
        SELECT CAST(COALESCE(SUM(new_stars_count), 0) AS BIGINT) AS total_stars
        FROM iceberg.gold.mart_trending_repos_daily
        WHERE metric_date = DATE '{target_date}'
        """
    ).collect()[0]["total_stars"]

    lang_gold_stars = spark.sql(
        f"""
        SELECT CAST(COALESCE(SUM(new_stars_count), 0) AS BIGINT) AS total_stars
        FROM iceberg.gold.mart_language_trends_daily
        WHERE metric_date = DATE '{target_date}'
        """
    ).collect()[0]["total_stars"]

    if silver_stars != trending_gold_stars:
        raise QualityGateError(
            f"Gold Gate Failed Reconciliation: Silver stars ({silver_stars}) != Trending Mart stars ({trending_gold_stars})"
        )

    if trending_gold_stars != lang_gold_stars:
        raise QualityGateError(
            f"Gold Gate Failed Reconciliation: Trending Mart stars ({trending_gold_stars}) != Language Mart stars ({lang_gold_stars})"
        )

    print(
        f"  -> Gold Gate Passed: All 3 Mart grains are strictly unique; Stars reconcile: {silver_stars} == {trending_gold_stars} == {lang_gold_stars}"
    )


def validate_volume_anomaly(
    spark: SparkSession,
    source_hour: str,
    min_expected: int = 1,
    max_expected: int = 1000000,
) -> None:
    """Operational Quality Gate: Detects hourly volume anomalies."""
    print(f"[Quality Gate] Validating Volume Anomaly for source_hour: {source_hour}")
    count = spark.sql(
        f"""
        SELECT COUNT(*) AS cnt
        FROM iceberg.bronze.github_events
        WHERE source_hour = '{source_hour}'
        """
    ).collect()[0]["cnt"]

    if count < min_expected:
        raise QualityGateError(
            f"Operational Anomaly Gate Failed: Event count {count} is below minimum threshold {min_expected} for {source_hour}"
        )
    if count > max_expected:
        raise QualityGateError(
            f"Operational Anomaly Gate Failed: Event count {count} exceeds maximum threshold {max_expected} for {source_hour}"
        )

    print(
        f"  -> Operational Anomaly Gate Passed: {count} events within safe bounds [{min_expected}, {max_expected}]."
    )


def run_all_quality_checks(
    spark: SparkSession, source_hour: str, target_date: str
) -> bool:
    """Executes all quality gates in sequence."""
    print(
        f"=== Starting Comprehensive Quality Gates for {source_hour} (Date: {target_date}) ==="
    )
    validate_volume_anomaly(spark, source_hour)
    validate_bronze_gate(spark, source_hour)
    validate_silver_gate(spark, source_hour)
    validate_gold_gate(spark, target_date)
    print("=== All Quality Gates Passed Successfully! ===")
    return True


def main() -> None:
    from spark.utils.spark_session import get_spark_session

    parser = argparse.ArgumentParser(
        description="End-to-end Lakehouse Quality Gates validator."
    )
    parser.add_argument(
        "--source-hour", required=True, help="Source hour in YYYY-MM-DD-H format"
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Optional metric date in YYYY-MM-DD format (derived from source-hour if omitted)",
    )
    args = parser.parse_args()

    date_str = args.date or args.source_hour.rsplit("-", 1)[0]
    spark = get_spark_session(f"QualityGate-{args.source_hour}")
    try:
        run_all_quality_checks(spark, args.source_hour, date_str)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
