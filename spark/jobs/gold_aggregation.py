from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from spark.utils.control_ledger import (
    advance_run_status,
    ensure_control_table,
    fail_run,
    get_ingestion_run,
)
from spark.utils.spark_session import get_spark_session


def ensure_gold_tables(spark: SparkSession) -> None:
    """Creates the gold namespace and trending repos mart if not exists."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.mart_trending_repos_daily (
            metric_date DATE NOT NULL,
            repo_id BIGINT NOT NULL,
            repo_name STRING NOT NULL,
            primary_language STRING NOT NULL,
            new_stars_count BIGINT NOT NULL,
            forks_count BIGINT NOT NULL,
            opened_prs_count BIGINT NOT NULL,
            merged_prs_count BIGINT NOT NULL,
            distinct_human_contributors BIGINT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (metric_date)
        """
    )


def run_gold_aggregation(
    spark: SparkSession,
    target_date: str,
    source_hour: str | None = None,
    run_id: str | None = None,
) -> int:
    """
    Aggregates Silver data into iceberg.gold.mart_trending_repos_daily for a specific date:
    - Grain: (metric_date, repo_id)
    - Fully reconcilable to Silver facts and dimensions.
    - Excludes bots from distinct_human_contributors.
    - Uses MERGE INTO on (metric_date, repo_id) to guarantee zero duplicate keys upon replay.
    - Advances control ledger to GOLD_COMPLETE.
    """
    ensure_control_table(spark)
    ensure_gold_tables(spark)

    active_hour = source_hour or f"{target_date}-0"
    run_record = get_ingestion_run(spark, active_hour)
    active_run_id = run_id or (
        run_record["run_id"] if run_record else str(uuid.uuid4())
    )

    print(
        f"=== Starting Gold Aggregation for metric_date: {target_date} (Run ID: {active_run_id}) ==="
    )

    if source_hour:
        advance_run_status(spark, source_hour, active_run_id, "GOLD_PROCESSING")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # noqa: UP017

    try:
        # 1. Staging query: Compute daily activity metrics across all Silver facts for target_date
        aggregation_sql = f"""
        WITH daily_stars AS (
            SELECT repo_id, COUNT(*) AS new_stars_count
            FROM iceberg.silver.fact_engagements
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
              AND engagement_type = 'star'
              AND action = 'started'
            GROUP BY repo_id
        ),
        daily_forks AS (
            SELECT repo_id, COUNT(*) AS forks_count
            FROM iceberg.silver.fact_engagements
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
              AND engagement_type = 'fork'
            GROUP BY repo_id
        ),
        daily_prs AS (
            SELECT
                repo_id,
                COUNT(CASE WHEN action = 'opened' THEN 1 END) AS opened_prs_count,
                COUNT(CASE WHEN is_merged = true THEN 1 END) AS merged_prs_count
            FROM iceberg.silver.fact_pull_requests
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
            GROUP BY repo_id
        ),
        all_human_activities AS (
            -- Union all events on this date and join dim_users to filter out bots
            SELECT repo_id, actor_id
            FROM iceberg.silver.fact_commits
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
            UNION
            SELECT repo_id, actor_id
            FROM iceberg.silver.fact_pull_requests
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
            UNION
            SELECT repo_id, actor_id
            FROM iceberg.silver.fact_engagements
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
        ),
        human_contributors AS (
            SELECT a.repo_id, COUNT(DISTINCT a.actor_id) AS distinct_human_contributors
            FROM all_human_activities a
            JOIN iceberg.silver.dim_users u ON a.actor_id = u.user_id
            WHERE u.is_bot = FALSE
            GROUP BY a.repo_id
        ),
        active_repos AS (
            SELECT repo_id FROM daily_stars
            UNION
            SELECT repo_id FROM daily_forks
            UNION
            SELECT repo_id FROM daily_prs
            UNION
            SELECT repo_id FROM human_contributors
        )
        SELECT
            DATE '{target_date}' AS metric_date,
            r.repo_id,
            COALESCE(dr.repo_name, 'unknown') AS repo_name,
            COALESCE(rm.primary_language, 'unknown') AS primary_language,
            CAST(COALESCE(s.new_stars_count, 0) AS BIGINT) AS new_stars_count,
            CAST(COALESCE(f.forks_count, 0) AS BIGINT) AS forks_count,
            CAST(COALESCE(p.opened_prs_count, 0) AS BIGINT) AS opened_prs_count,
            CAST(COALESCE(p.merged_prs_count, 0) AS BIGINT) AS merged_prs_count,
            CAST(COALESCE(h.distinct_human_contributors, 0) AS BIGINT) AS distinct_human_contributors,
            TIMESTAMP '{now_iso}' AS updated_at
        FROM active_repos r
        LEFT JOIN iceberg.silver.dim_repositories dr ON r.repo_id = dr.repo_id
        LEFT JOIN iceberg.silver.repo_metadata rm ON r.repo_id = rm.repo_id
        LEFT JOIN daily_stars s ON r.repo_id = s.repo_id
        LEFT JOIN daily_forks f ON r.repo_id = f.repo_id
        LEFT JOIN daily_prs p ON r.repo_id = p.repo_id
        LEFT JOIN human_contributors h ON r.repo_id = h.repo_id
        """

        staged_df = spark.sql(aggregation_sql)
        staged_df.createOrReplaceTempView("staged_trending_mart")

        staged_count = staged_df.count()
        print(f"Calculated {staged_count} trending repo records for {target_date}")

        # 2. Idempotent Merge into Gold Mart
        spark.sql(
            """
            MERGE INTO iceberg.gold.mart_trending_repos_daily AS target
            USING staged_trending_mart AS source
            ON target.metric_date = source.metric_date AND target.repo_id = source.repo_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.repo_name = source.repo_name,
                    target.primary_language = source.primary_language,
                    target.new_stars_count = source.new_stars_count,
                    target.forks_count = source.forks_count,
                    target.opened_prs_count = source.opened_prs_count,
                    target.merged_prs_count = source.merged_prs_count,
                    target.distinct_human_contributors = source.distinct_human_contributors,
                    target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (
                    metric_date, repo_id, repo_name, primary_language,
                    new_stars_count, forks_count, opened_prs_count, merged_prs_count,
                    distinct_human_contributors, updated_at
                )
                VALUES (
                    source.metric_date, source.repo_id, source.repo_name, source.primary_language,
                    source.new_stars_count, source.forks_count, source.opened_prs_count, source.merged_prs_count,
                    source.distinct_human_contributors, source.updated_at
                )
            """
        )

        if source_hour:
            advance_run_status(spark, source_hour, active_run_id, "GOLD_COMPLETE")

        print(f"=== Gold Aggregation Completed Successfully for {target_date} ===")
        return staged_count

    except Exception as exc:
        err_msg = f"Gold aggregation error: {exc}"
        print(err_msg, file=sys.stderr)
        if source_hour:
            fail_run(spark, source_hour, active_run_id, err_msg)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold trending repos aggregation job.")
    parser.add_argument(
        "--date", required=True, help="Target date in YYYY-MM-DD format"
    )
    parser.add_argument(
        "--source-hour",
        default=None,
        help="Optional source hour to link control status",
    )
    parser.add_argument(
        "--run-id", default=None, help="Optional run ID for traceability"
    )
    args = parser.parse_args()

    spark = get_spark_session(f"GoldAggregation-{args.date}")
    try:
        run_gold_aggregation(
            spark, args.date, source_hour=args.source_hour, run_id=args.run_id
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
