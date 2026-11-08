from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None  # type: ignore

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
    """Creates the gold namespace and all three analytical marts in Iceberg if not exist."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    # 1. Trending Repositories Daily Mart: Grain (metric_date, repo_id)
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

    # 2. Language Trends Daily Mart: Grain (metric_date, primary_language)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.mart_language_trends_daily (
            metric_date DATE NOT NULL,
            primary_language STRING NOT NULL,
            active_repos_count BIGINT NOT NULL,
            distinct_human_contributors BIGINT NOT NULL,
            new_stars_count BIGINT NOT NULL,
            forks_count BIGINT NOT NULL,
            opened_prs_count BIGINT NOT NULL,
            merged_prs_count BIGINT NOT NULL,
            commits_count BIGINT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (metric_date)
        """
    )

    # 3. Developer Work Patterns Daily Mart: Grain (metric_date, day_of_week, hour_of_day)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.mart_developer_work_patterns_daily (
            metric_date DATE NOT NULL,
            day_of_week INT NOT NULL,
            hour_of_day INT NOT NULL,
            commit_events_count BIGINT NOT NULL,
            pr_events_count BIGINT NOT NULL,
            engagement_events_count BIGINT NOT NULL,
            distinct_human_developers BIGINT NOT NULL,
            total_events_count BIGINT NOT NULL,
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
) -> dict[str, int]:
    """
    Aggregates Silver data into all 3 Iceberg Gold marts for a specific date:
    1. iceberg.gold.mart_trending_repos_daily:
       - Grain: (metric_date, repo_id)
    2. iceberg.gold.mart_language_trends_daily:
       - Grain: (metric_date, primary_language)
    3. iceberg.gold.mart_developer_work_patterns_daily:
       - Grain: (metric_date, day_of_week, hour_of_day)

    Guarantees:
    - Fully reconcilable to Silver facts and dimensions.
    - Excludes bots from distinct_human_contributors and distinct_human_developers.
    - Uses MERGE INTO on each mart's declared grain to guarantee zero duplicate keys upon replay.
    - Advances control ledger to GOLD_COMPLETE when source_hour is given.
    """
    ensure_control_table(spark)
    ensure_gold_tables(spark)

    active_hour = source_hour or f"{target_date}-0"
    run_record = get_ingestion_run(spark, active_hour)
    active_run_id = run_id or (
        run_record["run_id"] if run_record else str(uuid.uuid4())
    )

    print(
        f"=== Starting Gold Aggregations for metric_date: {target_date} (Run ID: {active_run_id}) ==="
    )

    if source_hour:
        advance_run_status(spark, source_hour, active_run_id, "GOLD_PROCESSING")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # noqa: UP017

    try:
        # =========================================================================
        # 1. Base Common CTEs & mart_trending_repos_daily
        # =========================================================================
        trending_sql = f"""
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

        trending_df = spark.sql(trending_sql)
        trending_df.createOrReplaceTempView("staged_trending_mart")
        count_trending = trending_df.count()

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
        print(
            f"Calculated and merged {count_trending} trending repos records for {target_date}"
        )

        # =========================================================================
        # 2. mart_language_trends_daily
        # Grain: (metric_date, primary_language)
        # =========================================================================
        language_sql = f"""
        WITH repo_languages AS (
            SELECT
                r.repo_id,
                COALESCE(rm.primary_language, 'unknown') AS primary_language
            FROM (
                SELECT DISTINCT repo_id FROM iceberg.silver.dim_repositories
                UNION
                SELECT DISTINCT repo_id FROM iceberg.silver.fact_commits WHERE CAST(created_at AS DATE) = DATE '{target_date}'
                UNION
                SELECT DISTINCT repo_id FROM iceberg.silver.fact_pull_requests WHERE CAST(created_at AS DATE) = DATE '{target_date}'
                UNION
                SELECT DISTINCT repo_id FROM iceberg.silver.fact_engagements WHERE CAST(created_at AS DATE) = DATE '{target_date}'
            ) r
            LEFT JOIN iceberg.silver.repo_metadata rm ON r.repo_id = rm.repo_id
        ),
        daily_stars_by_repo AS (
            SELECT repo_id, COUNT(*) AS stars_count
            FROM iceberg.silver.fact_engagements
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
              AND engagement_type = 'star'
              AND action = 'started'
            GROUP BY repo_id
        ),
        daily_forks_by_repo AS (
            SELECT repo_id, COUNT(*) AS forks_count
            FROM iceberg.silver.fact_engagements
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
              AND engagement_type = 'fork'
            GROUP BY repo_id
        ),
        daily_prs_by_repo AS (
            SELECT
                repo_id,
                COUNT(CASE WHEN action = 'opened' THEN 1 END) AS opened_prs_count,
                COUNT(CASE WHEN is_merged = true THEN 1 END) AS merged_prs_count
            FROM iceberg.silver.fact_pull_requests
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
            GROUP BY repo_id
        ),
        daily_commits_by_repo AS (
            SELECT repo_id, CAST(COALESCE(SUM(commit_count), 0) AS BIGINT) AS commits_count
            FROM iceberg.silver.fact_commits
            WHERE CAST(created_at AS DATE) = DATE '{target_date}'
            GROUP BY repo_id
        ),
        all_human_activities AS (
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
        human_activities_with_lang AS (
            SELECT
                a.actor_id,
                COALESCE(rl.primary_language, 'unknown') AS primary_language
            FROM all_human_activities a
            JOIN iceberg.silver.dim_users u ON a.actor_id = u.user_id
            LEFT JOIN repo_languages rl ON a.repo_id = rl.repo_id
            WHERE u.is_bot = FALSE
        ),
        distinct_human_by_lang AS (
            SELECT
                primary_language,
                COUNT(DISTINCT actor_id) AS distinct_human_contributors
            FROM human_activities_with_lang
            GROUP BY primary_language
        ),
        active_repos_with_activity AS (
            SELECT repo_id FROM daily_stars_by_repo
            UNION
            SELECT repo_id FROM daily_forks_by_repo
            UNION
            SELECT repo_id FROM daily_prs_by_repo
            UNION
            SELECT repo_id FROM daily_commits_by_repo
        ),
        active_repos_by_lang AS (
            SELECT
                rl.primary_language,
                COUNT(DISTINCT a.repo_id) AS active_repos_count,
                CAST(COALESCE(SUM(s.stars_count), 0) AS BIGINT) AS new_stars_count,
                CAST(COALESCE(SUM(f.forks_count), 0) AS BIGINT) AS forks_count,
                CAST(COALESCE(SUM(p.opened_prs_count), 0) AS BIGINT) AS opened_prs_count,
                CAST(COALESCE(SUM(p.merged_prs_count), 0) AS BIGINT) AS merged_prs_count,
                CAST(COALESCE(SUM(c.commits_count), 0) AS BIGINT) AS commits_count
            FROM active_repos_with_activity a
            LEFT JOIN repo_languages rl ON a.repo_id = rl.repo_id
            LEFT JOIN daily_stars_by_repo s ON a.repo_id = s.repo_id
            LEFT JOIN daily_forks_by_repo f ON a.repo_id = f.repo_id
            LEFT JOIN daily_prs_by_repo p ON a.repo_id = p.repo_id
            LEFT JOIN daily_commits_by_repo c ON a.repo_id = c.repo_id
            GROUP BY rl.primary_language
        ),
        all_languages AS (
            SELECT primary_language FROM active_repos_by_lang
            UNION
            SELECT primary_language FROM distinct_human_by_lang
        )
        SELECT
            DATE '{target_date}' AS metric_date,
            l.primary_language,
            CAST(COALESCE(a.active_repos_count, 0) AS BIGINT) AS active_repos_count,
            CAST(COALESCE(h.distinct_human_contributors, 0) AS BIGINT) AS distinct_human_contributors,
            CAST(COALESCE(a.new_stars_count, 0) AS BIGINT) AS new_stars_count,
            CAST(COALESCE(a.forks_count, 0) AS BIGINT) AS forks_count,
            CAST(COALESCE(a.opened_prs_count, 0) AS BIGINT) AS opened_prs_count,
            CAST(COALESCE(a.merged_prs_count, 0) AS BIGINT) AS merged_prs_count,
            CAST(COALESCE(a.commits_count, 0) AS BIGINT) AS commits_count,
            TIMESTAMP '{now_iso}' AS updated_at
        FROM all_languages l
        LEFT JOIN active_repos_by_lang a ON l.primary_language = a.primary_language
        LEFT JOIN distinct_human_by_lang h ON l.primary_language = h.primary_language
        """

        language_df = spark.sql(language_sql)
        language_df.createOrReplaceTempView("staged_language_mart")
        count_lang = language_df.count()

        spark.sql(
            """
            MERGE INTO iceberg.gold.mart_language_trends_daily AS target
            USING staged_language_mart AS source
            ON target.metric_date = source.metric_date AND target.primary_language = source.primary_language
            WHEN MATCHED THEN
                UPDATE SET
                    target.active_repos_count = source.active_repos_count,
                    target.distinct_human_contributors = source.distinct_human_contributors,
                    target.new_stars_count = source.new_stars_count,
                    target.forks_count = source.forks_count,
                    target.opened_prs_count = source.opened_prs_count,
                    target.merged_prs_count = source.merged_prs_count,
                    target.commits_count = source.commits_count,
                    target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (
                    metric_date, primary_language, active_repos_count,
                    distinct_human_contributors, new_stars_count, forks_count,
                    opened_prs_count, merged_prs_count, commits_count, updated_at
                )
                VALUES (
                    source.metric_date, source.primary_language, source.active_repos_count,
                    source.distinct_human_contributors, source.new_stars_count, source.forks_count,
                    source.opened_prs_count, source.merged_prs_count, source.commits_count, source.updated_at
                )
            """
        )
        print(
            f"Calculated and merged {count_lang} language trend records for {target_date}"
        )

        # =========================================================================
        # 3. mart_developer_work_patterns_daily
        # Grain: (metric_date, day_of_week, hour_of_day)
        # =========================================================================
        work_patterns_sql = f"""
        WITH human_commits AS (
            SELECT
                DATE(c.created_at) AS metric_date,
                DAYOFWEEK(c.created_at) AS day_of_week,
                HOUR(c.created_at) AS hour_of_day,
                c.actor_id,
                1 AS is_commit,
                0 AS is_pr,
                0 AS is_engagement
            FROM iceberg.silver.fact_commits c
            JOIN iceberg.silver.dim_users u ON c.actor_id = u.user_id
            WHERE CAST(c.created_at AS DATE) = DATE '{target_date}'
              AND u.is_bot = FALSE
        ),
        human_prs AS (
            SELECT
                DATE(p.created_at) AS metric_date,
                DAYOFWEEK(p.created_at) AS day_of_week,
                HOUR(p.created_at) AS hour_of_day,
                p.actor_id,
                0 AS is_commit,
                1 AS is_pr,
                0 AS is_engagement
            FROM iceberg.silver.fact_pull_requests p
            JOIN iceberg.silver.dim_users u ON p.actor_id = u.user_id
            WHERE CAST(p.created_at AS DATE) = DATE '{target_date}'
              AND u.is_bot = FALSE
        ),
        human_engagements AS (
            SELECT
                DATE(e.created_at) AS metric_date,
                DAYOFWEEK(e.created_at) AS day_of_week,
                HOUR(e.created_at) AS hour_of_day,
                e.actor_id,
                0 AS is_commit,
                0 AS is_pr,
                1 AS is_engagement
            FROM iceberg.silver.fact_engagements e
            JOIN iceberg.silver.dim_users u ON e.actor_id = u.user_id
            WHERE CAST(e.created_at AS DATE) = DATE '{target_date}'
              AND u.is_bot = FALSE
        ),
        all_human_events AS (
            SELECT * FROM human_commits
            UNION ALL
            SELECT * FROM human_prs
            UNION ALL
            SELECT * FROM human_engagements
        )
        SELECT
            DATE '{target_date}' AS metric_date,
            day_of_week,
            hour_of_day,
            CAST(SUM(is_commit) AS BIGINT) AS commit_events_count,
            CAST(SUM(is_pr) AS BIGINT) AS pr_events_count,
            CAST(SUM(is_engagement) AS BIGINT) AS engagement_events_count,
            CAST(COUNT(DISTINCT actor_id) AS BIGINT) AS distinct_human_developers,
            CAST(COUNT(*) AS BIGINT) AS total_events_count,
            TIMESTAMP '{now_iso}' AS updated_at
        FROM all_human_events
        GROUP BY day_of_week, hour_of_day
        """

        work_patterns_df = spark.sql(work_patterns_sql)
        work_patterns_df.createOrReplaceTempView("staged_work_patterns_mart")
        count_patterns = work_patterns_df.count()

        spark.sql(
            """
            MERGE INTO iceberg.gold.mart_developer_work_patterns_daily AS target
            USING staged_work_patterns_mart AS source
            ON target.metric_date = source.metric_date
               AND target.day_of_week = source.day_of_week
               AND target.hour_of_day = source.hour_of_day
            WHEN MATCHED THEN
                UPDATE SET
                    target.commit_events_count = source.commit_events_count,
                    target.pr_events_count = source.pr_events_count,
                    target.engagement_events_count = source.engagement_events_count,
                    target.distinct_human_developers = source.distinct_human_developers,
                    target.total_events_count = source.total_events_count,
                    target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (
                    metric_date, day_of_week, hour_of_day, commit_events_count,
                    pr_events_count, engagement_events_count, distinct_human_developers,
                    total_events_count, updated_at
                )
                VALUES (
                    source.metric_date, source.day_of_week, source.hour_of_day,
                    source.commit_events_count, source.pr_events_count,
                    source.engagement_events_count, source.distinct_human_developers,
                    source.total_events_count, source.updated_at
                )
            """
        )
        print(
            f"Calculated and merged {count_patterns} work pattern records for {target_date}"
        )

        if source_hour:
            advance_run_status(spark, source_hour, active_run_id, "GOLD_COMPLETE")

        print(f"=== Gold Aggregation Completed Successfully for {target_date} ===")
        return {
            "trending_repos": count_trending,
            "language_trends": count_lang,
            "work_patterns": count_patterns,
        }

    except Exception as exc:
        err_msg = f"Gold aggregation error: {exc}"
        print(err_msg, file=sys.stderr)
        if source_hour:
            fail_run(spark, source_hour, active_run_id, err_msg)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gold aggregation job for all 3 analytical marts."
    )
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
