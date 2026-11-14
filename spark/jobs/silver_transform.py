from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from spark.utils.bot_detector import is_bot_col
from spark.utils.control_ledger import (
    advance_run_status,
    ensure_control_table,
    fail_run,
    get_ingestion_run,
)
from spark.utils.enrichment import enrich_repositories, ensure_repo_metadata_table
from spark.utils.spark_session import get_spark_session

# Schemas for parsing raw_payload JSON in Spark
PUSH_PAYLOAD_SCHEMA = StructType(
    [
        StructField("size", IntegerType(), True),
        StructField("distinct_size", IntegerType(), True),
        StructField("ref", StringType(), True),
        StructField("head", StringType(), True),
    ]
)

PR_PAYLOAD_SCHEMA = StructType(
    [
        StructField("action", StringType(), True),
        StructField(
            "pull_request",
            StructType(
                [
                    StructField("id", LongType(), True),
                    StructField("number", IntegerType(), True),
                    StructField("state", StringType(), True),
                    StructField("merged", BooleanType(), True),
                    StructField("additions", IntegerType(), True),
                    StructField("deletions", IntegerType(), True),
                    StructField(
                        "base",
                        StructType(
                            [
                                StructField(
                                    "repo",
                                    StructType(
                                        [
                                            StructField("language", StringType(), True),
                                        ]
                                    ),
                                    True,
                                )
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

GENERIC_PAYLOAD_SCHEMA = StructType(
    [
        StructField("action", StringType(), True),
    ]
)


def ensure_silver_tables(spark: SparkSession) -> None:
    """Creates the silver namespace and tables in Iceberg if not exist."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    # 1. dim_users (SCD Type 1)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.dim_users (
            user_id BIGINT NOT NULL,
            user_login STRING NOT NULL,
            avatar_url STRING,
            is_bot BOOLEAN NOT NULL,
            first_seen_at TIMESTAMP NOT NULL,
            last_active_at TIMESTAMP NOT NULL,
            ingestion_run_id STRING NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        USING iceberg
        """
    )

    # 2. dim_repositories (SCD Type 1)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.dim_repositories (
            repo_id BIGINT NOT NULL,
            repo_name STRING NOT NULL,
            owner_login STRING,
            first_seen_at TIMESTAMP NOT NULL,
            last_active_at TIMESTAMP NOT NULL,
            ingestion_run_id STRING NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        USING iceberg
        """
    )

    # 3. fact_commits (PushEvent summaries)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.fact_commits (
            event_id STRING NOT NULL,
            repo_id BIGINT NOT NULL,
            actor_id BIGINT NOT NULL,
            branch_name STRING,
            commit_count INT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            source_hour STRING NOT NULL,
            ingestion_run_id STRING NOT NULL,
            ingested_at TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (hours(created_at))
        """
    )

    # 4. fact_pull_requests (PullRequestEvent)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.fact_pull_requests (
            event_id STRING NOT NULL,
            pr_id BIGINT,
            repo_id BIGINT NOT NULL,
            actor_id BIGINT NOT NULL,
            action STRING NOT NULL,
            is_merged BOOLEAN NOT NULL,
            language STRING,
            additions INT,
            deletions INT,
            created_at TIMESTAMP NOT NULL,
            source_hour STRING NOT NULL,
            ingestion_run_id STRING NOT NULL,
            ingested_at TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (hours(created_at))
        """
    )

    # 5. fact_engagements (WatchEvent, ForkEvent, IssuesEvent)
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.fact_engagements (
            event_id STRING NOT NULL,
            repo_id BIGINT NOT NULL,
            actor_id BIGINT NOT NULL,
            engagement_type STRING NOT NULL,
            action STRING NOT NULL,
            created_at TIMESTAMP NOT NULL,
            source_hour STRING NOT NULL,
            ingestion_run_id STRING NOT NULL,
            ingested_at TIMESTAMP NOT NULL
        )
        USING iceberg
        PARTITIONED BY (hours(created_at))
        """
    )

    # 6. repo_metadata
    ensure_repo_metadata_table(spark)


def run_silver_transformation(
    spark: SparkSession,
    source_hour: str,
    run_id: str | None = None,
    force: bool = False,
) -> dict[str, int]:
    """
    Executes Silver transformation for the given source_hour:
    1. Reads deduplicated Bronze records for source_hour.
    2. Builds/updates dim_users with SCD-1 merge (preserving first_seen_at).
    3. Builds/updates dim_repositories with SCD-1 merge.
    4. Parses raw_payload into fact_commits, fact_pull_requests, fact_engagements.
    5. Performs repository enrichment in silver.repo_metadata.
    6. Advances control ledger to SILVER_COMPLETE -> ENRICHED.
    """
    ensure_control_table(spark)
    ensure_silver_tables(spark)

    run_record = get_ingestion_run(spark, source_hour)
    active_run_id = run_id or (
        run_record["run_id"] if run_record else str(uuid.uuid4())
    )

    print(
        f"=== Starting Silver Transformation for source_hour: {source_hour} (Run ID: {active_run_id}) ==="
    )

    # Advance status to SILVER_PROCESSING
    advance_run_status(spark, source_hour, active_run_id, "SILVER_PROCESSING")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # noqa: UP017

    try:
        bronze_df = spark.sql(
            f"""
            SELECT event_id, record_hash, event_type, actor_id, actor_login,
                   repo_id, repo_name, created_at, raw_payload,
                   source_hour, source_path, source_checksum, ingestion_run_id, ingested_at
            FROM iceberg.bronze.github_events
            WHERE source_hour = '{source_hour}'
            """
        )

        total_bronze = bronze_df.count()
        print(f"Found {total_bronze} Bronze events for source_hour {source_hour}")
        if total_bronze == 0:
            advance_run_status(spark, source_hour, active_run_id, "SILVER_COMPLETE")
            return {"users": 0, "repos": 0, "commits": 0, "prs": 0, "engagements": 0}

        # -------------------------------------------------------------
        # 1. Transform & Upsert silver.dim_users
        # -------------------------------------------------------------
        users_raw = bronze_df.filter(F.col("actor_id").isNotNull()).select(
            F.col("actor_id").alias("user_id"),
            F.coalesce(F.col("actor_login"), F.lit("unknown")).alias("user_login"),
            is_bot_col(F.col("actor_login")).alias("is_bot"),
            F.col("created_at"),
        )

        user_window = Window.partitionBy("user_id").orderBy(F.col("created_at").desc())
        users_dedup = (
            users_raw.groupBy("user_id")
            .agg(
                F.min("created_at").alias("first_seen_at"),
                F.max("created_at").alias("last_active_at"),
            )
            .join(
                users_raw.withColumn("rn", F.row_number().over(user_window))
                .filter(F.col("rn") == 1)
                .select("user_id", "user_login", "is_bot"),
                on="user_id",
            )
            .select(
                "user_id",
                "user_login",
                F.lit(None).cast(StringType()).alias("avatar_url"),
                "is_bot",
                "first_seen_at",
                "last_active_at",
                F.lit(active_run_id).alias("ingestion_run_id"),
                F.to_timestamp(F.lit(now_iso)).alias("updated_at"),
            )
        )

        users_dedup.createOrReplaceTempView("staged_users")
        spark.sql(
            """
            MERGE INTO iceberg.silver.dim_users AS target
            USING staged_users AS source
            ON target.user_id = source.user_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.user_login = source.user_login,
                    target.avatar_url = COALESCE(source.avatar_url, target.avatar_url),
                    target.is_bot = source.is_bot,
                    target.first_seen_at = LEAST(target.first_seen_at, source.first_seen_at),
                    target.last_active_at = GREATEST(target.last_active_at, source.last_active_at),
                    target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (user_id, user_login, avatar_url, is_bot, first_seen_at, last_active_at, ingestion_run_id, updated_at)
                VALUES (source.user_id, source.user_login, source.avatar_url, source.is_bot, source.first_seen_at, source.last_active_at, source.ingestion_run_id, source.updated_at)
            """
        )
        user_count = users_dedup.count()

        # -------------------------------------------------------------
        # 2. Transform & Upsert silver.dim_repositories
        # -------------------------------------------------------------
        repos_raw = bronze_df.filter(F.col("repo_id").isNotNull()).select(
            F.col("repo_id"),
            F.coalesce(F.col("repo_name"), F.lit("unknown")).alias("repo_name"),
            F.split(F.col("repo_name"), "/").getItem(0).alias("owner_login"),
            F.col("created_at"),
        )

        repo_window = Window.partitionBy("repo_id").orderBy(F.col("created_at").desc())
        repos_dedup = (
            repos_raw.groupBy("repo_id")
            .agg(
                F.min("created_at").alias("first_seen_at"),
                F.max("created_at").alias("last_active_at"),
            )
            .join(
                repos_raw.withColumn("rn", F.row_number().over(repo_window))
                .filter(F.col("rn") == 1)
                .select("repo_id", "repo_name", "owner_login"),
                on="repo_id",
            )
            .select(
                "repo_id",
                "repo_name",
                "owner_login",
                "first_seen_at",
                "last_active_at",
                F.lit(active_run_id).alias("ingestion_run_id"),
                F.to_timestamp(F.lit(now_iso)).alias("updated_at"),
            )
        )

        repos_dedup.createOrReplaceTempView("staged_repos")
        spark.sql(
            """
            MERGE INTO iceberg.silver.dim_repositories AS target
            USING staged_repos AS source
            ON target.repo_id = source.repo_id
            WHEN MATCHED THEN
                UPDATE SET
                    target.repo_name = source.repo_name,
                    target.owner_login = COALESCE(source.owner_login, target.owner_login),
                    target.first_seen_at = LEAST(target.first_seen_at, source.first_seen_at),
                    target.last_active_at = GREATEST(target.last_active_at, source.last_active_at),
                    target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (repo_id, repo_name, owner_login, first_seen_at, last_active_at, ingestion_run_id, updated_at)
                VALUES (source.repo_id, source.repo_name, source.owner_login, source.first_seen_at, source.last_active_at, source.ingestion_run_id, source.updated_at)
            """
        )
        repo_count = repos_dedup.count()

        # -------------------------------------------------------------
        # 3. Transform & Upsert silver.fact_commits (PushEvent)
        # -------------------------------------------------------------
        push_df = (
            bronze_df.filter(
                (F.col("event_type") == "PushEvent")
                & F.col("repo_id").isNotNull()
                & F.col("actor_id").isNotNull()
            )
            .withColumn(
                "parsed_payload", F.from_json("raw_payload", PUSH_PAYLOAD_SCHEMA)
            )
            .select(
                F.col("event_id"),
                F.col("repo_id"),
                F.col("actor_id"),
                F.coalesce(F.col("parsed_payload.ref"), F.lit("unknown")).alias(
                    "branch_name"
                ),
                F.coalesce(
                    F.col("parsed_payload.size"),
                    F.col("parsed_payload.distinct_size"),
                    F.lit(1),
                )
                .cast(IntegerType())
                .alias("commit_count"),
                F.col("created_at"),
                F.col("source_hour"),
                F.lit(active_run_id).alias("ingestion_run_id"),
                F.col("ingested_at"),
            )
            .dropDuplicates(["event_id"])
        )

        commit_count = push_df.count()
        if commit_count > 0:
            push_df.createOrReplaceTempView("staged_commits")
            spark.sql(
                """
                MERGE INTO iceberg.silver.fact_commits AS target
                USING staged_commits AS source
                ON target.event_id = source.event_id
                WHEN MATCHED THEN
                    UPDATE SET
                        target.repo_id = source.repo_id,
                        target.actor_id = source.actor_id,
                        target.branch_name = source.branch_name,
                        target.commit_count = source.commit_count,
                        target.created_at = source.created_at,
                        target.source_hour = source.source_hour,
                        target.ingestion_run_id = source.ingestion_run_id,
                        target.ingested_at = source.ingested_at
                WHEN NOT MATCHED THEN
                    INSERT (event_id, repo_id, actor_id, branch_name, commit_count, created_at, source_hour, ingestion_run_id, ingested_at)
                    VALUES (source.event_id, source.repo_id, source.actor_id, source.branch_name, source.commit_count, source.created_at, source.source_hour, source.ingestion_run_id, source.ingested_at)
                """
            )

        # -------------------------------------------------------------
        # 4. Transform & Upsert silver.fact_pull_requests (PullRequestEvent)
        # -------------------------------------------------------------
        pr_df = (
            bronze_df.filter(
                (F.col("event_type") == "PullRequestEvent")
                & F.col("repo_id").isNotNull()
                & F.col("actor_id").isNotNull()
            )
            .withColumn("parsed_payload", F.from_json("raw_payload", PR_PAYLOAD_SCHEMA))
            .select(
                F.col("event_id"),
                F.col("parsed_payload.pull_request.id").alias("pr_id"),
                F.col("repo_id"),
                F.col("actor_id"),
                F.coalesce(F.col("parsed_payload.action"), F.lit("unknown")).alias(
                    "action"
                ),
                F.coalesce(
                    F.col("parsed_payload.pull_request.merged"), F.lit(False)
                ).alias("is_merged"),
                F.coalesce(
                    F.col("parsed_payload.pull_request.base.repo.language"),
                    F.lit("unknown"),
                ).alias("language"),
                F.coalesce(
                    F.col("parsed_payload.pull_request.additions"), F.lit(0)
                ).alias("additions"),
                F.coalesce(
                    F.col("parsed_payload.pull_request.deletions"), F.lit(0)
                ).alias("deletions"),
                F.col("created_at"),
                F.col("source_hour"),
                F.lit(active_run_id).alias("ingestion_run_id"),
                F.col("ingested_at"),
            )
            .dropDuplicates(["event_id"])
        )

        pr_count = pr_df.count()
        if pr_count > 0:
            pr_df.createOrReplaceTempView("staged_prs")
            spark.sql(
                """
                MERGE INTO iceberg.silver.fact_pull_requests AS target
                USING staged_prs AS source
                ON target.event_id = source.event_id
                WHEN MATCHED THEN
                    UPDATE SET
                        target.pr_id = source.pr_id,
                        target.repo_id = source.repo_id,
                        target.actor_id = source.actor_id,
                        target.action = source.action,
                        target.is_merged = source.is_merged,
                        target.language = source.language,
                        target.additions = source.additions,
                        target.deletions = source.deletions,
                        target.created_at = source.created_at,
                        target.source_hour = source.source_hour,
                        target.ingestion_run_id = source.ingestion_run_id,
                        target.ingested_at = source.ingested_at
                WHEN NOT MATCHED THEN
                    INSERT (event_id, pr_id, repo_id, actor_id, action, is_merged, language, additions, deletions, created_at, source_hour, ingestion_run_id, ingested_at)
                    VALUES (source.event_id, source.pr_id, source.repo_id, source.actor_id, source.action, source.is_merged, source.language, source.additions, source.deletions, source.created_at, source.source_hour, source.ingestion_run_id, source.ingested_at)
                """
            )

        # -------------------------------------------------------------
        # 5. Transform & Upsert silver.fact_engagements (Watch, Fork, Issues)
        # -------------------------------------------------------------
        # Note: WatchEvent action 'started' is star. ForkEvent is fork. IssuesEvent is issue.
        eng_raw = bronze_df.filter(
            F.col("event_type").isin("WatchEvent", "ForkEvent", "IssuesEvent")
            & F.col("repo_id").isNotNull()
            & F.col("actor_id").isNotNull()
        )
        eng_df = (
            eng_raw.withColumn(
                "parsed_payload", F.from_json("raw_payload", GENERIC_PAYLOAD_SCHEMA)
            )
            .select(
                F.col("event_id"),
                F.col("repo_id"),
                F.col("actor_id"),
                F.when(F.col("event_type") == "WatchEvent", F.lit("star"))
                .when(F.col("event_type") == "ForkEvent", F.lit("fork"))
                .when(F.col("event_type") == "IssuesEvent", F.lit("issue"))
                .otherwise(F.lit("other"))
                .alias("engagement_type"),
                F.coalesce(
                    F.col("parsed_payload.action"),
                    F.when(
                        F.col("event_type") == "ForkEvent", F.lit("forked")
                    ).otherwise(F.lit("action")),
                ).alias("action"),
                F.col("created_at"),
                F.col("source_hour"),
                F.lit(active_run_id).alias("ingestion_run_id"),
                F.col("ingested_at"),
            )
            .dropDuplicates(["event_id"])
        )

        eng_count = eng_df.count()
        if eng_count > 0:
            eng_df.createOrReplaceTempView("staged_engagements")
            spark.sql(
                """
                MERGE INTO iceberg.silver.fact_engagements AS target
                USING staged_engagements AS source
                ON target.event_id = source.event_id
                WHEN MATCHED THEN
                    UPDATE SET
                        target.repo_id = source.repo_id,
                        target.actor_id = source.actor_id,
                        target.engagement_type = source.engagement_type,
                        target.action = source.action,
                        target.created_at = source.created_at,
                        target.source_hour = source.source_hour,
                        target.ingestion_run_id = source.ingestion_run_id,
                        target.ingested_at = source.ingested_at
                WHEN NOT MATCHED THEN
                    INSERT (event_id, repo_id, actor_id, engagement_type, action, created_at, source_hour, ingestion_run_id, ingested_at)
                    VALUES (source.event_id, source.repo_id, source.actor_id, source.engagement_type, source.action, source.created_at, source.source_hour, source.ingestion_run_id, source.ingested_at)
                """
            )

        # -------------------------------------------------------------
        # 6. Repository Enrichment (silver.repo_metadata)
        # -------------------------------------------------------------
        repos_to_enrich = [
            {"repo_id": row["repo_id"], "repo_name": row["repo_name"]}
            for row in repos_dedup.select("repo_id", "repo_name").collect()
        ]
        enriched_count = enrich_repositories(
            spark, repos_to_enrich, force_refresh=force
        )
        print(f"Enriched {enriched_count} repositories in silver.repo_metadata")

        advance_run_status(spark, source_hour, active_run_id, "ENRICHED")
        print(f"=== Silver Transformation Completed for {source_hour} ===")
        print(
            f"Users: {user_count} | Repos: {repo_count} | Commits: {commit_count} | PRs: {pr_count} | Engagements: {eng_count}"
        )

        return {
            "users": user_count,
            "repos": repo_count,
            "commits": commit_count,
            "prs": pr_count,
            "engagements": eng_count,
        }

    except Exception as exc:
        err_msg = f"Silver transformation error: {exc}"
        print(err_msg, file=sys.stderr)
        fail_run(spark, source_hour, active_run_id, err_msg)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Silver transformation job for GitHub events."
    )
    parser.add_argument(
        "--source-hour", required=True, help="Target hour in YYYY-MM-DD-H format"
    )
    parser.add_argument(
        "--run-id", default=None, help="Optional run ID for traceability"
    )
    parser.add_argument(
        "--force", action="store_true", help="Force refresh metadata cache"
    )
    args = parser.parse_args()

    spark = get_spark_session(f"SilverTransform-{args.source_hour}")
    try:
        run_silver_transformation(
            spark, args.source_hour, run_id=args.run_id, force=args.force
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
