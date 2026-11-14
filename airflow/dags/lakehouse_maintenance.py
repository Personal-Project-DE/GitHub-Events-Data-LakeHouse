from __future__ import annotations

import os
from datetime import UTC, datetime

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import Param, dag
except ImportError:
    from airflow.decorators import dag
    from airflow.models.param import Param
    from airflow.operators.bash import BashOperator

WORKSPACE_ROOT = os.getenv("WORKSPACE_ROOT", "/opt/spark/work-dir")
SPARK_CONTAINER = os.getenv("SPARK_CONTAINER", "lakehouse-spark-master")
SPARK_JARS = (
    "/opt/spark/work-dir/spark/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,"
    "/opt/spark/work-dir/spark/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,"
    "/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-aws-bundle-1.6.1.jar,"
    "/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar,"
    "/opt/spark/work-dir/spark/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
)

default_args = {
    "owner": "lakehouse_admin",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}


@dag(
    dag_id="github_events_lakehouse_maintenance",
    default_args=default_args,
    description="Daily FIFO Retention Pruning and Apache Iceberg Table Optimization",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    params={
        "retention_days": Param(
            default=7,
            type="integer",
            description="Sliding window retention days (prunes oldest days exceeding this count)",
        ),
        "target_file_size_mb": Param(
            default=128,
            type="integer",
            description="Target file size for compaction in MB",
        ),
    },
    tags=["lakehouse", "maintenance", "fifo-retention", "iceberg", "airflow-3"],
)
def github_events_lakehouse_maintenance():
    # Task 1: FIFO Sliding Window Pruning
    fifo_prune_task = BashOperator(
        task_id="fifo_retention_prune",
        bash_command=(
            f"docker exec -e PYTHONPATH=/opt/spark/work-dir {SPARK_CONTAINER} /opt/spark/bin/spark-submit --jars {SPARK_JARS} "
            "/opt/spark/work-dir/spark/jobs/iceberg_maintenance.py "
            "--fifo-retention-days {{ params.retention_days }}"
        ),
    )

    # Task 2: Table Maintenance (Compaction, Expire Snapshots, Orphan Cleanup)
    table_maintenance_task = BashOperator(
        task_id="table_compaction_and_cleanup",
        bash_command=(
            f"docker exec -e PYTHONPATH=/opt/spark/work-dir {SPARK_CONTAINER} /opt/spark/bin/spark-submit --jars {SPARK_JARS} "
            "/opt/spark/work-dir/spark/jobs/iceberg_maintenance.py "
            "--all-tables --target-file-size-mb {{ params.target_file_size_mb }} --older-than-days 1"
        ),
    )

    fifo_prune_task >> table_maintenance_task


github_events_lakehouse_maintenance()
