from __future__ import annotations

import os
from datetime import UTC, datetime

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import Param, dag, task
except ImportError:
    from airflow.decorators import dag, task
    from airflow.models.param import Param
    from airflow.operators.bash import BashOperator

# Ensure spark project root is in python path
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
    dag_id="github_events_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-end Medallion pipeline: Download -> Bronze -> Silver -> Enrich -> Gold -> Quality Gates",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,  # Strictly prevents overlapping runs across source hours
    params={
        "source_hour": Param(
            default="",
            type="string",
            description="Explicit source hour in YYYY-MM-DD-H format (empty for auto current hour)",
        ),
        "force": Param(
            default=False,
            type="boolean",
            description="Force reprocessing/replay even if already completed",
        ),
    },
    tags=["lakehouse", "phase-3", "iceberg", "airflow-3"],
)
def github_events_lakehouse_pipeline():

    @task
    def resolve_pipeline_params(**context) -> dict[str, str]:
        """Resolves source_hour, date, run_id and force flag from DAG context."""
        from datetime import timedelta

        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run else {}
        dag_params = context.get("params") or {}

        explicit_hour = conf.get("source_hour")
        if not explicit_hour:
            param_hour = dag_params.get("source_hour")
            if param_hour and param_hour.strip():
                explicit_hour = param_hour.strip()

        if not explicit_hour:
            logical_date = context.get("logical_date") or context.get(
                "data_interval_start"
            )
            if logical_date:
                explicit_hour = (
                    f"{logical_date.strftime('%Y-%m-%d')}-{logical_date.hour}"
                )
            else:
                now_utc = datetime.now(UTC)
                prev_hour = now_utc - timedelta(hours=1)
                explicit_hour = f"{prev_hour.strftime('%Y-%m-%d')}-{prev_hour.hour}"

        target_date = explicit_hour.rsplit("-", 1)[0]
        run_id = context.get("run_id", "manual_run")
        force_flag = "--force" if (conf.get("force") or dag_params.get("force")) else ""

        return {
            "source_hour": explicit_hour,
            "target_date": target_date,
            "run_id": run_id,
            "force_flag": force_flag,
        }

    params = resolve_pipeline_params()

    # Task 1: Download & Land GH Archive into MinIO
    download_task = BashOperator(
        task_id="download_and_land",
        bash_command=(
            f"docker exec -e PYTHONPATH=/opt/spark/work-dir {SPARK_CONTAINER} python3 /opt/spark/work-dir/spark/utils/downloader.py "
            "--source-hour {{ ti.xcom_pull(task_ids='resolve_pipeline_params')['source_hour'] }} "
            "{{ ti.xcom_pull(task_ids='resolve_pipeline_params')['force_flag'] }}"
        ),
    )

    # Task 2: Bronze Ingestion & Deduplication (Iceberg Merge)
    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command=(
            f"docker exec -e PYTHONPATH=/opt/spark/work-dir {SPARK_CONTAINER} /opt/spark/bin/spark-submit --jars {SPARK_JARS} "
            "/opt/spark/work-dir/spark/jobs/bronze_ingestion.py "
            "--source-hour {{ ti.xcom_pull(task_ids='resolve_pipeline_params')['source_hour'] }} "
            "--run-id {{ ti.xcom_pull(task_ids='resolve_pipeline_params')['run_id'] }} "
            "{{ ti.xcom_pull(task_ids='resolve_pipeline_params')['force_flag'] }}"
        ),
    )

    # Task 3: Silver Transformation (SCD-1 Dims, Facts, State Machine)
    silver_task = BashOperator(
        task_id="silver_transformation",
        bash_command=(
            f"docker exec -e PYTHONPATH=/opt/spark/work-dir {SPARK_CONTAINER} /opt/spark/bin/spark-submit --jars {SPARK_JARS} "
            "/opt/spark/work-dir/spark/jobs/silver_transform.py "
            "--source-hour {{ ti.xcom_pull(task_ids='resolve_pipeline_params')['source_hour'] }} "
            "--run-id {{ ti.xcom_pull(task_ids='resolve_pipeline_params')['run_id'] }} "
            "{{ ti.xcom_pull(task_ids='resolve_pipeline_params')['force_flag'] }}"
        ),
    )

    # Task 4: Gold Marts Aggregation (All 3 Marts)
    gold_task = BashOperator(
        task_id="gold_aggregation",
        bash_command=(
            f"docker exec -e PYTHONPATH=/opt/spark/work-dir {SPARK_CONTAINER} /opt/spark/bin/spark-submit --jars {SPARK_JARS} "
            "/opt/spark/work-dir/spark/jobs/gold_aggregation.py "
            "--date {{ ti.xcom_pull(task_ids='resolve_pipeline_params')['target_date'] }} "
            "--source-hour {{ ti.xcom_pull(task_ids='resolve_pipeline_params')['source_hour'] }} "
            "--run-id {{ ti.xcom_pull(task_ids='resolve_pipeline_params')['run_id'] }}"
        ),
    )

    # Task 5: Comprehensive Quality Gates & Reconciliation
    quality_task = BashOperator(
        task_id="quality_gates_validation",
        bash_command=(
            f"docker exec -e PYTHONPATH=/opt/spark/work-dir {SPARK_CONTAINER} /opt/spark/bin/spark-submit --jars {SPARK_JARS} "
            "/opt/spark/work-dir/spark/jobs/quality_check.py "
            "--source-hour {{ ti.xcom_pull(task_ids='resolve_pipeline_params')['source_hour'] }} "
            "--date {{ ti.xcom_pull(task_ids='resolve_pipeline_params')['target_date'] }}"
        ),
    )

    # Pipeline execution flow: download -> bronze -> silver -> enrich -> gold -> validate
    params >> download_task >> bronze_task >> silver_task >> gold_task >> quality_task


pipeline_dag = github_events_lakehouse_pipeline()
