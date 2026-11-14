"""
Lightweight Background Stream Poller for GitHub Events Lakehouse.

Features:
- Polls GH Archive using lightweight HTTP HEAD requests (0 MB memory overhead).
- Automatically detects when a new hour is published.
- Queries Trino/Control Ledger to check if already completed.
- Runs 'spark-submit' on demand per hour, terminating the JVM immediately upon completion
  to ensure zero memory leak.
- Enforces 7-day FIFO sliding window retention and runs table maintenance.
"""

import datetime
import subprocess
import time
import urllib.request

import trino.dbapi

RETENTION_DAYS = 7
CHECK_INTERVAL_SEC = 300  # Poll every 5 minutes
GH_ARCHIVE_URL_TEMPLATE = "https://data.gharchive.org/{hour_str}.json.gz"

SPARK_SUBMIT_CMD = [
    "docker",
    "exec",
    "-e",
    "PYTHONPATH=/opt/spark/work-dir",
    "lakehouse-spark-master",
    "/opt/spark/bin/spark-submit",
    "--jars",
    (
        "/opt/spark/work-dir/spark/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,"
        "/opt/spark/work-dir/spark/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,"
        "/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-aws-bundle-1.6.1.jar,"
        "/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar,"
        "/opt/spark/work-dir/spark/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar"
    ),
    "/opt/spark/work-dir/spark/jobs/pipeline_stream_runner.py",
    "--retention-days",
    str(RETENTION_DAYS),
]


def log(msg: str):
    print(msg, flush=True)


def is_hour_in_lakehouse(hour_str: str) -> bool:
    try:
        conn = trino.dbapi.connect(
            host="localhost", port=8082, user="admin", catalog="iceberg"
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT status FROM iceberg.control.ingestion_runs WHERE source_hour = ?",
            [hour_str],
        )
        row = cur.fetchone()
        conn.close()
        return bool(
            row and row[0] in ("GOLD_COMPLETE", "SILVER_COMPLETE", "BRONZE_COMPLETE")
        )
    except Exception as e:  # noqa: BLE001
        log(f"[Warning] Failed to query Trino: {e}")
        return False


def is_hour_published_on_gharchive(hour_str: str) -> bool:
    url = GH_ARCHIVE_URL_TEMPLATE.format(hour_str=hour_str)
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0"}, method="HEAD"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status == 200
    except Exception:  # noqa: BLE001
        return False


def run_pipeline_for_hour(date_str: str, hour_int: int):
    hour_str = f"{date_str}-{hour_int}"
    log(f"\n[{datetime.datetime.now()}] >>> Starting ingestion for hour: {hour_str}")
    cmd = SPARK_SUBMIT_CMD + [
        "--start-date",
        date_str,
        "--end-date",
        date_str,
        "--hours",
        str(hour_int),
    ]
    ret = subprocess.run(cmd, check=False)
    if ret.returncode == 0:
        log(f"[{datetime.datetime.now()}] Successfully processed hour: {hour_str}")
    else:
        log(
            f"[{datetime.datetime.now()}] [ERROR] Spark pipeline failed with code {ret.returncode}"
        )


def poll_loop():
    log("=== Starting GitHub Events Lakehouse Realtime Poller ===")
    log(f"Retention policy: {RETENTION_DAYS} days sliding window")
    log(f"Check interval: {CHECK_INTERVAL_SEC} seconds\n")

    while True:
        now_utc = datetime.datetime.now(datetime.UTC)
        # Check previous 2 hours to ensure no missed slots
        for delta_h in [2, 1]:
            target_time = now_utc - datetime.timedelta(hours=delta_h)
            date_str = target_time.strftime("%Y-%m-%d")
            hour_int = target_time.hour
            hour_str = f"{date_str}-{hour_int}"

            if is_hour_in_lakehouse(hour_str):
                continue

            if is_hour_published_on_gharchive(hour_str):
                run_pipeline_for_hour(date_str, hour_int)
            else:
                log(
                    f"[{datetime.datetime.now()}] Hour {hour_str} not yet available on GH Archive."
                )

        time.sleep(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    poll_loop()
