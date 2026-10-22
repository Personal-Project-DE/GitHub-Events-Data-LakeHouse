import sys
from pathlib import Path

from pyspark.sql import functions as F

# Ensure project root is in python path
project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from spark.utils.spark_session import get_spark_session


def run_vertical_slice(source_hour: str = "2024-01-01-0"):
    print(f"--- Starting Vertical Slice Proof for {source_hour} ---")
    spark = get_spark_session(f"VerticalSlice-{source_hour}")

    # Determine input path
    # If MinIO is accessible, read from S3 landing path; otherwise fallback to fixture
    landing_s3_path = f"s3a://lakehouse/landing/gharchive/year=2024/month=01/day=01/{source_hour}.json.gz"
    print(f"Reading Landing JSON from: {landing_s3_path}")

    # Read gzip-compressed NDJSON
    raw_df = spark.read.json(landing_s3_path)
    print(f"Raw landing rows read: {raw_df.count()}")
    raw_df.printSchema()

    # Extract typed columns for proof table
    proof_df = raw_df.select(
        F.col("id").cast("string").alias("event_id"),
        F.col("type").cast("string").alias("event_type"),
        F.col("actor.id").cast("long").alias("actor_id"),
        F.col("actor.login").cast("string").alias("actor_login"),
        F.col("repo.id").cast("long").alias("repo_id"),
        F.col("repo.name").cast("string").alias("repo_name"),
        F.to_timestamp(F.col("created_at")).alias("created_at"),
    ).filter(F.col("event_id").isNotNull())

    # Create catalog namespace and write to Iceberg proof table
    print("Creating namespace 'iceberg.demo' if not exists...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.demo")

    table_name = "iceberg.demo.proof_events"
    print(f"Writing proof dataframe to Iceberg table: {table_name}...")
    proof_df.writeTo(table_name).createOrReplace()

    # Query back using Spark SQL
    print(f"Querying written Iceberg table: {table_name} via Spark SQL:")
    result_df = spark.sql(f"SELECT * FROM {table_name}")
    row_count = result_df.count()
    print(f"Total rows in {table_name}: {row_count}")
    result_df.show(5, truncate=False)

    spark.stop()
    print("--- Vertical Slice Spark Write Completed Successfully ---")
    return row_count


if __name__ == "__main__":
    run_vertical_slice()
