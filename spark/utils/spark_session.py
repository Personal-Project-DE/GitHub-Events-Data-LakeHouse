import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except ImportError:
    pass

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "GitHubEventsLakehouse") -> SparkSession:
    """Creates and returns a SparkSession configured for Apache Iceberg REST Catalog and MinIO S3."""
    # Detect if running inside Docker container or from host
    is_docker = os.path.exists("/.dockerenv")

    minio_endpoint = (
        os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        if is_docker
        else os.getenv("MINIO_HOST_ENDPOINT", "http://localhost:9000")
    )

    rest_catalog_uri = (
        os.getenv("REST_CATALOG_URI", "http://iceberg-rest:8181")
        if is_docker
        else os.getenv("REST_CATALOG_HOST_URI", "http://localhost:8181")
    )

    access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")
    warehouse = os.getenv("ICEBERG_WAREHOUSE_LOCATION", "s3a://lakehouse/warehouse")

    # Pinned versions for Iceberg runtime and AWS integration
    iceberg_version = "1.6.1"
    spark_version = "3.5_2.12"
    hadoop_version = "3.3.4"
    aws_sdk_version = "1.12.262"

    packages = [
        f"org.apache.iceberg:iceberg-spark-runtime-{spark_version}:{iceberg_version}",
        f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version}",
        f"org.apache.hadoop:hadoop-aws:{hadoop_version}",
        f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}",
    ]

    builder = (
        SparkSession.builder.appName(app_name)
        # Iceberg SQL Extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # Iceberg REST Catalog Setup
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", rest_catalog_uri)
        .config("spark.sql.catalog.iceberg.warehouse", warehouse)
        .config(
            "spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.iceberg.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", access_key)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", secret_key)
        .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
        # Hadoop S3A configurations for direct Landing reads
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.defaultCatalog", "iceberg")
    )

    # If running outside container, include Maven packages for standalone session
    if not is_docker:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    return builder.getOrCreate()


if __name__ == "__main__":
    spark = get_spark_session("SessionTest")
    print(f"Spark Version: {spark.version}")
    print(f"Catalogs: {spark.sql('SHOW CATALOGS').collect()}")
    spark.stop()
