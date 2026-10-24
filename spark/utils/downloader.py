from __future__ import annotations

import argparse
import gzip
import os
from pathlib import Path

from dotenv import load_dotenv

from spark.utils.path_utils import (
    compute_sha256,
    get_landing_s3_key,
    parse_source_hour,
)

try:
    import boto3
    from botocore.client import Config
except ImportError:
    boto3 = None
    Config = None

try:
    import requests
except ImportError:
    requests = None

env_path = Path(__file__).resolve().parents[2] / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)


def validate_gzipped_json(file_path: Path) -> int:
    """
    Validates that a file exists, is non-zero in size, and can be decompressed as gzip.
    Returns the count of non-empty lines.
    """
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"Landing file not found: {p}")
    if p.stat().st_size == 0:
        raise ValueError(f"Landing file is empty (0 bytes): {p}")

    line_count = 0
    try:
        with gzip.open(p, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.strip():
                    line_count += 1
    except Exception as e:
        raise ValueError(f"Failed to decompress and read gzip file {p}: {e}") from e

    if line_count == 0:
        raise ValueError(f"Landing file has no valid data lines: {p}")

    return line_count


def get_s3_client(endpoint_url: str | None = None):
    """Creates a boto3 S3 client for MinIO."""
    if boto3 is None:
        raise ImportError("boto3 is not installed in the current environment.")

    is_docker = os.path.exists("/.dockerenv")
    default_endpoint = (
        os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        if is_docker
        else os.getenv("MINIO_HOST_ENDPOINT", "http://localhost:9010")
    )
    endpoint = endpoint_url or default_endpoint
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
    region = os.getenv("AWS_REGION", "us-east-1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4") if Config else None,
    )


def validate_landing_s3_object(s3_client, bucket: str, s3_key: str) -> dict:
    """
    Verifies that the object exists in MinIO S3 and is non-zero in size.
    """
    head = s3_client.head_object(Bucket=bucket, Key=s3_key)
    size_bytes = head.get("ContentLength", 0)
    if size_bytes == 0:
        raise ValueError(f"MinIO object s3://{bucket}/{s3_key} is 0 bytes.")
    return {
        "bucket": bucket,
        "key": s3_key,
        "size_bytes": size_bytes,
        "etag": head.get("ETag", "").strip('"'),
    }


def download_and_land_source_hour(
    source_hour: str,
    target_dir: Path | None = None,
    upload_to_s3: bool = True,
    fixture_path: Path | None = None,
    s3_endpoint: str | None = None,
) -> dict:
    """
    Downloads GH Archive file or uses fixture, validates gzip integrity,
    computes sha256, and uploads to MinIO.
    Returns metadata dict: {source_hour, local_path, s3_path, checksum, size_bytes, line_count}.
    """
    _year, _month, _day, _hour = parse_source_hour(source_hour)
    target_dir = target_dir or (
        Path(__file__).resolve().parents[2] / "data" / "landing"
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    local_file = target_dir / f"{source_hour}.json.gz"

    if fixture_path and Path(fixture_path).exists():
        print(f"Loading from fixture: {fixture_path}")
        with open(fixture_path, "rb") as src, open(local_file, "wb") as dst:
            dst.write(src.read())
    else:
        if requests is None:
            raise ImportError("requests library is required for downloading.")
        url = f"https://data.gharchive.org/{source_hour}.json.gz"
        print(f"Downloading {url} to {local_file}...")
        headers = {"User-Agent": "GitHubEventsLakehouseDownloader/1.0"}
        resp = requests.get(url, headers=headers, stream=True, timeout=60)
        resp.raise_for_status()
        with open(local_file, "wb") as f:
            f.writelines(resp.iter_content(chunk_size=65536))

    # Landing Integrity Validation
    line_count = validate_gzipped_json(local_file)
    checksum = compute_sha256(local_file)
    size_bytes = local_file.stat().st_size
    s3_key = get_landing_s3_key(source_hour)
    bucket = "lakehouse"

    if upload_to_s3:
        s3 = get_s3_client(s3_endpoint)
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception as e:  # noqa: BLE001
            print(f"Bucket {bucket} not found or inaccessible ({e}), creating...")
            s3.create_bucket(Bucket=bucket)

        print(f"Uploading {local_file} to s3://{bucket}/{s3_key}...")
        s3.upload_file(str(local_file), bucket, s3_key)
        validate_landing_s3_object(s3, bucket, s3_key)
        print(
            f"Uploaded successfully. Size: {size_bytes} bytes, SHA-256: {checksum}, Records: {line_count}"
        )

    return {
        "source_hour": source_hour,
        "local_path": str(local_file),
        "s3_path": f"s3a://{bucket}/{s3_key}",
        "checksum": checksum,
        "size_bytes": size_bytes,
        "line_count": line_count,
    }


def main():
    parser = argparse.ArgumentParser(description="GH Archive Landing Downloader CLI")
    parser.add_argument(
        "--source-hour",
        type=str,
        default="2024-01-01-0",
        help="Source hour in YYYY-MM-DD-H format (e.g. 2024-01-01-0)",
    )
    parser.add_argument(
        "--fixture",
        type=str,
        default=None,
        help="Optional path to local fixture file instead of downloading",
    )
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Skip uploading to MinIO S3",
    )
    args = parser.parse_args()

    meta = download_and_land_source_hour(
        source_hour=args.source_hour,
        upload_to_s3=not args.no_s3,
        fixture_path=Path(args.fixture) if args.fixture else None,
    )
    print("Result:", meta)


if __name__ == "__main__":
    main()
