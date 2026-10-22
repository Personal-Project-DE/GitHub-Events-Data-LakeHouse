import argparse
import hashlib
import os
from pathlib import Path

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parents[2] / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)


def compute_sha256(file_path: Path) -> str:
    """Computes SHA-256 checksum of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def get_s3_client(endpoint_url: str | None = None):
    """Creates a boto3 S3 client for MinIO."""
    endpoint = endpoint_url or os.getenv("MINIO_HOST_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")
    region = os.getenv("AWS_REGION", "us-east-1")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )


def parse_source_hour(source_hour: str) -> tuple[str, str, str, str]:
    """
    Parses 'YYYY-MM-DD-H' into (year, month, day, hour).
    Example: '2024-01-01-0' -> ('2024', '01', '01', '0')
    """
    parts = source_hour.strip().split("-")
    if len(parts) != 4:
        raise ValueError(
            f"Invalid source_hour format '{source_hour}'. Expected 'YYYY-MM-DD-H'."
        )
    return parts[0], parts[1], parts[2], parts[3]


def get_landing_s3_key(source_hour: str) -> str:
    """Builds canonical landing key in MinIO."""
    year, month, day, _ = parse_source_hour(source_hour)
    return (
        f"landing/gharchive/year={year}/month={month}/day={day}/{source_hour}.json.gz"
    )


def download_and_land_source_hour(
    source_hour: str,
    target_dir: Path | None = None,
    upload_to_s3: bool = True,
    fixture_path: Path | None = None,
    s3_endpoint: str | None = None,
) -> dict:
    """
    Downloads GH Archive file or uses fixture, computes sha256, and uploads to MinIO.
    Returns metadata dict: {source_hour, local_path, s3_key, checksum, size_bytes}.
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
        url = f"https://data.gharchive.org/{source_hour}.json.gz"
        print(f"Downloading {url} to {local_file}...")
        headers = {"User-Agent": "GitHubEventsLakehouseDownloader/1.0"}
        resp = requests.get(url, headers=headers, stream=True, timeout=60)
        resp.raise_for_status()
        with open(local_file, "wb") as f:
            f.writelines(resp.iter_content(chunk_size=65536))

    checksum = compute_sha256(local_file)
    size_bytes = local_file.stat().st_size
    s3_key = get_landing_s3_key(source_hour)
    bucket = "lakehouse"

    if upload_to_s3:
        s3 = get_s3_client(s3_endpoint)
        # Ensure bucket exists
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception as e:  # noqa: BLE001
            print(f"Bucket {bucket} not found or inaccessible ({e}), creating...")
            s3.create_bucket(Bucket=bucket)

        print(f"Uploading {local_file} to s3://{bucket}/{s3_key}...")
        s3.upload_file(str(local_file), bucket, s3_key)
        print(f"Uploaded successfully. Size: {size_bytes} bytes, SHA-256: {checksum}")

    return {
        "source_hour": source_hour,
        "local_path": str(local_file),
        "s3_path": f"s3a://{bucket}/{s3_key}",
        "checksum": checksum,
        "size_bytes": size_bytes,
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
