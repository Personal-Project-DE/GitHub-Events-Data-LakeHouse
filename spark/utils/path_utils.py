from __future__ import annotations

import hashlib
from pathlib import Path


def compute_sha256(file_path: Path | str) -> str:
    """Computes SHA-256 checksum of a file using standard library."""
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {p}")
    sha256 = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def compute_string_sha256(content: str) -> str:
    """Computes SHA-256 of a string."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


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
    year, month, day, hour = parts[0], parts[1], parts[2], parts[3]
    if not (len(year) == 4 and len(month) == 2 and len(day) == 2 and hour.isdigit()):
        raise ValueError(
            f"Invalid source_hour parts in '{source_hour}'. Expected 'YYYY-MM-DD-H'."
        )
    return year, month, day, hour


def get_landing_s3_key(source_hour: str) -> str:
    """Builds canonical landing key in MinIO."""
    year, month, day, _ = parse_source_hour(source_hour)
    return (
        f"landing/gharchive/year={year}/month={month}/day={day}/{source_hour}.json.gz"
    )
