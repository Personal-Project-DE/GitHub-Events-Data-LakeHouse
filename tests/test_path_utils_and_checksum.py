import gzip
import tempfile
from pathlib import Path

import pytest

from spark.utils.downloader import validate_gzipped_json
from spark.utils.path_utils import (
    compute_sha256,
    compute_string_sha256,
    get_landing_s3_key,
    parse_source_hour,
)


def test_parse_source_hour_valid():
    year, month, day, hour = parse_source_hour("2024-01-01-0")
    assert (year, month, day, hour) == ("2024", "01", "01", "0")

    year, month, day, hour = parse_source_hour("2023-12-31-23")
    assert (year, month, day, hour) == ("2023", "12", "31", "23")


def test_parse_source_hour_invalid():
    with pytest.raises(ValueError, match="Invalid source_hour format"):
        parse_source_hour("invalid-hour")

    with pytest.raises(ValueError, match="Invalid source_hour format"):
        parse_source_hour("2024-01-01")

    with pytest.raises(ValueError, match="Invalid source_hour parts"):
        parse_source_hour("24-01-01-0")


def test_get_landing_s3_key():
    key = get_landing_s3_key("2024-01-01-0")
    assert key == "landing/gharchive/year=2024/month=01/day=01/2024-01-01-0.json.gz"


def test_compute_sha256_deterministic():
    content = b'{"id": "123", "type": "PushEvent"}\n'
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)

    try:
        hash1 = compute_sha256(tmp_path)
        hash2 = compute_sha256(tmp_path)
        assert hash1 == hash2
        assert len(hash1) == 64
    finally:
        tmp_path.unlink()


def test_compute_string_sha256():
    payload = '{"id": "30000000001", "type": "WatchEvent"}'
    h1 = compute_string_sha256(payload)
    h2 = compute_string_sha256(payload)
    assert h1 == h2
    assert len(h1) == 64

    # Changing any byte must change the hash
    h3 = compute_string_sha256(payload + " ")
    assert h1 != h3


def test_validate_gzipped_json_valid():
    lines = [b'{"id": "1"}\n', b'{"id": "2"}\n', b'{"id": "3"}\n']
    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with gzip.open(tmp_path, "wb") as gz:
            gz.writelines(lines)

        count = validate_gzipped_json(tmp_path)
        assert count == 3
    finally:
        tmp_path.unlink()


def test_validate_gzipped_json_corrupted():
    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        tmp.write(b"not a gzip file at all")
        tmp_path = Path(tmp.name)

    try:
        with pytest.raises(ValueError, match="Failed to decompress and read gzip file"):
            validate_gzipped_json(tmp_path)
    finally:
        tmp_path.unlink()
