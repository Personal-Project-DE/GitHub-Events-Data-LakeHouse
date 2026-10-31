from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from spark.utils.downloader import (
    download_and_land_source_hour,
    get_s3_client,
    validate_landing_s3_object,
)


def test_validate_landing_s3_object_valid():
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {
        "ContentLength": 54321,
        "ETag": '"abcdef123456"',
    }
    meta = validate_landing_s3_object(mock_s3, "lakehouse", "landing/test.json.gz")
    assert meta["bucket"] == "lakehouse"
    assert meta["key"] == "landing/test.json.gz"
    assert meta["size_bytes"] == 54321
    assert meta["etag"] == "abcdef123456"


def test_validate_landing_s3_object_empty_raises():
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {
        "ContentLength": 0,
        "ETag": '"empty"',
    }
    with pytest.raises(ValueError, match="is 0 bytes"):
        validate_landing_s3_object(mock_s3, "lakehouse", "landing/empty.json.gz")


def test_download_and_land_source_hour_from_fixture():
    fixture_file = (
        Path(__file__).resolve().parent
        / "fixtures"
        / "landing"
        / "2024-01-01-0.json.gz"
    )
    assert fixture_file.exists(), f"Fixture file not found: {fixture_file}"

    with tempfile.TemporaryDirectory() as tmpdir:
        res = download_and_land_source_hour(
            source_hour="2024-01-01-0",
            target_dir=Path(tmpdir),
            upload_to_s3=False,
            fixture_path=fixture_file,
        )

        assert res["source_hour"] == "2024-01-01-0"
        assert res["size_bytes"] > 0
        assert res["line_count"] == 4
        assert len(res["checksum"]) == 64
        assert Path(res["local_path"]).exists()


def test_download_and_land_source_hour_mock_http():
    fixture_file = (
        Path(__file__).resolve().parent
        / "fixtures"
        / "landing"
        / "2024-01-01-0.json.gz"
    )
    with open(fixture_file, "rb") as f:
        gz_bytes = f.read()

    mock_resp = MagicMock()
    mock_resp.iter_content.return_value = [gz_bytes]
    mock_resp.raise_for_status = MagicMock()

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        patch("requests.get", return_value=mock_resp),
    ):
        res = download_and_land_source_hour(
            source_hour="2024-01-01-0",
            target_dir=Path(tmpdir),
            upload_to_s3=False,
            fixture_path=None,
        )

        assert res["source_hour"] == "2024-01-01-0"
        assert res["size_bytes"] == len(gz_bytes)
        assert res["line_count"] == 4
        assert len(res["checksum"]) == 64


def test_get_s3_client_initialization():
    with patch("spark.utils.downloader.boto3.client") as mock_client:
        mock_client.return_value = MagicMock()
        client = get_s3_client("http://localhost:9010")
        assert client is not None
        assert mock_client.called
