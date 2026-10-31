from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from spark.utils.bot_detector import BOT_DETECTOR_VERSION, is_bot_account
from spark.utils.enrichment import fetch_repo_metadata_from_api


def test_bot_detector_version():
    assert BOT_DETECTOR_VERSION == "v1.0"


@pytest.mark.parametrize(
    "login, expected",
    [
        ("dependabot[bot]", True),
        ("Dependabot[Bot]", True),
        ("github-actions[bot]", True),
        ("renovate[bot]", True),
        ("renovate-bot", True),
        ("snyk-bot", True),
        ("greenkeeper[bot]", True),
        ("codecov", True),
        ("my-service_bot", True),
        # Humans & false-positive defense
        ("renovatefoo", False),
        ("codecovx", False),
        ("torvalds", False),
        ("octocat", False),
        (
            "botanist",
            False,
        ),  # Contains "bot" substring but is not a bot suffix or keyword
        ("abbot", False),
        ("robotics-enthusiast", False),
        (None, False),
        ("", False),
        ("   ", False),
    ],
)
def test_is_bot_account(login: str | None, expected: bool):
    assert is_bot_account(login) == expected


def test_fetch_repo_metadata_success():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"language": "Python"}
    mock_resp.headers = {"ETag": 'W/"12345"'}

    with patch("requests.get", return_value=mock_resp):
        res = fetch_repo_metadata_from_api("octocat/Hello-World")
        assert res["primary_language"] == "Python"
        assert res["source"] == "github_api"
        assert res["etag"] == 'W/"12345"'
        assert res["not_modified"] is False


def test_fetch_repo_metadata_304_not_modified():
    mock_resp = MagicMock()
    mock_resp.status_code = 304
    mock_resp.headers = {}

    with patch("requests.get", return_value=mock_resp):
        res = fetch_repo_metadata_from_api(
            "octocat/Hello-World", cached_etag='W/"12345"'
        )
        assert res["primary_language"] == "cached"
        assert res["source"] == "github_api_304"
        assert res["etag"] == 'W/"12345"'
        assert res["not_modified"] is True


def test_fetch_repo_metadata_rate_limited_or_error_fallback():
    mock_resp = MagicMock()
    mock_resp.status_code = 403
    mock_resp.headers = {}

    with patch("requests.get", return_value=mock_resp):
        res = fetch_repo_metadata_from_api("octocat/Hello-World")
        # Fail-safe: A failed enrichment must never invent a language; sets primary_language to unknown
        assert res["primary_language"] == "unknown"
        assert res["source"] == "http_403"
        assert res["etag"] is None


def test_fetch_repo_metadata_network_exception():
    with patch(
        "requests.get", side_effect=requests.ConnectionError("Connection timed out")
    ):
        res = fetch_repo_metadata_from_api("octocat/Hello-World")
        assert res["primary_language"] == "unknown"
        assert "error" in res["source"]
        assert res["etag"] is None


def test_bot_regex_spark_word_boundaries():
    import re

    from spark.utils.bot_detector import BOT_REGEX_SPARK

    assert bool(re.search(BOT_REGEX_SPARK, "dependabot")) is True
    assert bool(re.search(BOT_REGEX_SPARK, "renovate[bot]")) is True
    assert bool(re.search(BOT_REGEX_SPARK, "user-bot")) is True
    assert bool(re.search(BOT_REGEX_SPARK, "renovatefoo")) is False
    assert bool(re.search(BOT_REGEX_SPARK, "codecovx")) is False
