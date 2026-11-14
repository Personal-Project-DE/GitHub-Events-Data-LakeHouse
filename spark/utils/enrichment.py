from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_metadata_schema():
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("repo_id", LongType(), False),
            StructField("repo_name", StringType(), False),
            StructField("primary_language", StringType(), False),
            StructField("response_time_ms", IntegerType(), True),
            StructField("source", StringType(), False),
            StructField("etag", StringType(), True),
            StructField("fetched_at", TimestampType(), False),
        ]
    )


def ensure_repo_metadata_table(spark: SparkSession) -> None:
    """Creates iceberg.silver.repo_metadata if not exists."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.repo_metadata (
            repo_id BIGINT NOT NULL,
            repo_name STRING NOT NULL,
            primary_language STRING NOT NULL,
            response_time_ms INT,
            source STRING NOT NULL,
            etag STRING,
            fetched_at TIMESTAMP NOT NULL
        )
        USING iceberg
        """
    )


def fetch_repo_metadata_from_api(
    repo_name: str,
    token: str | None = None,
    cached_etag: str | None = None,
    timeout_sec: float = 3.0,
) -> dict[str, Any]:
    """
    Fetches repo metadata from GitHub REST API with ETag and rate-limit awareness.
    Guarantees returning a dictionary with primary_language ('unknown' on failure or missing).
    """
    url = f"https://api.github.com/repos/{repo_name}"
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "GitHub-Events-Data-LakeHouse-Enricher/1.0",
    }
    github_token = token or os.getenv("GITHUB_TOKEN")
    if github_token:
        headers["Authorization"] = f"token {github_token}"
    if cached_etag:
        headers["If-None-Match"] = cached_etag

    start_time = time.time()
    try:
        response = requests.get(url, headers=headers, timeout=timeout_sec)
        elapsed_ms = int((time.time() - start_time) * 1000)

        if response.status_code == 304:
            return {
                "primary_language": "cached",
                "response_time_ms": elapsed_ms,
                "source": "github_api_304",
                "etag": cached_etag,
                "not_modified": True,
            }

        if response.status_code == 200:
            data = response.json()
            lang = data.get("language") or "unknown"
            etag = response.headers.get("ETag")
            return {
                "primary_language": str(lang),
                "response_time_ms": elapsed_ms,
                "source": "github_api",
                "etag": etag,
                "not_modified": False,
            }

        # Handle 403 (Rate limit), 404 (Not found / private / renamed), etc.
        return {
            "primary_language": "unknown",
            "response_time_ms": elapsed_ms,
            "source": f"http_{response.status_code}",
            "etag": None,
            "not_modified": False,
        }
    except (requests.RequestException, OSError, ValueError) as exc:
        elapsed_ms = int((time.time() - start_time) * 1000)
        return {
            "primary_language": "unknown",
            "response_time_ms": elapsed_ms,
            "source": f"error_{type(exc).__name__}",
            "etag": None,
            "not_modified": False,
        }


def enrich_repositories(
    spark: SparkSession,
    repo_list: list[dict[str, Any]],
    force_refresh: bool = False,
    max_api_calls: int = 50,
) -> int:
    """
    Enriches given repositories into iceberg.silver.repo_metadata.
    - Checks for existing cached entries in silver.repo_metadata.
    - Only calls GitHub API for uncached repositories (up to max_api_calls limit).
    - Writes/merges results into iceberg.silver.repo_metadata.
    Returns number of newly enriched repositories.
    """
    ensure_repo_metadata_table(spark)

    if not repo_list:
        return 0

    # Read existing metadata to find already cached repos
    existing_df = spark.sql(
        "SELECT repo_id, repo_name, primary_language, etag FROM iceberg.silver.repo_metadata"
    )
    existing_map = {row["repo_id"]: row.asDict() for row in existing_df.collect()}

    records_to_upsert: list[dict[str, Any]] = []
    api_calls_count = 0
    now_dt = datetime.now(timezone.utc)  # noqa: UP017

    for item in repo_list:
        repo_id = item["repo_id"]
        repo_name = item.get("repo_name", "")
        if not repo_id or not repo_name or "/" not in repo_name:
            continue

        cached = existing_map.get(repo_id)
        if cached and not force_refresh:
            continue

        if api_calls_count >= max_api_calls:
            # Reached budget: record as unknown fallback to satisfy schema
            records_to_upsert.append(
                {
                    "repo_id": repo_id,
                    "repo_name": repo_name,
                    "primary_language": cached["primary_language"]
                    if cached
                    else "unknown",
                    "response_time_ms": 0,
                    "source": "budget_limit_fallback",
                    "etag": cached.get("etag") if cached else None,
                    "fetched_at": now_dt,
                }
            )
            continue

        cached_etag = cached.get("etag") if cached else None
        res = fetch_repo_metadata_from_api(repo_name, cached_etag=cached_etag)
        api_calls_count += 1

        primary_lang = res["primary_language"]
        if res.get("not_modified") and cached:
            primary_lang = cached["primary_language"]

        records_to_upsert.append(
            {
                "repo_id": repo_id,
                "repo_name": repo_name,
                "primary_language": primary_lang,
                "response_time_ms": res.get("response_time_ms", 0),
                "source": res.get("source", "github_api"),
                "etag": res.get("etag"),
                "fetched_at": now_dt,
            }
        )

    if not records_to_upsert:
        return 0

    metadata_schema = get_metadata_schema()
    upsert_df = spark.createDataFrame(records_to_upsert, schema=metadata_schema)
    upsert_df.createOrReplaceTempView("staged_repo_metadata")

    spark.sql(
        """
        MERGE INTO iceberg.silver.repo_metadata AS target
        USING staged_repo_metadata AS source
        ON target.repo_id = source.repo_id
        WHEN MATCHED THEN
            UPDATE SET
                target.repo_name = source.repo_name,
                target.primary_language = source.primary_language,
                target.response_time_ms = source.response_time_ms,
                target.source = source.source,
                target.etag = source.etag,
                target.fetched_at = source.fetched_at
        WHEN NOT MATCHED THEN
            INSERT (repo_id, repo_name, primary_language, response_time_ms, source, etag, fetched_at)
            VALUES (source.repo_id, source.repo_name, source.primary_language, source.response_time_ms, source.source, source.etag, source.fetched_at)
        """
    )

    return len(records_to_upsert)


def check_repo_liveness(
    repo_names: list[str],
    max_workers: int = 15,
    timeout_sec: float = 2.0,
) -> set[str]:
    """
    Checks HTTP accessibility of repo names concurrently using lightweight HEAD requests.
    Returns a set of confirmed INACCESSIBLE (404/410/deleted) repository names.
    """
    import concurrent.futures
    import urllib.error
    import urllib.request

    dead_repos: set[str] = set()
    if not repo_names:
        return dead_repos

    def _probe(repo_name: str) -> tuple[str, bool]:
        if not repo_name or "/" not in repo_name or repo_name == "unknown":
            return repo_name, False
        url = f"https://github.com/{repo_name}"
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
            method="HEAD",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                # 200, 301, 302 mean the repo is alive / redirected
                return repo_name, resp.status < 400
        except urllib.error.HTTPError as http_err:
            if http_err.code in (404, 410):
                return repo_name, False
            # 429 or 403 on github web: treat as alive to avoid false negative
            return repo_name, True
        except Exception:  # noqa: BLE001
            # Network timeout / intermittent error: do not penalize
            return repo_name, True

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(_probe, repo_names)
        for r_name, is_alive in results:
            if not is_alive:
                dead_repos.add(r_name)

    return dead_repos
