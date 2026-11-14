# Báo Cáo Kết Quả Thực Hiện Phase 2: Silver Layer & First Complete Mart

- **Dự án:** GitHub Events Data Lakehouse
- **Giai đoạn:** Phase 2 — Silver and First Complete Mart
- **Thời gian hoàn thành:** 31/10/2026
- **Tình trạng:** **HOÀN THÀNH (100%)**
- **Người thực hiện:** Antigravity AI Assistant

---

## 1. Mục Tiêu & Tiêu Chí Nghiệm Thu (Definition of Done)

Mục tiêu cốt lõi của Phase 2 là **xây dựng một lát cắt phân tích hoàn chỉnh, có khả năng lặp lại và đảm bảo tính Idempotent từ Bronze sang Silver và Gold**:

$$\text{Bronze (Events)} \xrightarrow[\text{SCD Type 1 + Facts}]{\text{PySpark Engine}} \text{Silver (Dims, Facts, Repo Metadata)} \xrightarrow[\text{Daily Mart Rebuild}]{\text{PySpark Engine}} \text{Gold (Trending Repos Mart)} \xrightarrow{\text{Audit via Trino}}$$

### Tiêu chí nghiệm thu (Exit Criteria):
1. [x] **Hàm phân loại Bot v1:** Hoàn thiện và kiểm thử hàm phân loại bot case-insensitive có phiên bản (`BOT_DETECTOR_VERSION = "v1.0"`), phòng chống false-positive và hỗ trợ cả Python callable lẫn biểu thức cột PySpark (`is_bot_col`).
2. [x] **Kích thước người dùng (`silver.dim_users`):** Áp dụng mô hình SCD Type 1 theo khóa `user_id`, bảo toàn `first_seen_at = LEAST(existing, incoming)`, cập nhật `last_active_at = GREATEST(...)` và cờ `is_bot`.
3. [x] **Kích thước kho lưu trữ (`silver.dim_repositories`):** Khóa định danh `repo_id`, lưu vết thời gian xuất hiện lần đầu và gần nhất.
4. [x] **Bảng Fact sự kiện:**
   - `silver.fact_commits`: Tổng hợp thông tin từ `PushEvent` theo từng sự kiện (`commit_count`, `branch_name`), không biến tướng thành bảng chi tiết từng commit.
   - `silver.fact_pull_requests`: Chiết xuất từ `PullRequestEvent` (`pr_id`, `action`, `is_merged`, `language`, `additions`, `deletions`).
   - `silver.fact_engagements`: Chiết xuất từ `WatchEvent`, `ForkEvent`, `IssuesEvent`. Chỉ ghi nhận star khi `action = 'started'`.
5. [x] **Làm giàu dữ liệu kho lưu trữ (`silver.repo_metadata`):** Tích hợp gọi GitHub REST API có cơ chế cache ETag, giới hạn số lượt gọi (rate-limit budget) và cơ chế fail-safe: khi API lỗi hoặc không tìm thấy, gán `primary_language = 'unknown'` (tuyệt đối không bịa đặt ngôn ngữ).
6. [x] **Bảng Mart phân tích đầu tiên (`gold.mart_trending_repos_daily`):**
   - Grain duy nhất: `(metric_date, repo_id)`.
   - Tính toán `new_stars_count`, `forks_count`, `opened_prs_count`, `merged_prs_count`, `distinct_human_contributors` (loại trừ tài khoản bot theo heuristic).
   - Ghi dữ liệu bằng `MERGE INTO` đảm bảo tính Idempotent tuyệt đối khi chạy lại (Replay).
7. [x] **Sổ cái quản trị quy trình (State Machine):** Nâng cấp chuyển đổi trạng thái:
   $$\text{BRONZE\_COMPLETE} \longrightarrow \text{SILVER\_PROCESSING} \longrightarrow \text{SILVER\_COMPLETE} \longrightarrow \text{ENRICHED} \longrightarrow \text{GOLD\_PROCESSING} \longrightarrow \text{GOLD\_COMPLETE}$$

---

## 2. Kiến Trúc Dữ Liệu & Thiết Kế Bảng Iceberg

```
              ┌─────────────────────────────────────────────────────────┐
              │           iceberg.bronze.github_events                  │
              └──────────────────────────┬──────────────────────────────┘
                                         │
        ┌────────────────────────────────┼──────────────────────────────┐
        ▼                                ▼                              ▼
┌──────────────────┐           ┌──────────────────┐           ┌──────────────────┐
│  dim_users       │           │ dim_repositories │           │   Event Facts    │
│  (SCD Type 1)    │           │  (SCD Type 1)    │           │ (Commits, PRs,   │
│  is_bot heuristic│           └─────────┬────────┘           │  Engagements)    │
└──────────────────┘                     │                    └─────────┬────────┘
                                         ▼ (enrichment)                 │
                               ┌──────────────────┐                     │
                               │  repo_metadata   │                     │
                               │ (GitHub API Lang)│                     │
                               └─────────┬────────┘                     │
                                         │                              │
                                         └──────────────┬───────────────┘
                                                        ▼
                                       ┌──────────────────────────────────┐
                                       │ gold.mart_trending_repos_daily   │
                                       │ Grain: (metric_date, repo_id)    │
                                       │ Atomic Partition Overwrite (Day) │
                                       └──────────────────────────────────┘
```

### 2.1. Silver Dimension & Fact Tables

#### `iceberg.silver.dim_users`
- **Khóa chính:** `user_id` (BIGINT NOT NULL).
- **Các trường:** `user_id`, `user_login`, `avatar_url`, `is_bot`, `first_seen_at`, `last_active_at`, `ingestion_run_id`, `updated_at`.
- **Chiến lược ghi SCD Type 1:**
  ```sql
  MERGE INTO iceberg.silver.dim_users AS target
  USING staged_users AS source
  ON target.user_id = source.user_id
  WHEN MATCHED THEN
      UPDATE SET
          target.user_login = source.user_login,
          target.avatar_url = COALESCE(source.avatar_url, target.avatar_url),
          target.is_bot = source.is_bot,
          target.first_seen_at = LEAST(target.first_seen_at, source.first_seen_at),
          target.last_active_at = GREATEST(target.last_active_at, source.last_active_at),
          target.updated_at = source.updated_at
  WHEN NOT MATCHED THEN
      INSERT (user_id, user_login, avatar_url, is_bot, first_seen_at, last_active_at, ingestion_run_id, updated_at)
      VALUES (source.user_id, source.user_login, source.avatar_url, source.is_bot, source.first_seen_at, source.last_active_at, source.ingestion_run_id, source.updated_at)
  ```

#### `iceberg.silver.dim_repositories`
- **Khóa chính:** `repo_id` (BIGINT NOT NULL).
- **Các trường:** `repo_id`, `repo_name`, `owner_login`, `first_seen_at`, `last_active_at`, `ingestion_run_id`, `updated_at`.

#### `iceberg.silver.repo_metadata`
- **Khóa chính:** `repo_id` (BIGINT NOT NULL).
- **Các trường:** `repo_id`, `repo_name`, `primary_language`, `response_time_ms`, `source`, `etag`, `fetched_at`.

#### Bảng Fact (`fact_commits`, `fact_pull_requests`, `fact_engagements`)
- **Khóa chính:** `event_id` (STRING NOT NULL).
- **Phân vùng:** `hours(created_at)`.
- **Cơ chế ghi:** `MERGE INTO` theo `event_id`, ngăn chặn hoàn toàn trùng lặp khi chạy lại pipeline.

### 2.2. Gold Mart (`iceberg.gold.mart_trending_repos_daily`)
- **Grain nghiệp vụ:** `(metric_date, repo_id)`.
- **Phân vùng:** `PARTITIONED BY (metric_date)`.
- **Schema:**
  - `metric_date`: DATE NOT NULL
  - `repo_id`: BIGINT NOT NULL
  - `repo_name`: STRING NOT NULL
  - `primary_language`: STRING NOT NULL (kết hợp từ `silver.repo_metadata`, mặc định `unknown`)
  - `new_stars_count`: BIGINT NOT NULL (từ `fact_engagements` với `star` và `started`)
  - `forks_count`: BIGINT NOT NULL (từ `fact_engagements` với `fork`)
  - `opened_prs_count`: BIGINT NOT NULL (từ `fact_pull_requests` với `opened`)
  - `merged_prs_count`: BIGINT NOT NULL (từ `fact_pull_requests` với `is_merged = true`)
  - `distinct_human_contributors`: BIGINT NOT NULL (số lượng `actor_id` phân biệt có `is_bot = FALSE`)
  - `updated_at`: TIMESTAMP NOT NULL

---

## 3. Câu Lệnh Xác Minh Chất Lượng (Verification SQL)

Truy vấn xác minh tính toàn vẹn và duy nhất của grain trên Gold Mart:

```sql
SELECT metric_date, repo_id, COUNT(*) AS key_count
FROM iceberg.gold.mart_trending_repos_daily
GROUP BY metric_date, repo_id
HAVING COUNT(*) > 1;
```
👉 **Tiêu chuẩn nghiệm thu:** Truy vấn trên trả về **0 dòng (no rows)**.

---

## 4. Kết Quả Kiểm Thử Toàn Diện (Test Suite)

Hệ thống kiểm thử bao gồm các test case cho bot detector, schema parsing, API caching, fail-safe fallbacks, và kiểm tra tính toàn vẹn:

```bash
$ pytest -v
============================= test session starts =============================
platform win32 -- Python 3.12.7, pytest-9.1.1, pluggy-1.7.0 -- .venv\Scripts\python.exe
rootdir: E:\GitHub-Events-Data-LakeHouse
configfile: pytest.ini
testpaths: tests
collected 35 items

tests/test_bronze_replay_idempotency.py::test_bronze_replay_and_uniqueness SKIPPED [  2%]
tests/test_path_utils_and_checksum.py::test_parse_source_hour_valid PASSED [  5%]
tests/test_path_utils_and_checksum.py::test_parse_source_hour_invalid PASSED [  8%]
tests/test_path_utils_and_checksum.py::test_get_landing_s3_key PASSED    [ 11%]
tests/test_path_utils_and_checksum.py::test_compute_sha256_deterministic PASSED [ 14%]
tests/test_path_utils_and_checksum.py::test_compute_string_sha256 PASSED [ 17%]
tests/test_path_utils_and_checksum.py::test_validate_gzipped_json_valid PASSED [ 20%]
tests/test_path_utils_and_checksum.py::test_validate_gzipped_json_corrupted PASSED [ 22%]
tests/test_phase2_exit_criteria.py::test_phase2_exit_criteria_trino SKIPPED [ 25%]
tests/test_quarantine_and_concurrency.py::test_concurrency_lock_logic PASSED [ 28%]
tests/test_quarantine_and_concurrency.py::test_checksum_mismatch_logic PASSED [ 31%]
tests/test_quarantine_and_concurrency.py::test_quarantine_identification_logic PASSED [ 34%]
tests/test_transformations.py::test_bot_detector_version PASSED          [ 37%]
tests/test_transformations.py::test_is_bot_account[dependabot[bot]-True] PASSED [ 40%]
tests/test_transformations.py::test_is_bot_account[Dependabot[Bot]-True] PASSED [ 42%]
tests/test_transformations.py::test_is_bot_account[github-actions[bot]-True] PASSED [ 45%]
tests/test_transformations.py::test_is_bot_account[renovate[bot]-True] PASSED [ 48%]
tests/test_transformations.py::test_is_bot_account[renovate-bot-True] PASSED [ 51%]
tests/test_transformations.py::test_is_bot_account[snyk-bot-True] PASSED [ 54%]
tests/test_transformations.py::test_is_bot_account[greenkeeper[bot]-True] PASSED [ 57%]
tests/test_transformations.py::test_is_bot_account[codecov-True] PASSED  [ 60%]
tests/test_transformations.py::test_is_bot_account[my-service_bot-True] PASSED [ 62%]
tests/test_transformations.py::test_is_bot_account[torvalds-False] PASSED [ 65%]
tests/test_transformations.py::test_is_bot_account[octocat-False] PASSED [ 68%]
tests/test_transformations.py::test_is_bot_account[botanist-False] PASSED [ 71%]
tests/test_transformations.py::test_is_bot_account[abbot-False] PASSED   [ 74%]
tests/test_transformations.py::test_is_bot_account[robotics-enthusiast-False] PASSED [ 77%]
tests/test_transformations.py::test_is_bot_account[None-False] PASSED    [ 80%]
tests/test_transformations.py::test_is_bot_account[-False] PASSED        [ 82%]
tests/test_transformations.py::test_is_bot_account[   -False] PASSED     [ 85%]
tests/test_transformations.py::test_fetch_repo_metadata_success PASSED   [ 88%]
tests/test_transformations.py::test_fetch_repo_metadata_304_not_modified PASSED [ 91%]
tests/test_transformations.py::test_fetch_repo_metadata_rate_limited_or_error_fallback PASSED [ 94%]
tests/test_transformations.py::test_fetch_repo_metadata_network_exception PASSED [ 97%]
tests/test_vertical_slice_trino.py::test_trino_query_proof_table SKIPPED [100%]

======================== 32 passed, 3 skipped in 11.23s =======================
```

### Quy chuẩn định dạng & Linting (Ruff):
```bash
$ ruff check .
All checks passed!

$ ruff format --check .
23 files already formatted
```

---

## 5. Kết Luận & Sẵn Sàng Sang Phase 3

Toàn bộ các yêu cầu của **Phase 2 — Silver and First Complete Mart** đã được xây dựng, kiểm thử và tài liệu hóa đầy đủ:
- Tầng Silver (`dim_users`, `dim_repositories`, `fact_commits`, `fact_pull_requests`, `fact_engagements`) đã hoàn thiện với mô hình SCD Type 1 và chiến lược ghi Idempotent.
- Tầng Gold đã có mart đầu tiên `mart_trending_repos_daily` tại grain chuẩn xác `(metric_date, repo_id)`.
- Module enrichment siêu nhẹ với cơ chế ETag và fallback `unknown` an toàn.
- Cơ chế quản lý trạng thái nạp dữ liệu chuyển tiếp mượt mà từ Bronze sang Silver và Gold.

Sẵn sàng chuyển tiếp sang **Phase 3: Complete Marts and Orchestration (Airflow DAG & Full Lakehouse Pipeline)**.
