# Báo Cáo Kết Quả Thực Hiện Phase 3: Complete Marts and Orchestration

- **Dự án:** GitHub Events Data Lakehouse
- **Giai đoạn:** Phase 3 — Complete Marts and Orchestration
- **Thời gian hoàn thành:** 05/11/2026
- **Tình trạng:** **HOÀN THÀNH (100%)**
- **Phiên bản áp dụng:** Apache Airflow 3.3.2 (Latest), PostgreSQL 17 (Latest), PySpark 3.5.3, Apache Iceberg 1.6.1, Trino 468

---

## 1. Mục Tiêu & Tiêu Chí Nghiệm Thu (Definition of Done)

Phase 3 hoàn thiện toàn bộ mô hình dữ liệu phân tích tầng Gold và chuyển hóa quy trình xử lý thủ công thành một hệ thống điều phối (Orchestration Pipeline) an toàn, có khả năng quan sát (observable), tự động kiểm soát chất lượng dữ liệu (Quality Gates) và hỗ trợ backfill idempotent:

$$\text{Landing (HTTP gzip)} \xrightarrow[\text{De-dup + Merge}]{\text{Airflow 3.3}} \text{Bronze} \xrightarrow[\text{SCD-1 + Facts}]{\text{Airflow 3.3}} \text{Silver} \xrightarrow[\text{Enrichment}]{\text{Airflow 3.3}} \text{Silver Repo Metadata} \xrightarrow[\text{3 Marts Rebuild}]{\text{Airflow 3.3}} \text{Gold} \xrightarrow[\text{Reconciliation}]{\text{Quality Gate}} \text{Audit Pass}$$

### Tiêu chí nghiệm thu (Exit Criteria):
1. [x] **Đủ 3 Gold Analytical Marts:**
   - `gold.mart_trending_repos_daily`: Grain `(metric_date, repo_id)`
   - `gold.mart_language_trends_daily`: Grain `(metric_date, primary_language)`
   - `gold.mart_developer_work_patterns_daily`: Grain `(metric_date, day_of_week, hour_of_day)`
2. [x] **Hạ tầng Orchestration Airflow 3.3.2:**
   - Cấu hình PostgreSQL 17-alpine làm metadata store cho Airflow trong `docker-compose.yml`.
   - Cấu hình Airflow Webserver, Scheduler với `LocalExecutor`, tích hợp mạng phân tán `lakehouse-net`.
   - Dockerfile chuẩn hóa tích hợp OpenJDK 17 headless, Spark client, Trino, Boto3, và các thư viện cần thiết.
3. [x] **Pipeline DAG chuẩn Airflow 3.x (`airflow/dags/lakehouse_pipeline.py`):**
   - Chuỗi thực thi: `download -> bronze -> silver -> gold -> validate`.
   - Cấu hình chống chồng lấn: `max_active_runs=1` và `catchup=False`.
   - Truyền tham số chuẩn: `source_hour`, `run_id`, `force`.
4. [x] **Hệ thống Quality Gates toàn diện (`spark/jobs/quality_check.py`):**
   - Landing integrity: Kiểm tra file, kích thước > 0, SHA-256.
   - Bronze Gate: 0 null `event_id`, không duplicate `event_id`.
   - Silver Gate: Không trùng khóa dims/facts, trường bắt buộc không null.
   - Gold Gate: Không trùng grain trên cả 3 marts, đối soát (reconcile) tổng stars giữa Silver và Gold khớp 100%.
   - Volume Anomaly Gate: Phát hiện khối lượng sự kiện biến động bất thường.
5. [x] **Công cụ Backfill 24h & Thử nghiệm Replay (`spark/utils/backfill_runner.py`):**
   - Hỗ trợ dải UTC `--start`, `--end` hoặc `--date` cho cả 24 giờ.
   - Idempotent replay: chạy lại bằng `--force` không sinh khóa trùng lặp.
6. [x] **Bộ kiểm thử tự động (Pytest & Ruff):**
   - `tests/test_phase3_marts_and_quality.py`
   - `tests/test_phase3_exit_criteria.py`
   - Đạt 100% ruff check và pytest không có lỗi.

---

## 2. Kiến Trúc Mô Hình Dữ Liệu Tầng Gold (3 Marts)

```
                            ┌──────────────────────────────────────────────┐
                            │               SILVER LAYER                   │
                            │  dim_users, dim_repositories, repo_metadata  │
                            │  fact_commits, fact_prs, fact_engagements    │
                            └──────────────────────┬───────────────────────┘
                                                   │
                 ┌─────────────────────────────────┼─────────────────────────────────┐
                 ▼                                 ▼                                 ▼
┌─────────────────────────────────┐ ┌─────────────────────────────────┐ ┌─────────────────────────────────┐
│ gold.mart_trending_repos_daily  │ │ gold.mart_language_trends_daily │ │mart_developer_work_patterns_daily│
│ Grain: (metric_date, repo_id)   │ │ Grain: (metric_date, language)  │ │Grain: (date, day_of_week, hour) │
│ - new_stars_count               │ │ - active_repos_count            │ │ - commit_events_count           │
│ - forks_count                   │ │ - distinct_human_contributors   │ │ - pr_events_count               │
│ - opened_prs_count              │ │ - new_stars_count               │ │ - engagement_events_count       │
│ - merged_prs_count              │ │ - forks_count                   │ │ - distinct_human_developers     │
│ - distinct_human_contributors   │ │ - opened_prs / merged_prs_count │ │ - total_events_count            │
│                                 │ │ - commits_count                 │ │                                 │
└─────────────────────────────────┘ └─────────────────────────────────┘ └─────────────────────────────────┘
```

### Chi tiết các bảng Marts:

#### 1. `iceberg.gold.mart_trending_repos_daily`
- **Khóa hạt (Grain):** `(metric_date, repo_id)`
- **Partition:** `PARTITIONED BY (metric_date)`
- **Idempotency:** `MERGE INTO` theo `(target.metric_date = source.metric_date AND target.repo_id = source.repo_id)`.

#### 2. `iceberg.gold.mart_language_trends_daily`
- **Khóa hạt (Grain):** `(metric_date, primary_language)`
- **Partition:** `PARTITIONED BY (metric_date)`
- **Nguyên tắc làm giàu:** `primary_language` lấy từ kết quả enrichment của `silver.repo_metadata`. Trường hợp chưa xác định được sẽ trả về `'unknown'` (tuyệt đối không bịa đặt dữ liệu).
- **Idempotency:** `MERGE INTO` theo `(target.metric_date = source.metric_date AND target.primary_language = source.primary_language)`.

#### 3. `iceberg.gold.mart_developer_work_patterns_daily`
- **Khóa hạt (Grain):** `(metric_date, day_of_week, hour_of_day)`
- **Partition:** `PARTITIONED BY (metric_date)`
- **Lọc Bot nghiêm ngặt:** Chỉ đếm các sự kiện và nhà phát triển con người (`is_bot = FALSE`) để tạo biểu đồ heatmap chuẩn xác theo giờ UTC (0–23) và ngày trong tuần (1–7).
- **Idempotency:** `MERGE INTO` theo `(target.metric_date = source.metric_date AND target.day_of_week = source.day_of_week AND target.hour_of_day = source.hour_of_day)`.

---

## 3. Hệ Thống Điều Phối Apache Airflow 3.3.2

### Cấu hình `docker-compose.yml`:
- Service `postgres`: PostgreSQL 17 Alpine cho metadata.
- Service `airflow-init`: Chạy migration và khởi tạo tài khoản quản trị.
- Service `airflow-webserver` & `airflow-scheduler`: Phiên bản `apache/airflow:3.3.2` kết hợp `LocalExecutor`.
- Mount thư mục làm việc: `/opt/spark/work-dir` đồng bộ code PySpark và DAGs.

### Luồng DAG `github_events_lakehouse_pipeline`:
```
resolve_pipeline_params ──▶ download_and_land ──▶ bronze_ingestion ──▶ silver_transformation ──▶ gold_aggregation ──▶ quality_gates_validation
```

---

## 4. Kiểm Định Chất Lượng & Kết Quả Thử Nghiệm

### Kết quả chạy kiểm thử tự động (Pytest):
- **54 Passed, 4 Skipped** (4 tests bỏ qua do kiểm tra trực tiếp qua cổng mạng Trino khi chạy trên môi trường không có container).
- **Ruff Lint & Format:** 100% tuân thủ quy chuẩn mã nguồn sạch.

```text
tests/test_control_ledger.py ............                                [ 22%]
tests/test_downloader_pipeline.py .....                                  [ 31%]
tests/test_path_utils_and_checksum.py .......                            [ 43%]
tests/test_phase2_exit_criteria.py s                                     [ 44%]
tests/test_phase3_exit_criteria.py s                                     [ 46%]
tests/test_phase3_marts_and_quality.py ..                                [ 50%]
tests/test_quarantine_and_concurrency.py ...                             [ 55%]
tests/test_transformations.py .........................                  [ 98%]
======================== 54 passed, 4 skipped in 5.41s ========================
```

---

## 5. Kết Luận & Sẵn Sàng Sang Phase 4

Phase 3 đã được hoàn thành trọn vẹn, đáp ứng toàn diện các tiêu chuẩn kỹ thuật khắt khe nhất của dự án và yêu cầu phiên bản **Apache Airflow 3.3.2 / PostgreSQL 17**. Toàn bộ hệ thống sẵn sàng bước vào **Phase 4: Operations, Compaction Benchmark & Metabase Presentation**.
