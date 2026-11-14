# Báo Cáo Kết Quả Thực Hiện Phase 1: Reliable Landing and Bronze

- **Dự án:** GitHub Events Data Lakehouse
- **Giai đoạn:** Phase 1 — Reliable Landing and Bronze
- **Thời gian hoàn thành:** 24/10/2026
- **Tình trạng:** **HOÀN THÀNH (100%)**
- **Người thực hiện:** Antigravity AI Assistant

---

## 1. Mục Tiêu & Tiêu Chí Nghiệm Thu (Definition of Done)

Mục tiêu cốt lõi của Phase 1 là **xây dựng một quy trình nạp dữ liệu theo từng giờ nguồn (source-hour workflow) bất biến và có tính Idempotent (chạy lại an toàn mà không sinh ra dữ liệu trùng lặp)**:

$$\text{Landing Object (S3)} \xrightarrow[\text{SHA-256 + Gzip Validation}]{\text{Landing Integrity}} \text{Control Ledger (Lock)} \xrightarrow[\text{Fingerprint + Dedup}]{\text{PySpark Engine}} \text{Bronze Table (MERGE INTO)} \xrightarrow{\text{Audit via Trino}}$$

### Tiêu chí nghiệm thu (Exit Criteria):
1. [x] Tạo sổ cái quản lý nạp dữ liệu `iceberg.control.ingestion_runs` ghi lại phiên bản, mã hash SHA-256, run ID, số lượng dòng các stage, và khóa tránh chạy chồng chéo (concurrency lock).
2. [x] Tạo bảng Bronze `iceberg.bronze.github_events` phân vùng ẩn `hours(created_at)` và bảng kiểm dịch `iceberg.bronze.github_events_quarantine`.
3. [x] Nạp cùng 1 giờ dữ liệu 2 lần liên tiếp: số lượng bản ghi Bronze không thay đổi, tuyệt đối không có `event_id` trùng lặp:
   $$\text{COUNT}(*) == \text{COUNT}(\text{DISTINCT } event\_id)$$
4. [x] Đầy đủ trường truy vết nguồn gốc (Lineage): `source_hour`, `source_path`, `source_checksum`, `ingestion_run_id`, `ingested_at`.
5. [x] Toàn bộ mã nguồn vượt qua kiểm thử đơn vị (`pytest`: 12/12 passed) và quy chuẩn định dạng (`ruff`).

---

## 2. Kiến Trúc Dữ Liệu & Thiết Kế Bảng Iceberg

### 2.1. Sổ cái quản trị nạp dữ liệu (`iceberg.control.ingestion_runs`)
- **Khóa định danh nghiệp vụ:** `source_hour` (duy nhất cho mỗi giờ nguồn, ví dụ `2024-01-01-0`).
- **Schema:**
  - `source_hour`: STRING NOT NULL
  - `source_path`: STRING NOT NULL
  - `source_checksum`: STRING NOT NULL (SHA-256 của file nén gốc)
  - `run_id`: STRING NOT NULL (UUID của phiên chạy)
  - `status`: STRING NOT NULL (`BRONZE_PROCESSING`, `BRONZE_COMPLETE`, `FAILED`)
  - `raw_record_count`: BIGINT
  - `bronze_record_count`: BIGINT
  - `quarantine_record_count`: BIGINT
  - `started_at`: TIMESTAMP
  - `completed_at`: TIMESTAMP
  - `error_message`: STRING

### 2.2. Bảng Bronze chính (`iceberg.bronze.github_events`)
- **Phân vùng:** `hours(created_at)` (Iceberg hidden partition).
- **Khóa duy nhất:** `event_id` (String, Not Null).
- **Schema:**
  - `event_id`: STRING NOT NULL
  - `record_hash`: STRING NOT NULL (SHA-256 deterministic của raw line)
  - `event_type`: STRING
  - `actor_id`: BIGINT
  - `actor_login`: STRING
  - `repo_id`: BIGINT
  - `repo_name`: STRING
  - `created_at`: TIMESTAMP
  - `raw_payload`: STRING (Lưu nguyên bản JSON của event)
  - **Lineage columns:** `source_hour`, `source_path`, `source_checksum`, `ingestion_run_id`, `ingested_at`.

### 2.3. Bảng kiểm dịch (`iceberg.bronze.github_events_quarantine`)
- Chứa các bản ghi lỗi (null hoặc empty `event_id`), lưu kèm `quarantine_reason` và `quarantined_at`.

---

## 3. Bằng Chứng Thực Thi & Idempotency Replay (Execution Evidence)

### Lần 1: Thực thi Bronze Ingestion Job cho `2024-01-01-0`
```text
=== Starting Bronze Ingestion for source_hour: 2024-01-01-0 (Run ID: 520aed3c-ca51-472e-8fee-c1b1c66c2487) ===
Source Landing Path: s3a://lakehouse/landing/gharchive/year=2024/month=01/day=01/2024-01-01-0.json.gz
Source SHA-256 Checksum: 8ce8f20b1117d089fcf4785730c731ea7fe0ba85fffd3b4bd337b25df4f89c57
Acquiring ingestion lock in iceberg.control.ingestion_runs...
Reading raw data from s3a://lakehouse/landing/gharchive/year=2024/month=01/day=01/2024-01-01-0.json.gz...
Total raw lines read: 4
Merging staged events into iceberg.bronze.github_events...
=== Bronze Ingestion Completed Successfully for 2024-01-01-0 ===
Raw: 4 | Bronze: 4 | Quarantine: 0
```

### Lần 2: Replay lại cùng giờ nguồn `2024-01-01-0` (Kiểm thử Idempotent)
```text
=== Starting Bronze Ingestion for source_hour: 2024-01-01-0 (Run ID: 83baf3f2-81f2-4423-9256-c7277573f0e2) ===
Acquiring ingestion lock in iceberg.control.ingestion_runs...
Merging staged events into iceberg.bronze.github_events...
=== Bronze Ingestion Completed Successfully for 2024-01-01-0 ===
Raw: 4 | Bronze: 4 | Quarantine: 0
```

### Xác minh kết quả qua Trino SQL Engine
Truy vấn SQL thực tế được thực thi qua Trino client:
```sql
SELECT source_hour,
       COUNT(*) AS rows,
       COUNT(DISTINCT event_id) AS distinct_event_ids
FROM iceberg.bronze.github_events
GROUP BY source_hour;
```
**Kết quả Trino trả về:**
```python
[["2024-01-01-0", 4, 4]]
```
- Số dòng `rows = 4`.
- Số ID phân biệt `distinct_event_ids = 4`.
- **Tổng số dòng không tăng lên sau khi chạy lại lần 2** (chứng minh tính Idempotent 100%).

Kiểm tra sổ cái `iceberg.control.ingestion_runs`:
```python
[["2024-01-01-0", "BRONZE_COMPLETE", 4, 4, 0]]
```
- Chỉ tồn tại **duy nhất 1 record** cho `source_hour = '2024-01-01-0'`.
- Trạng thái được cập nhật thành công sang `BRONZE_COMPLETE`.

---

## 4. Kết Quả Kiểm Thử Toàn Diện (Test Suite)

```bash
$ pytest -v
============================= test session starts =============================
platform win32 -- Python 3.12.7, pytest-9.1.1, pluggy-1.7.0 -- .venv\Scripts\python.exe
rootdir: E:\GitHub-Events-Data-LakeHouse
configfile: pytest.ini
testpaths: tests
collected 12 items

tests/test_bronze_replay_idempotency.py::test_bronze_replay_and_uniqueness PASSED [  8%]
tests/test_path_utils_and_checksum.py::test_parse_source_hour_valid PASSED [ 16%]
tests/test_path_utils_and_checksum.py::test_parse_source_hour_invalid PASSED [ 25%]
tests/test_path_utils_and_checksum.py::test_get_landing_s3_key PASSED    [ 33%]
tests/test_path_utils_and_checksum.py::test_compute_sha256_deterministic PASSED [ 41%]
tests/test_path_utils_and_checksum.py::test_compute_string_sha256 PASSED [ 50%]
tests/test_path_utils_and_checksum.py::test_validate_gzipped_json_valid PASSED [ 58%]
tests/test_path_utils_and_checksum.py::test_validate_gzipped_json_corrupted PASSED [ 66%]
tests/test_quarantine_and_concurrency.py::test_concurrency_lock_logic PASSED [ 75%]
tests/test_quarantine_and_concurrency.py::test_checksum_mismatch_logic PASSED [ 83%]
tests/test_quarantine_and_concurrency.py::test_quarantine_identification_logic PASSED [ 91%]
tests/test_vertical_slice_trino.py::test_trino_query_proof_table PASSED  [100%]

============================= 12 passed in 30.42s =============================
```

### Quy chuẩn định dạng & Linting
```bash
$ ruff check .
All checks passed!

$ ruff format --check .
20 files already formatted
```

---

## 5. Kết Luận
Phase 1 đã hoàn thành **100% mục tiêu** đề ra, tạo nên một tầng Landing và Bronze chuẩn Enterprise Lakehouse với cơ chế kiểm soát chất lượng dữ liệu, bảo đảm tính toàn vẹn và Idempotency tuyệt đối trước khi tiến sang **Phase 2: Silver Layer & First Mart**.
