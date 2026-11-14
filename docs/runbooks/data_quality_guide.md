# Data Quality & Anomaly Detection Guide

- **Dự án:** GitHub Events Data Lakehouse
- **Mục tiêu:** Đảm bảo độ tin cậy tuyệt đối của dữ liệu từ Landing đến các Gold Analytical Marts.

---

## 1. Hệ Thống Quality Gates Đa Tầng (Medallion Quality Framework)

Mỗi tầng trong kiến trúc Medallion đóng vai trò là một chốt chặn kiểm soát chất lượng (Quality Gate). Nếu một chốt chặn phát hiện vi phạm, tiến trình ETL sẽ bị ngắt và ghi nhận trạng thái lỗi trong `iceberg.control.ingestion_runs`.

```text
Landing Object ──▶ Bronze Gate ──▶ Silver Gate ──▶ Gold Gate ──▶ Anomaly Gate ──▶ Audit Passed
   (Checksum,       (Zero Null,    (PK Uniqueness,  (Grain Integrity, (Volume Deviation,
   Integrity)       No Duplicate)   Foreign Keys)   Reconciliation)   Spike/Drop Alerts)
```

---

## 2. Chi Tiết Các Tiêu Chí Kiểm Tra (Gate Specifications)

### 2.1 Landing Quality Gate
- **Kiểm tra file tồn tại:** File nén `.json.gz` phải tồn tại tại đường dẫn quy chuẩn `s3a://lakehouse/landing/gharchive/year=YYYY/month=MM/day=DD/YYYY-MM-DD-H.json.gz`.
- **Kiểm tra dung lượng:** Dung lượng file phải $> 0$ bytes (chống file rỗng do lỗi mạng tải về từ gharchive.org).
- **Tính toàn vẹn Checksum:** Giá trị băm SHA-256 của file được đối soát với bản ghi đăng ký trong `iceberg.control.ingestion_runs`. Nếu phát hiện khác biệt checksum mà không có flag `--force`, tiến trình sẽ dừng ngay lập tức.

### 2.2 Bronze Quality Gate
- **Zero Null Primary Key:** Cột `event_id` không được phép chứa giá trị `NULL`.
- **Uniqueness Check:** Đảm bảo $\text{COUNT}(*) = \text{COUNT}(\text{DISTINCT } event_id)$ cho từng `source_hour`.
- **Deduplication:** Các sự kiện trùng `event_id` trong cùng một lô nạp sẽ được loại bỏ thông qua deduplication logic trước khi gọi `MERGE INTO`.

### 2.3 Silver Quality Gate
- **Tính duy nhất của Dimension Keys:**
  - `silver.dim_users`: Khóa `user_id` duy nhất 100%.
  - `silver.dim_repositories`: Khóa `repo_id` duy nhất 100%.
- **Tính duy nhất của Fact Event IDs:**
  - `silver.fact_commits`, `silver.fact_pull_requests`, `silver.fact_engagements` không chứa bất kỳ `event_id` trùng lặp nào.
- **Ràng buộc toàn vẹn:** Các trường bắt buộc như `created_at`, `actor_login`, `repo_name` không được mang giá trị rỗng.

### 2.4 Gold Quality Gate & Reconciliation
- **Grain Integrity:**
  - `mart_trending_repos_daily`: Duy nhất theo `(metric_date, repo_id)`.
  - `mart_language_trends_daily`: Duy nhất theo `(metric_date, primary_language)`.
  - `mart_developer_work_patterns_daily`: Duy nhất theo `(metric_date, day_of_week, hour_of_day)`.
- **Đối soát số liệu (Reconciliation):**
  - Tổng số ngôi sao mới (`new_stars_count`) trên bảng `mart_trending_repos_daily` phải khớp $100\%$ với số lượng sự kiện Star trong `silver.fact_engagements` cùng ngày.

### 2.5 Phát Hiện Biến Động Khối Lượng Dị Thường (Volume Anomaly Gate)
- So sánh số lượng sự kiện của giờ hiện tại với trung bình di động của các giờ trước đó.
- Cảnh báo khi khối lượng sự kiện giảm đột ngột $> 80\%$ (nguy cơ mất dữ liệu mạng) hoặc tăng vọt $> 300\%$ (nguy cơ sự kiện spam hoặc bot flood).

---

## 3. Quản Lý & Xử Lý Bản Ghi Cô Lập (Quarantine Management)

Khi phát hiện dữ liệu lỗi (thiếu `event_id`, JSON hỏng, sai định dạng schema):
1. **Không âm thầm vứt bỏ (No Silent Dropping):** Bản ghi hỏng được chuyển hướng ghi vào bảng `iceberg.quarantine.invalid_events`.
2. **Lưu vết nguyên nhân:** Bảng Quarantine lưu kèm:
   - `raw_payload`: Toàn bộ nội dung chuỗi JSON gốc.
   - `quarantine_reason`: Lý do chi tiết (ví dụ: `MISSING_EVENT_ID`, `JSON_PARSE_ERROR`).
   - `source_hour` và `quarantined_at`.
3. **Quy trình xử lý:**
   - Kỹ sư dữ liệu truy vấn bảng `iceberg.quarantine.invalid_events` qua Trino để đánh giá tỷ lệ lỗi.
   - Nếu tỷ lệ lỗi dưới ngưỡng cho phép ($< 0.01\%$), tiến trình chính vẫn tiếp tục hoàn thành.
