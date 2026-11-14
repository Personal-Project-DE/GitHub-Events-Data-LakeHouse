# Disaster Recovery & Rollback Guide

- **Dự án:** GitHub Events Data Lakehouse
- **Hệ thống áp dụng:** Apache Iceberg 1.6.1 + MinIO S3 + Ingestion Ledger

---

## 1. Khả Năng Time Travel Trong Apache Iceberg

Nhờ kiến trúc metadata phân tầng (Snapshots, Manifest Lists, Manifest Files), mọi thao tác ghi (Append, Merge, Overwrite, Compaction) đều sinh ra một snapshot bất biến (immutable snapshot) có gắn ID và nhãn thời gian (Timestamp).

### 1.1 Xem lịch sử Snapshots của một bảng qua Trino
```sql
SELECT 
    snapshot_id, 
    committed_at, 
    operation, 
    summary['added-records-size'] AS added_bytes,
    summary['total-records'] AS total_records
FROM iceberg.gold."mart_trending_repos_daily$snapshots"
ORDER BY committed_at DESC;
```

### 1.2 Truy vấn dữ liệu trong quá khứ (Time Travel Query)
Truy vấn bảng tại một thời điểm lịch sử cụ thể:
```sql
-- Truy vấn theo mốc thời gian (System Time)
SELECT COUNT(*) 
FROM iceberg.gold.mart_trending_repos_daily 
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-11-12 10:00:00 UTC';

-- Hoặc truy vấn theo Snapshot ID cụ thể
SELECT COUNT(*) 
FROM iceberg.gold.mart_trending_repos_daily 
FOR SYSTEM_VERSION AS OF 8472918392019482012;
```

---

## 2. Quy Trình Rollback Khôi Phục Thảm Họa (Disaster Rollback)

Trường hợp một tác vụ nạp bị lỗi nặng hoặc dữ liệu bị biến đổi sai sót, hệ thống hỗ trợ Rollback trạng thái bảng về một Snapshot an toàn trước đó mà không cần phục hồi từ backup vật lý:

### 2.1 Thực hiện Rollback bằng Spark SQL Procedure
Khởi chạy phiên Spark và gọi procedure:
```sql
-- Khôi phục bảng về Snapshot an toàn đã xác định
CALL iceberg.system.rollback_to_snapshot(
    table => 'iceberg.gold.mart_trending_repos_daily',
    snapshot_id => 8472918392019482012
);
```

### 2.2 Hoặc Rollback về thời điểm trước khi có sự cố
```sql
CALL iceberg.system.rollback_to_timestamp(
    table => 'iceberg.gold.mart_trending_repos_daily',
    timestamp => TIMESTAMP '2026-11-12 09:30:00'
);
```

---

## 3. Quy Trình Chạy Lại Idempotent An Toàn Với Cờ `--force`

Hệ thống được thiết kế theo hợp đồng Idempotent: chạy lại một giờ nguồn bất kỳ sẽ không làm tăng số dòng hay tạo khóa trùng lặp.

Nếu một tiến trình gặp sự cố giữa chừng:
1. Bản ghi trạng thái trong `iceberg.control.ingestion_runs` sẽ lưu trạng thái `failed` hoặc đang giữ `run_lock`.
2. Để chạy lại an toàn, sử dụng tham số `--force`:
   ```bash
   python spark/utils/downloader.py --source-hour 2024-01-01-0 --force
   ```
3. Cờ `--force` sẽ:
   - Ghi đè khóa `run_lock` cũ bị treo (stale lock).
   - Kiểm tra và chấp nhận cập nhật file Landing nếu có bản vá dữ liệu.
   - Kích hoạt lại câu lệnh `MERGE INTO` ở Bronze/Silver (ghi đè theo `event_id` hoặc dimension keys).
   - Tái tính toán và ghi đè nguyên tử phân vùng ngày bị ảnh hưởng ở Gold (`REPLACE PARTITION`).
