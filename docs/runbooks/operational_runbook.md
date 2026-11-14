# Lakehouse Operational Runbook

- **Dự án:** GitHub Events Data Lakehouse
- **Hệ thống điều hành:** Docker Compose, Apache Airflow 3.3.2, Apache Iceberg 1.6.1, PySpark 3.5.3, Trino 468, Metabase v0.50.28

---

## 1. Khởi Động & Dừng Hệ Thống

### 1.1 Khởi động toàn bộ cụm Lakehouse
Khởi chạy tất cả các dịch vụ nền tảng (MinIO, Iceberg REST, Spark Master/Worker, Trino, Postgres, Airflow, Metabase):
```bash
# Sử dụng Docker Compose trực tiếp
docker compose up -d

# Hoặc sử dụng Makefile shortcut
make up
```

### 1.2 Kiểm tra trạng thái sức khỏe (Health Check)
```bash
docker compose ps
```
Bảng trạng thái tiêu chuẩn:
- `lakehouse-minio`: healthy (port 9010 / console 9011)
- `lakehouse-iceberg-rest`: running (port 8181)
- `lakehouse-spark-master`: running (port 7077 / UI 8088)
- `lakehouse-spark-worker`: running
- `lakehouse-trino`: running (port 8082)
- `lakehouse-postgres`: healthy (port 5432)
- `lakehouse-airflow-webserver`: running (port 8085)
- `lakehouse-airflow-scheduler`: running
- `lakehouse-metabase`: running (port 3000)

### 1.3 Dừng hệ thống an toàn
```bash
docker compose down
# Hoặc: make down
```

---

## 2. Vận Hành Pipeline Nạp Dữ Liệu (Ingestion & ETL)

### 2.1 Chạy pipeline điều phối tự động qua Airflow
- Truy cập giao diện Airflow Web UI: `http://localhost:8085` (User: `admin`, Password: `admin123`).
- DAG: `github_events_lakehouse_pipeline`.
- Chuỗi thực thi nhiệm vụ:
  ```text
  resolve_params -> download_and_land -> bronze_ingestion -> silver_transformation -> gold_aggregation -> quality_gates_validation
  ```

### 2.2 Kích hoạt nạp thủ công một giờ nguồn (Ad-hoc Single Hour)
Khi cần nạp kiểm thử hoặc xử lý một giờ cụ thể mà không qua Airflow UI:
```bash
# 1. Tải và ghi vào Landing (MinIO S3)
python spark/utils/downloader.py --source-hour 2024-01-01-0

# 2. Thực thi nạp Bronze
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/work-dir/spark/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/spark/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-aws-bundle-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar \
  /opt/spark/work-dir/spark/jobs/bronze_ingestion.py --source-hour 2024-01-01-0

# 3. Thực thi biến đổi Silver & Gold
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/work-dir/spark/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/spark/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-aws-bundle-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar \
  /opt/spark/work-dir/spark/jobs/silver_transform.py --source-hour 2024-01-01-0

docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/work-dir/spark/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/spark/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-aws-bundle-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar \
  /opt/spark/work-dir/spark/jobs/gold_aggregation.py --metric-date 2024-01-01
```

### 2.3 Chạy Backfill nhiều giờ liên tục
Sử dụng công cụ `backfill_runner.py` để nạp dữ liệu lịch sử theo dải thời gian:
```bash
# Nạp toàn bộ 24 giờ của một ngày
python spark/utils/backfill_runner.py --date 2024-01-01

# Nạp một dải thời gian tùy chọn
python spark/utils/backfill_runner.py --start 2024-01-01-0 --end 2024-01-02-23 --concurrency 1
```

---

## 3. Vận Hành Bảo Trì Bảng Iceberg (Compaction & Optimization)

Chạy định kỳ (hàng ngày hoặc sau mỗi đợt backfill lớn) để khắc phục tình trạng file nhỏ (Small Files Problem):

### 3.1 Chạy bảo trì toàn bộ các tầng (Bronze, Silver, Gold)
```bash
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/work-dir/spark/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/spark/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-aws-bundle-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar \
  /opt/spark/work-dir/spark/jobs/iceberg_maintenance.py --all-tables --target-file-size-mb 128 --older-than-days 3
```

### 3.2 Chạy bảo trì riêng cho một bảng cụ thể
```bash
docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
  --jars /opt/spark/work-dir/spark/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/spark/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-aws-bundle-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar \
  /opt/spark/work-dir/spark/jobs/iceberg_maintenance.py --table iceberg.gold.mart_trending_repos_daily --strategy sort
```

---

## 4. Đo Lường Benchmark Hiệu Năng
Thực thi kiểm tra độ trễ truy vấn Trino trước và sau khi bảo trì:
```bash
python spark/jobs/benchmark_compaction.py --phase both --repetitions 5
```
Kết quả thô sẽ được lưu tại `docs/benchmark/benchmark_results.csv` và báo cáo tổng hợp tại `docs/benchmark_report.md`.
