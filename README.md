# GitHub Events Data Lakehouse

An end-to-end local Data Lakehouse processing public GitHub Archive events using **PySpark, Apache Iceberg, MinIO, Trino, and Apache Airflow**.

---

## Architecture & Tech Stack

```text
       +------------------------------------+
       |  GH Archive (data.gharchive.org)   |
       +-----------------+------------------+
                         | HTTP GET (.json.gz)
                         v
       +------------------------------------+
       |       MinIO Landing Storage        |
       |  (s3a://lakehouse/landing/...)     |
       +-----------------+------------------+
                         |
                         v
       +------------------------------------+
       |         PySpark Processing         |
       |  - Bronze: Deduplicated Events     |
       |  - Silver: Dims & Facts (SCD1)     |
       |  - Gold: Daily Business Marts      |
       +--------+------------------+--------+
                |                  |
      Metadata  |                  | Parquet Data
                v                  v
+-----------------------+  +------------------------+
| Iceberg REST Catalog  |  |   MinIO Warehouse      |
| (localhost:8181)      |  | (s3a://lakehouse/wh)   |
+-----------+-----------+  +-----------+------------+
            ^                          ^
            |  Metadata & Data Files   |
            +-------------+------------+
                          |
                          v
            +---------------------------+
            |        Trino SQL          |
            |     (localhost:8082)      |
            +-------------+-------------+
                          |
                          v
            +---------------------------+
            |         Metabase          |
            |     (localhost:3000)      |
            +---------------------------+
```

### Pinned Versions

| Component | Pinned Version / Image | Role |
|---|---|---|
| **Object Storage** | `minio/minio:latest` | S3-compatible Landing & Iceberg Warehouse |
| **Catalog** | `tabulario/iceberg-rest:0.7.0` | Apache Iceberg REST Catalog |
| **Processing Engine** | `bitnami/spark:3.5.3` | PySpark 3.5.3 + Iceberg Spark Runtime 1.6.1 |
| **Query Engine** | `trinodb/trino:468` | Distributed SQL Query Engine |
| **Orchestration** | `apache/airflow:2.10.5-python3.10` | Pipeline Scheduling & Quality Gates |
| **BI** | `metabase/metabase:latest` | Gold Marts Dashboards |

---

## Quickstart (Phase 0: Vertical Slice)

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.12+

### 2. Environment Setup
```bash
# Clone and enter repo
cd GitHub-Events-Data-LakeHouse

# Setup virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Copy environment variables
cp .env.example .env
```

### 3. Start Core Services
```bash
docker compose up -d minio mc iceberg-rest trino spark-master spark-worker
```

### 4. Run Vertical Slice Verification
```bash
# 1. Download & upload 1 source hour to MinIO Landing
python spark/utils/downloader.py --source-hour 2024-01-01-0 --fixture tests/fixtures/landing/2024-01-01-0.json.gz

# 2. Write Iceberg proof table with Spark
make proof-spark

# 3. Query Iceberg proof table through Trino
python tests/test_vertical_slice_trino.py
```
