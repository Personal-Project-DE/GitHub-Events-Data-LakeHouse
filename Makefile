.PHONY: up down ps logs test download proof-spark proof-trino vertical-slice bronze-spark bronze-test lint format

# Start all Lakehouse services
up:
	docker compose up -d

# Stop Lakehouse services
down:
	docker compose down

# Check container status
ps:
	docker compose ps

# Follow container logs
logs:
	docker compose logs -f

# Download/Land a single source hour (e.g. source_hour=2024-01-01-0)
download:
	python spark/utils/downloader.py --source-hour 2024-01-01-0 --fixture tests/fixtures/landing/2024-01-01-0.json.gz

# Execute Phase 0 Spark vertical slice
proof-spark:
	docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
		--jars /opt/spark/work-dir/spark/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/spark/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-aws-bundle-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar \
		/opt/spark/work-dir/spark/jobs/proof_slice.py

# Query and verify Iceberg proof table through Trino
proof-trino:
	python tests/test_vertical_slice_trino.py

# Run complete Phase 0 Vertical Slice
vertical-slice: download proof-spark proof-trino

# Execute Phase 1 Bronze Ingestion
bronze-spark:
	docker exec lakehouse-spark-master /opt/spark/bin/spark-submit \
		--jars /opt/spark/work-dir/spark/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar,/opt/spark/work-dir/spark/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-aws-bundle-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar,/opt/spark/work-dir/spark/jars/org.wildfly.openssl_wildfly-openssl-1.0.7.Final.jar \
		/opt/spark/work-dir/spark/jobs/bronze_ingestion.py --source-hour 2024-01-01-0

# Query and verify Bronze replay idempotency through Trino
bronze-test:
	pytest -v tests/test_bronze_replay_idempotency.py

# Run Python unit tests
test:
	pytest -v

# Code quality checks
lint:
	ruff check .

format:
	ruff format .
