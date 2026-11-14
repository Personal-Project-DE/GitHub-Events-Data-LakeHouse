import trino.dbapi

conn = trino.dbapi.connect(host="localhost", port=8082, user="admin", catalog="iceberg")
cur = conn.cursor()

print("--- INGESTION RUNS ---")
cur.execute(
    "SELECT source_hour, status, bronze_record_count, started_at, completed_at FROM iceberg.control.ingestion_runs ORDER BY source_hour ASC"
)
for r in cur.fetchall():
    print(r)

print("\n--- BRONZE BY HOUR ---")
cur.execute(
    "SELECT source_hour, count(*) FROM iceberg.bronze.github_events GROUP BY source_hour ORDER BY source_hour ASC"
)
for r in cur.fetchall():
    print(r)

print("\n--- GOLD TRENDING DATES ---")
cur.execute(
    "SELECT metric_date, count(*) FROM iceberg.gold.mart_trending_repos_daily GROUP BY metric_date ORDER BY metric_date ASC"
)
for r in cur.fetchall():
    print(r)
