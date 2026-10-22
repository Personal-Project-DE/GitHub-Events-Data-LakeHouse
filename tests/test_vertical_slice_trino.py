import os
from pathlib import Path

from dotenv import load_dotenv

import trino

# Load .env
env_path = Path(__file__).resolve().parents[1] / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)


def test_trino_query_proof_table():
    trino_port = int(os.getenv("TRINO_PORT", "8082"))
    print(f"Connecting to Trino on localhost:{trino_port}...")

    conn = trino.dbapi.connect(
        host="localhost",
        port=trino_port,
        user="admin",
        catalog="iceberg",
        schema="demo",
    )
    cursor = conn.cursor()

    # Verify tables
    cursor.execute("SHOW TABLES FROM iceberg.demo")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"Discovered tables in 'iceberg.demo': {tables}")
    assert "proof_events" in tables, f"Expected proof_events in {tables}"

    # Query counts
    cursor.execute(
        "SELECT count(*), count(distinct event_id) FROM iceberg.demo.proof_events"
    )
    total_count, distinct_count = cursor.fetchone()
    print(
        f"Trino Query Result -> Total rows: {total_count}, Distinct event_ids: {distinct_count}"
    )
    assert total_count > 0, "Expected at least 1 row in proof_events"
    assert total_count == distinct_count, (
        "Expected no duplicate event_ids in proof_events"
    )

    # Query preview
    cursor.execute(
        "SELECT event_id, event_type, actor_login, repo_name, created_at FROM iceberg.demo.proof_events LIMIT 5"
    )
    rows = cursor.fetchall()
    print("\nPreview 5 rows from Trino:")
    for row in rows:
        print(row)

    print(
        "\n--- Verification Passed: Trino successfully queried Iceberg proof table! ---"
    )


if __name__ == "__main__":
    test_trino_query_proof_table()
