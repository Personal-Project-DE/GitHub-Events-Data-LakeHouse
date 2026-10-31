import os
import socket
from pathlib import Path

import pytest
from dotenv import load_dotenv

import trino


def is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# Load .env
env_path = Path(__file__).resolve().parents[1] / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)


def test_trino_query_proof_table():
    trino_port = int(os.getenv("TRINO_PORT", "8082"))
    if not is_port_open("localhost", trino_port):
        pytest.skip(
            f"Trino service is not accessible on localhost:{trino_port}; skipping live integration test."
        )
    try:
        conn = trino.dbapi.connect(
            host="localhost",
            port=trino_port,
            user="admin",
            catalog="iceberg",
            schema="demo",
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchall()
    except (trino.exceptions.TrinoConnectionError, OSError) as exc:
        pytest.skip(
            f"Trino service is not accessible ({exc}); skipping live integration test."
        )

    print(f"Connecting to Trino on localhost:{trino_port}...")

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
    assert len(rows) > 0, "Expected at least 1 row from preview query"


if __name__ == "__main__":
    try:
        test_trino_query_proof_table()
    except pytest.skip.Exception as exc:
        print(f"[SKIPPED] {exc}")
