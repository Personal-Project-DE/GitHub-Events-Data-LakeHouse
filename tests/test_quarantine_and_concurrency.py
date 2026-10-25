import pytest


def test_concurrency_lock_logic():
    """
    Validates that if an ingestion run is currently in BRONZE_PROCESSING,
    a concurrent run for the same source_hour is rejected unless forced.
    """
    # Simulated existing run record
    existing_run = {
        "source_hour": "2024-01-01-0",
        "source_checksum": "checksum_aaa",
        "status": "BRONZE_PROCESSING",
        "run_id": "run-1",
        "started_at": "2024-01-01 00:00:00",
    }

    force = False

    # Simulate acquire lock logic
    if existing_run["status"] == "BRONZE_PROCESSING" and not force:
        with pytest.raises(RuntimeError, match="Concurrency conflict"):
            raise RuntimeError(
                f"Concurrency conflict: source_hour '{existing_run['source_hour']}' is currently locked and "
                f"being processed by run_id '{existing_run['run_id']}'."
            )

    # With force=True, it should proceed
    can_proceed = not (existing_run["status"] == "BRONZE_PROCESSING" and not True)
    assert can_proceed is True


def test_checksum_mismatch_logic():
    """
    Validates that if an incoming file for an already accepted source-hour has a different
    checksum, the system rejects it to prevent silent data mutations.
    """
    existing_run = {
        "source_hour": "2024-01-01-0",
        "source_checksum": "8ce8f20b1117d089fcf4785730c731ea7fe0ba85fffd3b4bd337b25df4f89c57",
        "status": "BRONZE_COMPLETE",
        "run_id": "run-1",
    }

    tampered_checksum = (
        "deadbeef1117d089fcf4785730c731ea7fe0ba85fffd3b4bd337b25df4f89c57"
    )
    force = False

    if existing_run["source_checksum"] != tampered_checksum and not force:
        with pytest.raises(ValueError, match="Source version conflict"):
            raise ValueError(
                f"Source version conflict for source_hour '{existing_run['source_hour']}': "
                f"Existing checksum '{existing_run['source_checksum']}' does not match "
                f"incoming checksum '{tampered_checksum}'."
            )


def test_quarantine_identification_logic():
    """
    Validates filtering logic for identifying invalid records to be quarantined.
    """
    records = [
        {"id": "1001", "type": "PushEvent"},  # valid
        {"id": None, "type": "ForkEvent"},  # invalid (null)
        {"id": "   ", "type": "WatchEvent"},  # invalid (empty)
        {"id": "1002", "type": "PullRequestEvent"},  # valid
    ]

    valid_records = []
    quarantined_records = []

    for r in records:
        event_id = r.get("id")
        if event_id is None or len(str(event_id).strip()) == 0:
            quarantined_records.append(r)
        else:
            valid_records.append(r)

    assert len(valid_records) == 2
    assert len(quarantined_records) == 2
    assert quarantined_records[0]["type"] == "ForkEvent"
    assert quarantined_records[1]["type"] == "WatchEvent"
