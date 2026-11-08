from __future__ import annotations

import argparse
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


def generate_source_hours_range(start_str: str, end_str: str) -> list[str]:
    """
    Generates a list of source_hour strings from start_str to end_str (inclusive).
    Supports format YYYY-MM-DD-H or YYYY-MM-DD-HH.
    Outputs canonical format YYYY-MM-DD-H (e.g. 2024-01-01-0).
    """

    def parse_hour_key(key: str) -> datetime:
        parts = key.rsplit("-", 1)
        if len(parts) != 2:
            raise ValueError(
                f"Invalid source_hour format: {key}. Expected YYYY-MM-DD-H"
            )
        date_part, hour_part = parts
        try:
            dt = datetime.strptime(date_part, "%Y-%m-%d")
            return dt + timedelta(hours=int(hour_part))
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid source_hour format: {key}. Expected YYYY-MM-DD-H"
            ) from e

    start_dt = parse_hour_key(start_str)
    end_dt = parse_hour_key(end_str)

    if start_dt > end_dt:
        raise ValueError(
            f"Start datetime ({start_dt}) cannot be after end datetime ({end_dt})"
        )

    results: list[str] = []
    curr = start_dt
    while curr <= end_dt:
        results.append(f"{curr.strftime('%Y-%m-%d')}-{curr.hour}")
        curr += timedelta(hours=1)

    return results


def run_controlled_backfill(
    source_hours: list[str],
    force: bool = False,
    fixture_dir: Path | None = None,
) -> dict[str, list[str]]:
    """
    Executes a controlled sequential backfill through all Medallion layers:
    download -> bronze -> silver -> enrich -> gold -> validate
    """
    from spark.jobs.bronze_ingestion import run_bronze_ingestion
    from spark.jobs.gold_aggregation import run_gold_aggregation
    from spark.jobs.quality_check import run_all_quality_checks
    from spark.jobs.silver_transform import run_silver_transformation
    from spark.utils.downloader import download_and_land_source_hour
    from spark.utils.spark_session import get_spark_session

    spark = get_spark_session("ControlledBackfillRunner")
    successes: list[str] = []
    failures: list[str] = []

    print(f"=== Starting Controlled Backfill for {len(source_hours)} source hours ===")
    print(f"Hours: {source_hours}")
    print(f"Forced Replay: {force}")

    try:
        for idx, hour in enumerate(source_hours, 1):
            run_id = str(uuid.uuid4())
            target_date = hour.rsplit("-", 1)[0]
            print(
                f"\n--- [{idx}/{len(source_hours)}] Processing {hour} (Run ID: {run_id}) ---"
            )

            try:
                # 1. Landing / Download
                fixture_file = None
                if fixture_dir:
                    candidate = fixture_dir / f"{hour}.json.gz"
                    if candidate.exists():
                        fixture_file = candidate

                download_and_land_source_hour(
                    source_hour=hour,
                    fixture_path=fixture_file,
                    upload_to_s3=True,
                )

                # 2. Bronze Ingestion
                run_bronze_ingestion(
                    spark=spark,
                    source_hour=hour,
                    run_id=run_id,
                    force=force,
                )

                # 3. Silver Transformation + Enrichment
                run_silver_transformation(
                    spark=spark,
                    source_hour=hour,
                    run_id=run_id,
                    force=force,
                )

                # 4. Gold Aggregation (rebuilds target date daily grain)
                run_gold_aggregation(
                    spark=spark,
                    target_date=target_date,
                    source_hour=hour,
                    run_id=run_id,
                )

                # 5. Quality Gate Validation
                run_all_quality_checks(
                    spark=spark,
                    source_hour=hour,
                    target_date=target_date,
                )

                successes.append(hour)
                print(f"--- Completed successfully: {hour} ---")

            except Exception as e:  # noqa: BLE001
                print(
                    f"ERROR: Failed processing source_hour {hour}: {e}", file=sys.stderr
                )
                failures.append(hour)
                break  # Stop sequential backfill on first error to prevent state corruption

    finally:
        spark.stop()

    print("\n=== Backfill Summary ===")
    print(
        f"Total: {len(source_hours)}, Succeeded: {len(successes)}, Failed: {len(failures)}"
    )
    return {"succeeded": successes, "failed": failures}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manual controlled backfill runner for GitHub Events Lakehouse."
    )
    parser.add_argument("--start", help="Start UTC hour (e.g. 2024-01-01-0)")
    parser.add_argument("--end", help="End UTC hour (e.g. 2024-01-01-23)")
    parser.add_argument("--date", help="Target full 24-hour UTC date (e.g. 2024-01-01)")
    parser.add_argument(
        "--force", action="store_true", help="Force replay of already completed hours"
    )
    parser.add_argument(
        "--fixture-dir",
        default=None,
        help="Optional local directory containing test fixtures",
    )
    args = parser.parse_args()

    if args.date:
        hours = [f"{args.date}-{h}" for h in range(24)]
    elif args.start and args.end:
        hours = generate_source_hours_range(args.start, args.end)
    else:
        parser.error("Either --date OR (--start AND --end) must be provided.")

    fixture_dir_path = Path(args.fixture_dir) if args.fixture_dir else None
    result = run_controlled_backfill(
        hours, force=args.force, fixture_dir=fixture_dir_path
    )

    if result["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
