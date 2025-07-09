# local_crawler.py
"""Run the Tuchtrecht crawler locally without record limits."""

import os
import sys
from pathlib import Path
from datetime import datetime, timezone
import argparse
import jsonlines

ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from crawler.sru_client import get_records
from crawler.parser import parse_record
from crawler.scrubber import scrub_text

DATA_DIR = "data"
LAST_UPDATE_FILE = ".last_update"
BASE_QUERY = "c.product-area==tuchtrecht"
RECORDS_PER_SHARD = 350


def get_last_run_date() -> str | None:
    """Return the ISO timestamp of the last successful run if available."""
    if os.path.exists(LAST_UPDATE_FILE):
        with open(LAST_UPDATE_FILE, "r") as f:
            return f.read().strip()
    return None


def save_last_run_date() -> None:
    """Record the current timestamp so future runs fetch only new data."""
    with open(LAST_UPDATE_FILE, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch all available rulings from the Tuchtrecht SRU endpoint"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Ignore the last update timestamp and crawl the entire backlog",
    )
    parser.add_argument(
        "--output-dir",
        default=DATA_DIR,
        help="Directory where JSONL shards are stored",
    )
    parser.add_argument(
        "--no-scrub",
        action="store_true",
        help="Disable name scrubbing on the fetched content",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.output_dir

    if args.reset and os.path.exists(LAST_UPDATE_FILE):
        os.remove(LAST_UPDATE_FILE)

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        if os.path.exists(LAST_UPDATE_FILE) and not args.reset:
            print("Data directory missing. Removing stale last update timestamp.")
            os.remove(LAST_UPDATE_FILE)

    last_run_date = None if args.reset else get_last_run_date()

    if last_run_date:
        print(f"Fetching updates since: {last_run_date}")
    else:
        print("No previous run detected. Fetching full backlog.")

    records_iterator = get_records(BASE_QUERY, start_date=last_run_date)

    shard_index = 0
    records_in_current_shard = 0

    existing_shards = [
        f for f in os.listdir(data_dir) if f.startswith("tuchtrecht_shard_") and f.endswith(".jsonl")
    ]
    if existing_shards:
        existing_shards.sort()
        latest = existing_shards[-1]
        try:
            shard_index = int(latest.split("_")[-1].split(".")[0])
            with jsonlines.open(os.path.join(data_dir, latest), mode="r") as reader:
                for _ in reader:
                    records_in_current_shard += 1
            if records_in_current_shard >= RECORDS_PER_SHARD:
                shard_index += 1
                records_in_current_shard = 0
        except Exception as e:
            print(f"Could not inspect existing shard {latest}: {e}. Starting new shard.")
            shard_index += 1
            records_in_current_shard = 0

    output_file = os.path.join(data_dir, f"tuchtrecht_shard_{shard_index:03d}.jsonl")
    writer = jsonlines.open(output_file, mode="a")

    processed = 0
    for record in records_iterator:
        parsed = parse_record(record)
        if parsed:
            if not args.no_scrub:
                parsed["Content"] = scrub_text(parsed["Content"])
            writer.write(parsed)
            processed += 1
            records_in_current_shard += 1
            print(f"Saved record {processed}: {parsed['URL']}")

            if records_in_current_shard >= RECORDS_PER_SHARD:
                writer.close()
                shard_index += 1
                records_in_current_shard = 0
                output_file = os.path.join(data_dir, f"tuchtrecht_shard_{shard_index:03d}.jsonl")
                writer = jsonlines.open(output_file, mode="w")

    writer.close()
    print(f"Downloaded {processed} records in total.")

    if processed > 0 or not last_run_date:
        save_last_run_date()


if __name__ == "__main__":
    main()
