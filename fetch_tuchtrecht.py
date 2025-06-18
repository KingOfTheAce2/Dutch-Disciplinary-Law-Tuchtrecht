#!/usr/bin/env python3
"""
Crawl every public disciplinary ruling (±45 k) on https://tuchtrecht.overheid.nl
and create a newline–delimited JSON file with the schema

    {"url": <canonical_page>, "content": <all_visible_text>, "source": "Tuchtrecht"}

The script is **idempotent** and resumable:
* `visited.txt` – one URL per line, acts as the checkpoint.
* `tuchtrecht.jsonl` – the resulting dataset (append‑only).

It throttles requests (1‑2 s) and retries automatically.
After a successful run it pushes the dataset to the Hugging Face Hub.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Generator, Iterable

import bs4
import requests
from datasets import Dataset
from huggingface_hub import HfApi, login
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm

# ---------------------------------------------------------------------------#
# Configuration – adjust only if you really have to
# ---------------------------------------------------------------------------#
BATCH = 100                       # records returned per SRU call (max 100)
BASE_SRU = (
    "https://repository.overheid.nl/sru/Search?"
    "x-connection=tuchtrecht&recordPacking=json"
    "&maximumRecords={batch}&startRecord={start}"
)
CANONICAL = "https://tuchtrecht.overheid.nl/{eid}"          # eid = ECLI with '_'s
OUT_FILE = Path("tuchtrecht.jsonl")
VISITED_FILE = Path("visited.txt")
SOURCE = "Tuchtrecht"

HF_REPO = os.getenv("HF_REPO", "YOURUSER/tuchtrecht-uitspraken")
HF_TOKEN = os.getenv("HF_TOKEN")          # passed via GitHub Secrets
# ---------------------------------------------------------------------------#


def sru_records(session: requests.Session) -> Generator[dict, None, None]:
    """Generator that yields every SRU record as JSON."""
    start = 1
    with tqdm(desc="Fetching SRU batches") as bar:
        while True:
            url = BASE_SRU.format(batch=BATCH, start=start)
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()["searchRetrieveResponse"]

            records = data["records"]["record"]
            if isinstance(records, dict):      # single item edge‑case
                records = [records]

            if not records:
                break

            for rec in records:
                yield rec

            nxt = data.get("nextRecordPosition")
            if not nxt or int(nxt) <= start:
                break
            bar.update(len(records))
            start = int(nxt)


def record_to_page_url(record: dict) -> str:
    """Convert SRU record to canonical ruling URL."""
    ecli: str = record["recordData"]["gzd"]["originalData"]["meta"]["owmskern"]["identifier"]
    ecli_url = ecli.replace(":", "_")        # ECLI:NL:XXX -> ECLI_NL_XXX
    return CANONICAL.format(eid=ecli_url)


def visible_text(html: str) -> str:
    """Extract all visible text from a ruling page."""
    soup = bs4.BeautifulSoup(html, "lxml")
    # The main content lives in <main>, but fall back to body
    container = soup.find("main") or soup.body
    text = container.get_text(separator="\n", strip=True)
    return " ".join(text.split())            # normalise whitespace


def crawl_one(url: str, session: requests.Session) -> dict | None:
    """Download & parse a single ruling. Returns ready‑to‑dump dict or None."""
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        content = visible_text(resp.text)
        if len(content) < 200:               # rudimentary quality gate
            return None
        return {"url": url, "content": content, "source": SOURCE}
    except Exception as exc:                 # noqa: BLE001
        print(f"[WARN] {url} failed: {exc}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------#
# I/O helpers
# ---------------------------------------------------------------------------#
def load_visited() -> set[str]:
    if not VISITED_FILE.exists():
        return set()
    return set(VISITED_FILE.read_text(encoding="utf-8").splitlines())


def append_visited(urls: Iterable[str]) -> None:
    with VISITED_FILE.open("a", encoding="utf-8") as f:
        for u in urls:
            f.write(f"{u}\n")


def append_jsonl(rows: Iterable[dict]) -> None:
    with OUT_FILE.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------#
def build_session() -> requests.Session:
    """Shared HTTP session with retries & politeness."""
    sess = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    sess.headers.update(
        {
            "User-Agent": (
                "tuchtrecht-crawler (+https://github.com/your/repo;"
                " contact: you@example.com)"
            )
        }
    )
    return sess


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hard-reset", action="store_true",
                        help="ignore existing visited file")
    args = parser.parse_args()

    visited = set() if args.hard_reset else load_visited()
    new_visited: list[str] = []
    new_rows: list[dict] = []

    session = build_session()

    for rec in sru_records(session):
        page = record_to_page_url(rec)
        if page in visited:
            continue

        row = crawl_one(page, session)
        if row:
            new_rows.append(row)
            new_visited.append(page)

        # polite crawl delay
        time.sleep(random.uniform(1.0, 2.0))

        # flush periodically (every 100)
        if len(new_rows) >= 100:
            append_jsonl(new_rows)
            append_visited(new_visited)
            visited.update(new_visited)
            new_rows.clear()
            new_visited.clear()

    # final flush
    if new_rows:
        append_jsonl(new_rows)
        append_visited(new_visited)

    print(f"Collected {len(visited) + len(new_visited)} records so far.")

    # Push to HF if token is available
    if HF_TOKEN:
        login(token=HF_TOKEN)
        api = HfApi()
        api.create_repo(repo_id=HF_REPO, repo_type="dataset", exist_ok=True)
        api.upload_file(
            repo_id=HF_REPO,
            path_or_fileobj=str(OUT_FILE),
            path_in_repo="tuchtrecht.jsonl",
            repo_type="dataset",
        )
        print("Dataset pushed to the Hub.")
    else:
        print("HF_TOKEN not set – skipping Hub upload.")


if __name__ == "__main__":
    main()
