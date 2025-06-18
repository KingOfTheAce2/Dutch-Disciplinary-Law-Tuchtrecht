#!/usr/bin/env python3
"""
Crawl every disciplinary ruling (≈ 45 k) from https://tuchtrecht.overheid.nl
and produce a newline-delimited JSONL with fields:
  - url: canonical ruling URL
  - content: all visible textual content
  - source: "Tuchtrecht"

Supports:
* Resumable runs via visited.txt
* Throttling (1–2 s) and retries on HTTP errors
* Periodic flush to tuchtrecht.jsonl
* HF Hub upload when HF_TOKEN and HF_REPO are set
"""

import argparse
import json
import math
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Generator, Iterable
from urllib.parse import urljoin

import bs4
import requests
from requests.adapters import HTTPAdapter, Retry
from tqdm import tqdm
from datasets import Dataset
from huggingface_hub import HfApi, login

# ---------------------------------------------------------------------------- #
# Configuration
# ---------------------------------------------------------------------------- #
ITEMS_PER_PAGE = 50
BASE_SEARCH   = "https://tuchtrecht.overheid.nl/zoeken/resultaat"
BASE_TUCH     = "https://tuchtrecht.overheid.nl"
OUT_FILE      = Path("tuchtrecht.jsonl")
VISITED_FILE  = Path("visited.txt")
SOURCE_NAME   = "Tuchtrecht"
# HF upload settings (via GitHub Secrets)
HF_TOKEN = os.getenv("HF_TOKEN")
HF_REPO  = os.getenv("HF_REPO", "YOURUSER/tuchtrecht-uitspraken")
# ---------------------------------------------------------------------------- #


def build_session() -> requests.Session:
    """Create a requests.Session with retry/backoff."""
    sess = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    sess.mount("https://", HTTPAdapter(max_retries=retries))
    sess.headers.update({
        "User-Agent": "tuchtrecht-crawler (+https://github.com/your/repo; you@example.com)"
    })
    return sess


def list_case_urls(session: requests.Session) -> Generator[str, None, None]:
    """
    Walk through the paginated HTML search results and yield each ruling URL.
    """
    # Fetch first page to determine total number of results
    params0 = {"itemsPerPage": ITEMS_PER_PAGE, "page": 0}
    resp = session.get(BASE_SEARCH, params=params0, timeout=30)
    resp.raise_for_status()
    soup = bs4.BeautifulSoup(resp.text, "lxml")

    stats = soup.select_one("div.search__stats")
    total = int(re.search(r"van de\s+(\d+)", stats.text).group(1))
    pages = math.ceil(total / ITEMS_PER_PAGE)

    for page in range(pages):
        params = {"itemsPerPage": ITEMS_PER_PAGE, "page": page}
        resp = session.get(BASE_SEARCH, params=params, timeout=30)
        resp.raise_for_status()
        soup = bs4.BeautifulSoup(resp.text, "lxml")

        # Each result link has class "uitspraak__link"
        for a in soup.select("a.uitspraak__link"):
            href = a.get("href")
            if href:
                yield urljoin(BASE_TUCH, href)

        time.sleep(random.uniform(1.0, 2.0))


def visible_text(html: str) -> str:
    """
    Extract and normalize all visible text from the main content area.
    """
    soup = bs4.BeautifulSoup(html, "lxml")
    container = soup.find("main") or soup.body
    text = container.get_text(separator="\n", strip=True)
    return " ".join(text.split())


def crawl_one(url: str, session: requests.Session) -> dict | None:
    """
    Fetch a single ruling page, extract its text, and return the record dict.
    """
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        content = visible_text(resp.text)
        if len(content) < 200:
            # skip pages with too little content
            return None
        return {"url": url, "content": content, "source": SOURCE_NAME}
    except Exception as e:
        print(f"[WARN] Failed to fetch {url}: {e}", file=sys.stderr)
        return None


def load_visited() -> set[str]:
    if not VISITED_FILE.exists():
        return set()
    return set(VISITED_FILE.read_text(encoding="utf-8").splitlines())


def append_visited(urls: Iterable[str]) -> None:
    with VISITED_FILE.open("a", encoding="utf-8") as f:
        for u in urls:
            f.write(u + "\n")


def append_jsonl(rows: Iterable[dict]) -> None:
    with OUT_FILE.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def push_to_hf() -> None:
    if not HF_TOKEN:
        print("HF_TOKEN not set; skipping Hugging Face upload.")
        return
    login(token=HF_TOKEN)
    api = HfApi()
    api.create_repo(repo_id=HF_REPO, repo_type="dataset", exist_ok=True)
    api.upload_file(
        repo_id=HF_REPO,
        path_or_fileobj=str(OUT_FILE),
        path_in_repo=OUT_FILE.name,
        repo_type="dataset",
    )
    print(f"✅ Dataset pushed to Hugging Face: {HF_REPO}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--hard-reset", action="store_true",
        help="ignore existing visited.txt and start fresh"
    )
    args = parser.parse_args()

    visited = set() if args.hard_reset else load_visited()
    new_rows: list[dict] = []
    new_visited: list[str] = []

    session = build_session()

    for page_url in tqdm(list_case_urls(session), desc="Crawling rulings"):
        if page_url in visited:
            continue

        rec = crawl_one(page_url, session)
        if rec:
            new_rows.append(rec)
            new_visited.append(page_url)

        # flush every 100 new items
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

    print(f"✅ Completed crawl; total visited: {len(visited) + len(new_visited)}")

    # push to HF
    push_to_hf()


if __name__ == "__main__":
    main()
