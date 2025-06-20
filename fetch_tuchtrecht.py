#!/usr/bin/env python3
"""
Crawl all disciplinary rulings (≈ 45 k) from https://tuchtrecht.overheid.nl
into tuchtrecht.jsonl, one JSON per line:
  {"url":…, "content":…, "source":"Tuchtrecht"}

Supports:
 • Resumable via visited.txt
 • 1–2 s polite delay + automatic retries
 • Flush every 100 items
 • Push to HF Hub if HF_TOKEN & HF_REPO are set
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
SHARDS_DIR    = Path("shards")
OUT_FILE      = None  # will be set in main()
VISITED_FILE  = Path("visited.txt")
SOURCE_NAME   = "Tuchtrecht"
HF_TOKEN      = os.getenv("HF_TOKEN")
HF_REPO       = os.getenv("HF_REPO", "vGassen/Dutch-Open-Data-Tuchrecht-Disciplinary-Court-Cases")
# ---------------------------------------------------------------------------- #


def next_shard_path() -> Path:
    """Return a new shard path like shards/shard_00001.jsonl"""
    SHARDS_DIR.mkdir(exist_ok=True)
    existing = [
        int(p.stem.split('_')[1])
        for p in SHARDS_DIR.glob('shard_*.jsonl')
        if p.stem.startswith('shard_')
    ]
    idx = max(existing) + 1 if existing else 0
    return SHARDS_DIR / f"shard_{idx:05d}.jsonl"


def build_session() -> requests.Session:
    """Session with retry/backoff and a polite UA."""
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
    Walk the paginated HTML search results and yield each ruling URL.
    """
    # 1) Prime the pump: get page 0 and find the total count by regex
    resp = session.get(BASE_SEARCH, params={"itemsPerPage": ITEMS_PER_PAGE, "page": 0}, timeout=30)
    resp.raise_for_status()
    soup = bs4.BeautifulSoup(resp.text, "lxml")
    text = soup.get_text(separator=" ", strip=True)
    m = re.search(r"van de\s+([0-9\.\s]+)\s+result", text)
    if not m:
        raise RuntimeError("⚠️ Couldn't locate total number of results on page 0")
    total = int(m.group(1).replace(".", "").replace(" ", ""))
    pages = math.ceil(total / ITEMS_PER_PAGE)

    # 2) Iterate each page
    for page in range(pages):
        resp = session.get(BASE_SEARCH, params={"itemsPerPage": ITEMS_PER_PAGE, "page": page}, timeout=30)
        resp.raise_for_status()
        soup = bs4.BeautifulSoup(resp.text, "lxml")

        # Find any <a href="/zoeken/resultaat/uitspraak/...">
        for a in soup.find_all("a", href=re.compile(r"^/zoeken/resultaat/uitspraak/")):
            yield urljoin(BASE_TUCH, a["href"])

        # polite delay
        time.sleep(random.uniform(1.0, 2.0))


def visible_text(html: str) -> str:
    """Extract and normalize all visible text from <main> while dropping
    navigation, footer and privacy-sensitive lines."""
    soup = bs4.BeautifulSoup(html, "lxml")

    # remove obvious non-content tags
    for tag in soup.find_all(["header", "footer", "nav", "script", "style"]):
        tag.decompose()

    container = soup.find("main") or soup.body
    if container is None:
        return ""

    raw = container.get_text(separator="\n", strip=True)

    # filter header/footer boilerplate
    exclude_phrases = [
        "Direct naar content",
        "Print Download PDF",
        "Terug naar zoekresultaten",
        "Over deze website",
        "Privacy en cookies",
        "Toegankelijkheid",
        "Sitemap",
        "Open data",
        "Linked Data",
        "MijnOverheid.nl",
        "Rijksoverheid.nl",
        "Ondernemersplein.nl",
        "Werkenbijdeoverheid.nl",
        "U bent hier:",
    ]

    lines = [l for l in raw.splitlines() if not any(p in l for p in exclude_phrases)]
    text = "\n".join(lines)

    # remove privacy-sensitive closing paragraph if present
    text = re.sub(r"Deze beslissing is gegeven door:.*?(?:Voorzitter|Secretaris).*?w\.g\.\s*", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"Uitgesproken ter openbare zitting.*?w\.g\.\s*", "", text, flags=re.DOTALL | re.IGNORECASE)

    return " ".join(text.split())


def crawl_one(url: str, session: requests.Session) -> dict | None:
    """Fetch one ruling, extract text, return JSON-ready dict or None on failure."""
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        content = visible_text(resp.text)
        if len(content) < 200:
            # skip spurious pages
            return None
        return {"url": url, "content": content, "source": SOURCE_NAME}
    except Exception as e:
        print(f"[WARN] {url} → {e}", file=sys.stderr)
        return None


def load_visited() -> set[str]:
    if not VISITED_FILE.exists():
        return set()
    return set(VISITED_FILE.read_text(encoding="utf-8").splitlines())


def append_visited(urls: Iterable[str]) -> None:
    with VISITED_FILE.open("a", encoding="utf-8") as f:
        for u in urls:
            f.write(u + "\n")


def append_jsonl(rows: Iterable[dict], out_file: Path) -> None:
    with out_file.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def push_to_hf(out_file: Path) -> None:
    if not HF_TOKEN:
        print("HF_TOKEN not set; skipping Hugging Face upload.")
        return
    login(token=HF_TOKEN)
    api = HfApi()
    api.create_repo(repo_id=HF_REPO, repo_type="dataset", exist_ok=True)
    api.upload_file(
        repo_id=HF_REPO,
        path_or_fileobj=str(out_file),
        path_in_repo=out_file.name,
        repo_type="dataset",
    )
    print(f"✅ Dataset pushed to Hugging Face: {HF_REPO}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hard-reset", action="store_true",
                        help="ignore visited.txt and start from scratch")
    parser.add_argument("--limit", type=int, default=5000,
                        help="maximum number of new rulings to crawl")
    args = parser.parse_args()

    global OUT_FILE
    OUT_FILE = next_shard_path()

    visited = set() if args.hard_reset else load_visited()
    new_rows: list[dict] = []
    new_visited: list[str] = []
    limit = args.limit
    grabbed = 0

    session = build_session()

    for page_url in tqdm(list_case_urls(session), desc="Crawling rulings"):
        if page_url in visited:
            continue

        if grabbed >= limit:
            break

        rec = crawl_one(page_url, session)
        if rec:
            new_rows.append(rec)
            new_visited.append(page_url)
            grabbed += 1

        # flush every 100 items
        if len(new_rows) >= 100:
            append_jsonl(new_rows, OUT_FILE)
            append_visited(new_visited)
            visited.update(new_visited)
            new_rows.clear()
            new_visited.clear()

    # final flush
    if new_rows:
        append_jsonl(new_rows, OUT_FILE)
        append_visited(new_visited)

    print(f"✅ Done crawling {grabbed} items. Total visited: {len(visited) + len(new_visited)}")

    # optionally push to HF
    push_to_hf(OUT_FILE)


if __name__ == "__main__":
    main()
