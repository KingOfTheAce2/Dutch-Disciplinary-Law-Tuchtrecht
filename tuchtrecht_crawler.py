import argparse
import logging
import os
import re
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, Iterator, List, Dict

from tqdm import tqdm
import requests
from huggingface_hub import HfApi

from crawler_base import BaseCrawler


class TuchtrechtCrawler(BaseCrawler):
    ROOT_PATH = "/frbr/tuchtrecht"
    PAGE_SIZE = 11
    XML_PATTERN = re.compile(r"^/frbr/tuchtrecht/[12]\d{3}/[A-Z0-9\-]+/ocrxml$")

    def __init__(self, base_url: str, delay: float, repo: str, max_items: int, workers: int = 1):
        super().__init__(base_url, delay)
        self.repo = repo
        self.max_items = max_items
        self.workers = workers

    def iter_paths(self) -> Iterator[str]:
        offset = 0
        while True:
            path = f"{self.ROOT_PATH}?start={offset}" if offset else self.ROOT_PATH
            soup = self.fetch_soup(path)
            links = [a.get("href", "") for a in soup.select("a[href^='/frbr/tuchtrecht/']")]
            paths = [href for href in links if self.XML_PATTERN.match(href)]
            if not paths:
                break
            for p in paths:
                yield p
            offset += self.PAGE_SIZE
            time.sleep(self.delay)

    def extract_xml(self, path: str) -> Dict[str, str] | None:
        try:
            url = f"{self.base_url}{path}"
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            text = self.strip_xml(resp.content)
            return {"url": url, "content": text, "source": "Tuchtrechtspraak"}
        except Exception as e:
            self.log.error("Error fetching %s: %s", path, e)
            return None

    def crawl(self) -> List[Dict[str, str]]:
        visited = self.load_visited()
        records: List[Dict[str, str]] = []
        new_seen: List[str] = []

        paths = list(self.iter_paths())
        bar = tqdm(paths, desc="XML links")
        for path in bar:
            url = f"{self.base_url}{path}"
            if url in visited:
                continue
            if len(records) >= self.max_items:
                break
            rec = self.extract_xml(path)
            if rec:
                records.append(rec)
                new_seen.append(url)
            if len(new_seen) >= 50:
                self.append_visited(new_seen)
                new_seen.clear()
        if new_seen:
            self.append_visited(new_seen)
        return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Tuchtrecht crawler")
    parser.add_argument("--limit", type=int, default=500, help="max documents")
    parser.add_argument("--delay", type=float, default=0.3, help="delay between requests")
    parser.add_argument("--repo", type=str, default=os.getenv("HF_DATASET_REPO", ""))
    parser.add_argument("--workers", type=int, default=1, help="parallel downloads")
    parser.add_argument("--resume", action="store_true", help="resume from visited log")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    crawler = TuchtrechtCrawler(
        base_url="https://repository.overheid.nl",
        delay=args.delay,
        repo=args.repo,
        max_items=args.limit,
        workers=args.workers,
    )

    records = crawler.crawl()
    if args.repo and os.getenv("HF_TOKEN"):
        crawler.push_dataset(records, args.repo, os.getenv("HF_TOKEN"))


if __name__ == "__main__":
    main()
