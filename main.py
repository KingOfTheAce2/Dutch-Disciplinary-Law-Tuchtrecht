#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import json
import logging
import re
from pathlib import Path
from typing import List, Dict, Iterable, Set

import requests
from bs4 import BeautifulSoup
from lxml import etree
from tqdm import tqdm
from requests.adapters import HTTPAdapter, Retry
from huggingface_hub import HfApi

BASE_URL = "https://repository.overheid.nl"
ROOT_PATH = "/frbr/tuchtrecht"
HEADERS = {"User-Agent": "Tuchtrecht-Scraper"}
SLEEP = 0.5
RETRIES = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
session = requests.Session()
session.headers.update(HEADERS)
session.mount("https://", HTTPAdapter(max_retries=RETRIES))

# Crawl configuration
VISITED_FILE = "visited_xmls.txt"
LIMIT = 500


def load_visited() -> Set[str]:
    """Return a set of already processed XML URLs."""
    if not os.path.exists(VISITED_FILE):
        return set()
    with open(VISITED_FILE, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())


def append_visited(urls: Iterable[str]) -> None:
    """Append new visited URLs to VISITED_FILE."""
    if not urls:
        return
    with open(VISITED_FILE, "a", encoding="utf-8") as f:
        for u in urls:
            f.write(u + "\n")


def fetch_soup(path: str) -> BeautifulSoup:
    url = f"{BASE_URL}{path}"
    for _ in range(3):
        try:
            res = session.get(url, timeout=30)
            res.raise_for_status()
            return BeautifulSoup(res.text, "lxml")
        except Exception as e:
            logging.warning("Retry %s for %s (%s)", _ + 1, url, e)
            time.sleep(SLEEP * 2)
    raise Exception(f"Failed after retries: {url}")


def strip_xml(xml_bytes: bytes) -> str:
    parser = etree.XMLParser(recover=True, encoding="utf-8")
    root = etree.fromstring(xml_bytes, parser=parser)
    return " ".join(chunk.strip() for chunk in root.itertext() if chunk.strip())


def discover_years() -> List[str]:
    paths = []
    page = 0
    while True:
        page_path = f"{ROOT_PATH}?start={page * 11}" if page else ROOT_PATH
        soup = fetch_soup(page_path)

        year_links = soup.select("ul.browse__list a[href^='/frbr/tuchtrecht/']")
        if not year_links:
            break

        found = 0
        for a in year_links:
            href = a.get("href", "")
            # Match only hrefs like /frbr/tuchtrecht/1994
            if href.count("/") == 3 and href[len("/frbr/tuchtrecht/"):].isdigit():
                paths.append(href)
                found += 1

        if found == 0:
            break

        page += 1
        time.sleep(SLEEP)

    logging.info("Discovered %d years", len(paths))
    return paths


def discover_documents(year_path: str) -> List[str]:
    """Return all work-level paths for a specific year."""
    doc_paths: List[str] = []
    page = 0
    # Works are listed as ``/frbr/tuchtrecht/<year>/<ECLI>``. The ECLI may
    # contain colons and other characters, so match everything up to the next
    # slash.
    pattern = re.compile(rf"^/frbr/tuchtrecht/\d{{4}}/[^/]+$")

    while True:
        page_path = f"{year_path}?start={page * 11}" if page else year_path
        soup = fetch_soup(page_path)
        links = [a.get("href", "") for a in soup.select("a[href^='/frbr/tuchtrecht/']")]
        new_links = [href for href in links if pattern.match(href)]
        if not new_links:
            break
        doc_paths.extend(new_links)
        page += 1
        time.sleep(SLEEP)

    logging.info("  Found %d documents for %s", len(doc_paths), year_path)
    return doc_paths


def get_xml_urls(doc_path: str) -> List[str]:
    """Return the direct XML file URLs for a work."""
    expr_xml_path = f"{doc_path}/1/xml/"
    soup = fetch_soup(expr_xml_path)
    xml_links = soup.select("a[href$='.xml']")
    urls: List[str] = []
    for a in xml_links:
        href = a.get("href", "")
        if href:
            if href.startswith("http"):
                urls.append(href)
            else:
                urls.append(f"{BASE_URL}{href}")
    return urls


def discover_xml_urls(visited: Set[str], limit: int) -> List[str]:
    """Return up to ``limit`` new XML URLs that haven't been crawled yet."""
    xml_urls: List[str] = []
    years = discover_years()
    for idx, year_path in enumerate(years, 1):
        if len(xml_urls) >= limit:
            break
        logging.info("Processing year %s (%d/%d)", year_path.rsplit("/", 1)[-1], idx, len(years))
        for doc_path in discover_documents(year_path):
            if len(xml_urls) >= limit:
                break
            try:
                for url in get_xml_urls(doc_path):
                    if url in visited:
                        continue
                    xml_urls.append(url)
                    if len(xml_urls) >= limit:
                        break
            except Exception as e:
                logging.error("Failed to list XML for %s: %s", doc_path, e)
        time.sleep(SLEEP)
    logging.info("Discovered %d new XML files", len(xml_urls))
    return xml_urls


def record_stream(xml_urls: Iterable[str], visited: Set[str], limit: int) -> Iterable[Dict[str, str]]:
    """Yield records for XML URLs, stopping after ``limit`` new ones."""
    new_visited: List[str] = []
    count = 0
    for xml_url in xml_urls:
        if xml_url in visited:
            continue
        if count >= limit:
            break

        logging.info("Fetching XML %d: %s", count + 1, xml_url)
        try:
            resp = session.get(xml_url, timeout=60)
            resp.raise_for_status()
            content = strip_xml(resp.content)
            yield {
                "url": xml_url,
                "content": content,
                "source": "Tuchtrechtspraak",
            }
            visited.add(xml_url)
            new_visited.append(xml_url)
            count += 1
            if len(new_visited) >= 50:
                append_visited(new_visited)
                new_visited.clear()
        except Exception as e:
            logging.error("Failed to fetch %s: %s", xml_url, e)
        finally:
            time.sleep(SLEEP)

    if new_visited:
        append_visited(new_visited)


def push_dataset(records: Iterable[Dict[str, str]]):
    """Save ``records`` to a new JSONL shard and upload it."""
    repo = os.environ["HF_DATASET_REPO"]
    token = os.environ["HF_TOKEN"]
    private = os.getenv("HF_PRIVATE", "false").lower() == "true"

    data = list(records)
    if not data:
        logging.info("No new records to upload")
        return

    shard_dir = Path("shards")
    shard_dir.mkdir(exist_ok=True)
    shard_idx = 0
    if os.path.exists("last_shard.txt"):
        with open("last_shard.txt", "r", encoding="utf-8") as f:
            shard_idx = int(f.read().strip() or 0)

    shard_path = shard_dir / f"shard-{shard_idx:05d}.jsonl"
    with shard_path.open("w", encoding="utf-8") as f:
        for rec in data:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    api = HfApi(token=token)
    api.upload_file(
        path_or_fileobj=str(shard_path),
        path_in_repo=shard_path.as_posix(),
        repo_id=repo,
        repo_type="dataset",
        commit_message=f"Add shard {shard_idx}",
        private=private,
    )

    with open("last_shard.txt", "w", encoding="utf-8") as f:
        f.write(str(shard_idx + 1))

    logging.info("Uploaded %s with %d records", shard_path.name, len(data))


def main() -> None:
    try:
        logging.info("STEP 1: Finding all crawlable links for tuchtrecht")
        visited = load_visited()
        xml_urls = discover_xml_urls(visited, LIMIT)

        logging.info("STEP 2: Crawling up to %d new XMLs", LIMIT)
        records = record_stream(xml_urls, visited, LIMIT)

        logging.info("STEP 3: Scraping XML tags and uploading to Hugging Face")
        push_dataset(records)
    except KeyError as e:
        logging.critical("Missing env var: %s", e)
        exit(1)


if __name__ == "__main__":
    main()
