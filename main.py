#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import logging
import re
from typing import List, Dict, Iterable

import requests
from bs4 import BeautifulSoup
from lxml import etree
from datasets import Dataset, Features, Value
from tqdm import tqdm
from requests.adapters import HTTPAdapter, Retry

BASE_URL = "https://repository.overheid.nl"
ROOT_PATH = "/frbr/tuchtrecht"
HEADERS = {"User-Agent": "ESJ-Tuchtrecht-Scraper/1.0"}
SLEEP = 0.3
RETRIES = Retry(total=5, backoff_factor=1.5, status_forcelist=[500, 502, 503, 504])

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
session = requests.Session()
session.headers.update(HEADERS)
session.mount("https://", HTTPAdapter(max_retries=RETRIES))


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


def discover_xml_urls() -> List[str]:
    """Return a flat list of all XML file URLs under the tuchtrecht section."""
    xml_urls: List[str] = []
    years = discover_years()
    for idx, year_path in enumerate(years, 1):
        logging.info("Processing year %s (%d/%d)", year_path.rsplit("/", 1)[-1], idx, len(years))
        for doc_path in discover_documents(year_path):
            try:
                xmls = get_xml_urls(doc_path)
                xml_urls.extend(xmls)
            except Exception as e:
                logging.error("Failed to list XML for %s: %s", doc_path, e)
    logging.info("Discovered %d XML files", len(xml_urls))
    return xml_urls


def record_stream(xml_urls: Iterable[str]) -> Iterable[Dict[str, str]]:
    for idx, xml_url in enumerate(xml_urls, 1):
        logging.info("Fetching XML %d: %s", idx, xml_url)
        try:
            resp = session.get(xml_url, timeout=60)
            resp.raise_for_status()
            content = strip_xml(resp.content)
            yield {
                "url": xml_url,
                "content": content,
                "source": "Tuchtrechtspraak",
            }
        except Exception as e:
            logging.error("Failed to fetch %s: %s", xml_url, e)
        finally:
            time.sleep(SLEEP)


def push_dataset(records: Iterable[Dict[str, str]]):
    repo = os.environ["HF_DATASET_REPO"]
    token = os.environ["HF_TOKEN"]
    private = os.getenv("HF_PRIVATE", "false").lower() == "true"

    features = Features({
        "url": Value("string"),
        "content": Value("string"),
        "source": Value("string"),
    })

    chunk, chunk_size = [], 1000
    total = 0
    for rec in tqdm(records, desc="Scraping XML"):
        chunk.append(rec)
        if len(chunk) >= chunk_size:
            _upload(chunk, features, repo, token, private)
            total += len(chunk)
            chunk.clear()
    if chunk:
        _upload(chunk, features, repo, token, private)
        total += len(chunk)
    logging.info("Finished uploading %d records.", total)


def _upload(data: List[Dict[str, str]], features, repo, token, private):
    ds = Dataset.from_list(data, features=features)
    ds.push_to_hub(
        repo_id=repo,
        token=token,
        split="train",
        private=private,
        max_shard_size="500MB",
    )
    logging.info("Uploaded %d rows to %s", len(data), repo)


def main() -> None:
    try:
        logging.info("STEP 1: Finding all crawlable links for tuchtrecht")
        xml_urls = discover_xml_urls()

        logging.info("STEP 2: Crawling all crawlable links")
        records = record_stream(xml_urls)

        logging.info("STEP 3: Scraping XML tags and uploading to Hugging Face")
        push_dataset(records)
    except KeyError as e:
        logging.critical("Missing env var: %s", e)
        exit(1)


if __name__ == "__main__":
    main()
