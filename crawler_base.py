import json
import logging
import os
import time
import re
from pathlib import Path
from typing import Iterable, Iterator, List, Dict, Set

import requests
from bs4 import BeautifulSoup
from lxml import etree
from requests.adapters import HTTPAdapter, Retry
from datasets import Dataset, Features, Value
from tqdm import tqdm
from huggingface_hub import HfApi


class BaseCrawler:
    """Common utilities for disciplined crawlers."""

    def __init__(self, base_url: str, delay: float = 0.3, visited_file: str = "visited_xmls.txt"):
        self.base_url = base_url.rstrip("/")
        self.delay = delay
        self.visited_file = Path(visited_file)
        self.session = self._build_session()
        self.log = logging.getLogger(self.__class__.__name__)

    def _build_session(self) -> requests.Session:
        sess = requests.Session()
        retries = Retry(total=5, backoff_factor=1.5, status_forcelist=[500, 502, 503, 504])
        sess.mount("https://", HTTPAdapter(max_retries=retries))
        sess.headers.update({"User-Agent": "ESJ-Tuchtrecht-Scraper/1.0"})
        return sess

    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------
    def fetch_soup(self, path: str) -> BeautifulSoup:
        url = f"{self.base_url}{path}"
        for _ in range(3):
            try:
                res = self.session.get(url, timeout=30)
                res.raise_for_status()
                return BeautifulSoup(res.text, "lxml")
            except Exception as e:
                self.log.warning(json.dumps({"url": url, "error": str(e), "phase": "fetch"}))
                time.sleep(self.delay * 2)
        raise RuntimeError(f"Failed after retries: {url}")

    def strip_xml(self, xml_bytes: bytes) -> str:
        parser = etree.XMLParser(recover=True, encoding="utf-8")
        root = etree.fromstring(xml_bytes, parser=parser)
        text = " ".join(chunk.strip() for chunk in root.itertext() if chunk.strip())
        # remove closing paragraphs containing names
        text = re.sub(r"Aldus.*?(?:voorzitter|secretaris).*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"w\.g\.\s*", "", text, flags=re.IGNORECASE)
        return text.strip()

    def load_visited(self) -> Set[str]:
        if not self.visited_file.exists():
            return set()
        return set(self.visited_file.read_text(encoding="utf-8").splitlines())

    def append_visited(self, urls: Iterable[str]) -> None:
        if not urls:
            return
        with self.visited_file.open("a", encoding="utf-8") as f:
            for u in urls:
                f.write(u + "\n")

    def push_dataset(self, records: Iterable[Dict[str, str]], repo: str, token: str, private: bool = False) -> None:
        data = list(records)
        if not data:
            self.log.info("No data to push")
            return
        features = Features({"url": Value("string"), "content": Value("string"), "source": Value("string")})
        ds = Dataset.from_list(data, features=features)
        api = HfApi(token=token)
        api.create_repo(repo_id=repo, repo_type="dataset", exist_ok=True)
        ds.push_to_hub(repo_id=repo, token=token, split="train", private=private, max_shard_size="500MB")
        self.log.info("Uploaded %d rows to %s", len(data), repo)
