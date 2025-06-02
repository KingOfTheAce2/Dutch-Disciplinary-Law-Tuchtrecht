# scrape_and_push.py

import scrapy
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlencode
import json
import os
import pandas as pd
from huggingface_hub import HfApi, Repository

MAX_CASES = 50

class DisciplinaryCasesSpider(scrapy.Spider):
    name = "disciplinary_cases"
    base_url = "https://tuchtrecht.overheid.nl/Search/Search?"
    results = []
    total_cases = 0

    def start_requests(self):
        for beroepsgroep in self.beroepsgroepen:
            if self.total_cases >= MAX_CASES:
                break
            params = {
                "SearchJson": json.dumps({
                    "Beroep": beroepsgroep
                })
            }
            url = self.base_url + urlencode(params)
            yield scrapy.Request(url=url, callback=self.parse_search_results, meta={"beroepsgroep": beroepsgroep})

    def parse_search_results(self, response):
        if self.total_cases >= MAX_CASES:
            return
        beroepsgroep = response.meta["beroepsgroep"]
        case_links = response.css(".result-list a::attr(href)").getall()
        for link in case_links:
            if self.total_cases >= MAX_CASES:
                return
            yield response.follow(url=link, callback=self.parse_case_details, meta={"beroepsgroep": beroepsgroep})
        next_page = response.css(".pagination-next a::attr(href)").get()
        if next_page and self.total_cases < MAX_CASES:
            yield response.follow(url=next_page, callback=self.parse_search_results, meta={"beroepsgroep": beroepsgroep})

    def parse_case_details(self, response):
        if self.total_cases >= MAX_CASES:
            return
        ecli = response.css(".ecli::text").get()
        decision = response.css(".decision::text").get()
        if ecli and decision:
            self.results.append({
                "URL": f"https://tuchtrecht.overheid.nl/{ecli}",
                "decision": decision.strip(),
                "source": "Open Data Tuchrecht"
            })
            self.total_cases += 1

def run_spider_and_push():
    spider = DisciplinaryCasesSpider()
    process = CrawlerProcess(settings={"LOG_LEVEL": "ERROR"})
    process.crawl(spider)
    process.start()

    df = pd.DataFrame(spider.results)
    os.makedirs("tuchrecht_data", exist_ok=True)
    df.to_csv("tuchrecht_data/data.csv", index=False)

    hf_token = os.getenv("HF_TOKEN")
    repo_url = HfApi().create_repo(token=hf_token, repo_id="vGassen/Dutch-Disciplinary-Law-Tuchtrecht", repo_type="dataset", exist_ok=True)
    repo = Repository(local_dir="tuchrecht_data", clone_from=repo_url, token=hf_token)
    repo.push_to_hub(commit_message="Initial 50-case scrape")

if __name__ == "__main__":
    run_spider_and_push()
