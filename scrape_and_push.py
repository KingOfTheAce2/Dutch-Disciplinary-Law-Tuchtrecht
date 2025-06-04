import scrapy
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlencode
import json
import os
from huggingface_hub import HfApi, Repository

MAX_CASES = 50

class DisciplinaryCasesSpider(scrapy.Spider):
    name = "disciplinary_cases"
    base_url = "https://tuchtrecht.overheid.nl/Search/Search?"
    total_cases = 0

    custom_settings = {
        "FEED_EXPORT_FIELDS": ["URL", "decision", "source"]
    }

    def start_requests(self):
        # No filters at all – get all cases
        params = {
            "SearchJson": json.dumps({})
        }
        url = self.base_url + urlencode(params)
        yield scrapy.Request(url=url, callback=self.parse_search_results)

    def parse_search_results(self, response):
        if self.total_cases >= MAX_CASES:
            return
        case_links = response.css(".result-list a::attr(href)").getall()
        for link in case_links:
            if self.total_cases >= MAX_CASES:
                return
            yield response.follow(url=link, callback=self.parse_case_details)

        next_page = response.css(".pagination-next a::attr(href)").get()
        if next_page and self.total_cases < MAX_CASES:
            yield response.follow(url=next_page, callback=self.parse_search_results)

    def parse_case_details(self, response):
        if self.total_cases >= MAX_CASES:
            return
        ecli = response.css(".ecli::text").get()
        decision = response.css(".decision::text").get()
        if ecli and decision:
            self.total_cases += 1
            yield {
                "URL": f"https://tuchtrecht.overheid.nl/{ecli}",
                "decision": decision.strip(),
                "source": "Open Data Tuchrecht"
            }

def run_spider_and_push():
    hf_token = os.getenv("HF_TOKEN")
    repo_url = HfApi().create_repo(
        token=hf_token,
        repo_id="vGassen/Dutch-Disciplinary-Law-Tuchtrecht",
        repo_type="dataset",
        exist_ok=True
    )

    repo = Repository(local_dir="tuchrecht_data", clone_from=repo_url, token=hf_token)

    process = CrawlerProcess(settings={
        "LOG_LEVEL": "ERROR",
        "FEEDS": {
            f"{repo.local_dir}/data.csv": {
                "format": "csv",
                "overwrite": True,
            },
        },
    })
    process.crawl(DisciplinaryCasesSpider)
    process.start()

    repo.push_to_hub(commit_message="Scraped 50 tuchtrecht cases")

if __name__ == "__main__":
    run_spider_and_push()
