import os
import requests
import xml.etree.ElementTree as ET
from huggingface_hub import HfApi
import json

# Config
HUGGINGFACE_TOKEN = os.getenv("HF_TOKEN")
REPO_ID = "vGassen/Dutch-Disciplinary-Law-Tuchtrecht"
BASE_URL = "https://repository.overheid.nl/sru"
NS = {
    "gzd": "http://standaarden.overheid.nl/sru/gzd/1.0",
    "dcterms": "http://purl.org/dc/terms/"
}

def fetch_eclis(count=50):
    url = (
        f"{BASE_URL}?version=2.0"
        f"&operation=searchRetrieve"
        f"&query=c.product-area==tuchtrecht"
        f"&startRecord=1"
        f"&maximumRecords={count}"
        f"&recordSchema=gzd"
    )
    r = requests.get(url)
    r.raise_for_status()
    tree = ET.fromstring(r.content)
    return tree.findall(".//gzd:record", NS)

def parse_record(record):
    identifier = record.find(".//dcterms:identifier", NS)
    original_data = record.find(".//gzd:originalData", NS)
    if identifier is None or original_data is None:
        return None
    ecli = identifier.text.strip()
    content = ET.tostring(original_data, encoding="unicode")
    return {
        "url": f"https://tuchtrecht.overheid.nl/{ecli}",
        "content": content,
        "source": "Tuchtrecht"
    }, ecli

def upload_records(records):
    api = HfApi(token=HF_TOKEN)
    for entry, ecli in records:
        filename = f"data/{ecli}.json"
        content_bytes = json.dumps(entry, ensure_ascii=False, indent=2).encode("utf-8")
        api.upload_file(
            path_or_fileobj=content_bytes,
            path_in_repo=filename,
            repo_id=REPO_ID,
            repo_type="dataset"
        )
        print(f"Uploaded: {filename}")

if __name__ == "__main__":
    if not HUGGINGFACE_TOKEN:
        raise RuntimeError("Missing HF_TOKEN environment variable")

    records = []
    raw_records = fetch_eclis()
    for r in raw_records:
        parsed = parse_record(r)
        if parsed:
            records.append(parsed)

    print(f"Parsed {len(records)} records, uploading to Hugging Face…")
    upload_records(records)
