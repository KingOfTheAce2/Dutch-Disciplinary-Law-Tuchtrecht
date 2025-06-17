import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from datasets import Dataset
from huggingface_hub import HfApi, login

# Directory for downloaded XML files
REPO_DIR = Path(__file__).resolve().parent
DATA_DIR = REPO_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# Output file named with today's date
TODAY = datetime.today().strftime("%Y-%m-%d")
FILENAME = f"tuchtrecht_{TODAY}.xml"
OUTPUT_FILE = DATA_DIR / FILENAME

# SRU endpoint parameters
URL = "https://repository.overheid.nl/sru"
PARAMS = {
    "version": "2.0",
    "operation": "searchRetrieve",
    "x-connection": "tuchtrecht",
    "query": "cql.allRecords=1",
    "maximumRecords": "100",
}

NS = {
    "gzd": "http://standaarden.overheid.nl/sru/gzd/1.0",
    "dcterms": "http://purl.org/dc/terms/",
}

# Dataset on Hugging Face to push the records to
HF_REPO_ID = "vGassen/Dutch-Disciplinary-Law-Tuchtrecht"


def fetch_xml() -> bytes:
    """Download XML records from the SRU endpoint."""
    resp = requests.get(URL, params=PARAMS, timeout=20)
    resp.raise_for_status()
    return resp.content


def parse_records(xml_content: bytes) -> list[dict]:
    """Parse SRU XML and extract ECLI and original XML."""
    root = ET.fromstring(xml_content)
    records = []
    for record in root.findall(".//gzd:record", NS):
        identifier = record.find(".//dcterms:identifier", NS)
        original_data = record.find(".//gzd:originalData", NS)
        if identifier is None or original_data is None:
            continue
        ecli = identifier.text.strip()
        xml_str = ET.tostring(original_data, encoding="unicode")
        records.append({
            "URL": f"https://tuchtrecht.overheid.nl/{ecli}",
            "xml": xml_str,
            "source": "Tuchtrecht",
        })
    return records


def push_dataset(records: list[dict]):
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("HF_TOKEN not provided")
        return
    login(token=token)
    ds = Dataset.from_list(records)
    api = HfApi()
    api.create_repo(repo_id=HF_REPO_ID, repo_type="dataset", exist_ok=True)
    ds.push_to_hub(HF_REPO_ID, private=False)


def main():
    xml_content = fetch_xml()
    OUTPUT_FILE.write_bytes(xml_content)
    records = parse_records(xml_content)
    if records:
        push_dataset(records)
    else:
        print("No records parsed")


if __name__ == "__main__":
    main()