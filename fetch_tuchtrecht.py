import os
import requests
import xml.etree.ElementTree as ET
from huggingface_hub import HfApi
import json
import time
from pathlib import Path

# Read Hugging Face token from environment variable (GitHub secret)
HF_TOKEN = os.environ["HF_TOKEN"]
HF_REPO_ID = "vGassen/Dutch-Disciplinary-Law-Tuchtrecht"

# Output directory
output_dir = Path("data")
output_dir.mkdir(exist_ok=True)

# SRU settings
base_url = "https://repository.overheid.nl/sru"
max_records = 100
start_record = 1
total_processed = 0
max_total_records = 10000  # adjust to limit how deep the script scans

# Hugging Face client
api = HfApi()

# Output file
output_file = output_dir / "tuchtrecht_batch.jsonl"
with output_file.open("w", encoding="utf-8") as out_f:
    while total_processed < max_total_records:
        params = {
            "version": "2.0",
            "operation": "searchRetrieve",
            "x-connection": "tuchtrecht",
            "query": "cql.allRecords=1",
            "maximumRecords": str(max_records),
            "startRecord": str(start_record)
        }

        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print(f"❌ Failed request at startRecord={start_record}")
            break

        root = ET.fromstring(response.content)
        records = root.findall(".//{http://standaarden.overheid.nl/sru}record")

        if not records:
            print("✅ No more records to scan.")
            break

        for record in records:
            try:
                product_area = record.find(".//{http://standaarden.overheid.nl/sru}product-area")
                if product_area is None or "tuchtrecht" not in product_area.text.lower():
                    continue

                identifier = record.find(".//{http://purl.org/dc/terms/}identifier")
                content_elem = record.find(".//{http://standaarden.overheid.nl/sru}originalData")

                if identifier is not None and content_elem is not None:
                    ecli = identifier.text.strip()
                    ecli_safe = ecli.replace(":", "_")
                    url = f"https://tuchtrecht.overheid.nl/{ecli_safe}"
                    content = ET.tostring(content_elem, encoding="unicode")

                    record_json = {
                        "url": url,
                        "content": content,
                        "source": "Open Data Tuchtrecht"
                    }

                    out_f.write(json.dumps(record_json, ensure_ascii=False) + "\n")
                    total_processed += 1
            except Exception as e:
                print(f"⚠️ Error parsing record: {e}")

        start_record += max_records
        time.sleep(1)  # respectful delay

# Upload to Hugging Face
api.upload_file(
    path_or_fileobj=str(output_file),
    path_in_repo=f"tuchtrecht/{output_file.name}",
    repo_id=HF_REPO_ID,
    repo_type="dataset",
    token=HF_TOKEN
)

print(f"✅ Uploaded {output_file.name} with {total_processed} records to Hugging Face.")
