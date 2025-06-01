import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from huggingface_hub import HfApi
import json
import time
import os

# Config
HF_TOKEN = os.environ["HF_TOKEN"]
HF_REPO_ID = "vGassen/Dutch-Disciplinary-Law-Tuchtrecht"
START_DATE = datetime(2000, 1, 1)
BATCH_DAYS = 60  # ~1500 uitspraken per run

api = HfApi()
existing_files = api.list_repo_files(
    repo_id=HF_REPO_ID,
    repo_type="dataset",
    token=HF_TOKEN
)

# Output directory
REPO_DIR = Path(__file__).resolve().parent
DATA_DIR = REPO_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

def format_date(d): return d.strftime("%Y-%m-%d")

date_cursor = START_DATE
today = datetime.today()
processed = 0

while date_cursor <= today and processed < BATCH_DAYS:
    day_str = format_date(date_cursor)
    remote_path = f"tuchtrecht/{day_str}.jsonl"
    
    if remote_path in existing_files:
        print(f"✔️ Skipping {day_str} (already uploaded)")
        date_cursor += timedelta(days=1)
        continue

    url = f"https://repository.officiele-overheidspublicaties.nl/officielepublicaties/_events/{day_str}.xml"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️ No event file for {day_str}")
            date_cursor += timedelta(days=1)
            continue

        root = ET.fromstring(resp.content)
        records = root.findall(".//{http://standaarden.overheid.nl/sru}record")
        items = []

        for record in records:
            product_area = record.find(".//{http://standaarden.overheid.nl/sru}product-area")
            if product_area is None or "tuchtrecht" not in product_area.text.lower():
                continue

            url_elem = record.find(".//{http://standaarden.overheid.nl/sru}url")
            content_elem = record.find(".//{http://standaarden.overheid.nl/sru}originalData")

            items.append({
                "url": url_elem.text if url_elem is not None else "",
                "content": ET.tostring(content_elem, encoding="unicode") if content_elem is not None else "",
                "source": "Open Data Tuchtrecht"
            })

        if not items:
            print(f"➖ No Tuchtrecht items for {day_str}")
        else:
            out_file = DATA_DIR / f"{day_str}.jsonl"
            with open(out_file, "w", encoding="utf-8") as f:
                for item in items:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")

            print(f"⬆️ Uploading {out_file.name} ({len(items)} items)...")
            api.upload_file(
                path_or_fileobj=str(out_file),
                path_in_repo=f"tuchtrecht/{day_str}.jsonl",
                repo_id=HF_REPO_ID,
                repo_type="dataset",
                token=HF_TOKEN
            )

        processed += 1
        time.sleep(1)  # Be gentle

    except Exception as e:
        print(f"❌ Error on {day_str}: {e}")

    date_cursor += timedelta(days=1)

print(f"✅ Done. Uploaded {processed} new day(s).")
