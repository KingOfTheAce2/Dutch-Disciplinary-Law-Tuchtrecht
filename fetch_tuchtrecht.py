import requests
from datetime import datetime
from pathlib import Path
from huggingface_hub import HfApi
import subprocess

# Config
REPO_DIR = Path(__file__).resolve().parent
DATA_DIR = REPO_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
TODAY = datetime.today().strftime("%Y-%m-%d")
FILENAME = f"tuchtrecht_{TODAY}.xml"
OUTPUT_FILE = DATA_DIR / FILENAME

# SRU request
params = {
    "version": "2.0",
    "operation": "searchRetrieve",
    "x-connection": "tuchtrecht",
    "query": "cql.allRecords=1",
    "maximumRecords": "100"
}
url = "https://repository.overheid.nl/sru"
response = requests.get(url, params=params)
response.raise_for_status()
OUTPUT_FILE.write_bytes(response.content)
print(f"Saved: {OUTPUT_FILE}")

# GitHub push
subprocess.run(["git", "add", str(OUTPUT_FILE)])
subprocess.run(["git", "commit", "-m", f"Daily update {TODAY}"])
subprocess.run(["git", "push"])

# Hugging Face push
HF_TOKEN = "your-huggingface-token"  # replace this!
HF_REPO_ID = "your-username/your-dataset-name"  # replace this!

api = HfApi()
api.upload_file(
    path_or_fileobj=str(OUTPUT_FILE),
    path_in_repo=f"tuchtrecht/{FILENAME}",
    repo_id=HF_REPO_ID,
    repo_type="dataset",
    token=HF_TOKEN
)
