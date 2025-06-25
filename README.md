# Dutch Disciplinary Law - Tuchtrecht Open Data

This project fetches daily data from the Tuchtrecht SRU endpoint provided by the Dutch government and:

1. Saves the XML to this repository.
2. Pushes the file to Hugging Face datasets.

## Setup

Use Python 3.11 which has pre-built wheels for all dependencies:

```bash
python3.11 -m pip install -r requirements.txt
```

## Daily Fetch Script

Run manually (the crawler fetches up to 5000 new rulings per run):

```bash
python main.py
```

Use `python main.py --resume` to continue from the last visited log instead of
starting a fresh crawl.

Each run writes a new JSONL file under `shards/` and uploads it to the
configured Hugging Face dataset. The current shard number is stored in
`last_shard.txt` so consecutive runs don't overwrite previous data.

Or add to cron to automate daily.

## Hugging Face

Set the following environment variables before running the fetch script:

* `HF_TOKEN` – an access token with write permissions
* `HF_DATASET_REPO` – Hugging Face dataset repository name
* `HF_PRIVATE` – set to `true` to create a private dataset (optional)

The dataset will be created under `HF_DATASET_REPO`, for example
`vGassen/Dutch-Open-Data-Tuchrecht-Disciplinary-Court-Cases`.

## GitHub Actions

A workflow is included to automate fetching. It runs every Sunday and can also
be triggered manually from the Actions tab. Configure the `HF_TOKEN` secret in
your repository settings so the script can push the latest data to Hugging Face.
