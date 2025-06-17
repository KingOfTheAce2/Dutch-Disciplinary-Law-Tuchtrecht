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

Run manually:

```bash
python fetch_tuchtrecht.py
```

Or add to cron to automate daily.

## Hugging Face

Set the following environment variables before running the fetch script:

* `HF_TOKEN` – an access token with write permissions

The dataset will be created under `vGassen/Dutch-Disciplinary-Law-Tuchtrecht`.

## GitHub Actions

A workflow is included to automate fetching. It runs every Sunday and can also
be triggered manually from the Actions tab. Configure the `HF_TOKEN` secret in
your repository settings so the script can push the latest data to Hugging Face.
