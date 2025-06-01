# Dutch Disciplinary Law - Tuchtrecht Open Data

This project fetches daily data from the Tuchtrecht SRU endpoint provided by the Dutch government and:

1. Saves the XML to this repository.
2. Pushes the file to Hugging Face datasets.

## Setup

```bash
pip install -r requirements.txt
```

## Daily Fetch Script

Run manually:

```bash
python fetch_tuchtrecht.py
```

Or add to cron to automate daily.

## Hugging Face

Make sure to set your token and dataset repo name in `fetch_tuchtrecht.py`.
