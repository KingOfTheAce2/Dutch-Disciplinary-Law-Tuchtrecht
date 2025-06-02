import sys
import requests
import xml.etree.ElementTree as ET
from huggingface_hub import HfApi, DatasetInfo

def fetch_uitspraak(ecli):
    url = f"https://repository.overheid.nl/sru?version=2.0&operation=searchRetrieve&query=dt.identifier={ecli}&maximumRecords=1&recordSchema=gzd"
    headers = {"Accept": "application/xml"}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    tree = ET.fromstring(r.content)
    ns = {
        "gzd": "http://standaarden.overheid.nl/sru/gzd/1.0",
        "dcterms": "http://purl.org/dc/terms/"
    }
    content = tree.find('.//dcterms:source', ns)
    uitspraak = tree.find('.//gzd:originalData', ns)
    uitspraak_text = ET.tostring(uitspraak, encoding='unicode') if uitspraak is not None else ''
    return {
        "url": f"https://tuchtrecht.overheid.nl/{ecli}",
        "content": uitspraak_text,
        "source": "Open Data Tuchtrecht"
    }

def push_to_huggingface(data, ecli):
    from huggingface_hub import HfApi
    api = HfApi(token=os.getenv("HUGGINGFACE_TOKEN"))
    repo_id = "vGassen/Dutch-Disciplinary-Law-Tuchtrecht"
    filename = f"{ecli}.json"
    content = f"{data}\n".encode("utf-8")
    api.upload_file(
        path_or_fileobj=content,
        path_in_repo=f"data/{filename}",
        repo_id=repo_id,
        repo_type="dataset"
    )

if __name__ == "__main__":
    import json, os
    if len(sys.argv) != 2:
        print("Usage: python upload_to_huggingface.py <ECLI>")
        sys.exit(1)

    ecli = sys.argv[1]
    result = fetch_uitspraak(ecli)
    push_to_huggingface(json.dumps(result, indent=2), ecli)
