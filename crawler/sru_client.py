# crawler/sru_client.py
# This module handles all SRU 2.0 protocol communication.

from typing import Iterator, Dict, Any

import requests

from .utils import get_session

_SESSION = get_session()

BASE_URL = "https://repository.overheid.nl/sru"
PAGE_SIZE = 100 # As per SRU documentation, max is 1000, but we'll use a smaller size

def get_records(query: str, start_date: str = None) -> Iterator[Dict[str, Any]]:
    """
    Fetches records from the SRU endpoint using pagination.

    Args:
        query: The base CQL query.
        start_date: An optional ISO 8601 date string to get modified records.

    Yields:
        A dictionary representing a single record from the SRU response.
    """
    start_record = 1
    
    if start_date:
        query = f"({query}) AND dt.modified>={start_date}"

    while True:
        params = {
            'operation': 'searchRetrieve',
            'version': '2.0',
            'query': query,
            'startRecord': start_record,
            'maximumRecords': PAGE_SIZE,
            'recordSchema': 'gzd',
            'httpAccept': 'application/xml',
        }
        
        try:
            response = _SESSION.get(BASE_URL, params=params)
            response.raise_for_status()
            
            import xmltodict
            data = xmltodict.parse(response.content)
            
            search_retrieve_response = data.get('sru:searchRetrieveResponse', {})
            records = search_retrieve_response.get('sru:records', {}).get('sru:record', [])

            if not records:
                break
            
            # If there's only one record, it's not in a list
            if not isinstance(records, list):
                records = [records]

            for record in records:
                yield record

            start_record += PAGE_SIZE

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from SRU endpoint: {e}")
            break
        except Exception as e:
            print(f"An error occurred while processing SRU response: {e}")
            break
