# crawler/parser.py
# This module is responsible for parsing the XML responses from the SRU endpoint.

from typing import Dict, Any, Optional
import requests

def get_full_text(url: str) -> Optional[str]:
    """
    Fetches the full text content from a given URL.
    It expects the content to be XML and will extract text from it.
    
    Args:
        url: The URL of the XML file.
        
    Returns:
        The extracted full text as a string, or None if fetching fails.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        # We assume the content is XML and needs parsing to extract text.
        # This is a simple text extraction. More complex XML structures might need a more robust parser.
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response.content)
        # Concatenate all text from all elements
        return "".join(root.itertext())
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch full text from {url}: {e}")
    except ET.ParseError as e:
        print(f"Failed to parse XML from {url}: {e}")
    return None

def parse_record(record: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Parses a single SRU record to extract URL, content, and source.

    Args:
        record: A dictionary representing a single SRU record.

    Returns:
        A dictionary with "URL", "Content", and "Source", or None if parsing fails.
    """
    try:
        record_data = record.get('sru:recordData', {}).get('gzd:gzd', {})
        enriched_data = record_data.get('gzd:enrichedData', {})
        
        # Prefer XML URL for full text extraction
        item_urls = enriched_data.get('gzd:itemUrl', [])
        if not isinstance(item_urls, list):
            item_urls = [item_urls]
            
        xml_url = None
        for item in item_urls:
            if item.get('@manifestation') == 'xml':
                xml_url = item.get('#text')
                break
        
        pdf_url = None
        if not xml_url:
            for item in item_urls:
                if item.get('@manifestation') == 'pdf':
                    pdf_url = item.get('#text')
                    break

        target_url = xml_url or pdf_url or enriched_data.get('gzd:url')

        if not target_url:
            return None

        content = get_full_text(target_url) if xml_url else "Content from non-XML source, e.g., PDF, not extracted."
        
        if not content:
            return None

        return {
            "URL": target_url,
            "Content": content,
            "Source": "Tuchtrecht"
        }
    except Exception as e:
        print(f"Error parsing record: {e}")
        return None
