import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_session() -> requests.Session:
    """Return a requests session with retry policy for transient errors."""
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
