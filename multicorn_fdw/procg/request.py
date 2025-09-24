import requests
import logging
from multicorn.utils import log_to_postgres

def do_request(method, url, headers=None, **kwargs):
    """Perform HTTP request with auto token refresh logic if 401."""
    resp = requests.request(method, url, headers=headers, **kwargs)
    if resp.status_code == 401:
        log_to_postgres("Token expired, refreshing token.", level=logging.INFO)
        # Assume headers['Authorization'] has a callable login()
        if "Authorization" in headers and callable(headers["Authorization"]):
            headers["Authorization"] = headers["Authorization"]()
            resp = requests.request(method, url, headers=headers, **kwargs)
    resp.raise_for_status()
    return resp

def fetch(method, url, headers=None, params=None):
    resp = do_request(method, url, headers=headers, params=params)
    try:
        return resp.json()
    except Exception:
        return []
