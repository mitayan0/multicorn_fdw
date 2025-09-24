import requests
from multicorn.utils import log_to_postgres

def do_request(method, url, headers=None, params=None, body=None):
    try:
        resp = requests.request(method, url, headers=headers, params=params, json=body, timeout=30)
        resp.raise_for_status()
        return resp.json() if resp.content else {}
    except Exception as e:
        log_to_postgres(f"Request failed: {e}", level=30)
        raise
