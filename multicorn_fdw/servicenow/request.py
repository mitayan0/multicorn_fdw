import requests
from multicorn.utils import log_to_postgres
import logging

def do_request(method, url, headers=None, params=None, body=None, auth=None):
    """
    Perform an HTTP request safely, logging errors.
    """
    try:
        resp = requests.request(
            method,
            url,
            headers=headers,
            params=params,
            json=body if isinstance(body, dict) else None,
            data=body if isinstance(body, str) else None,
            auth=auth,
            timeout=30
        )
        resp.raise_for_status()
        return resp
    except Exception as e:
        log_to_postgres(f"Request failed: {e}", level=logging.WARNING)
        raise
