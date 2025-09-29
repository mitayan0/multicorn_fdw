#multicorn_fdw/servicenow/api_client.py

import requests
from multicorn.utils import log_to_postgres
import logging


class ServiceNowApiClient:
    """Wrapper around ServiceNow REST API requests."""

    def __init__(self, api_url, username=None, password=None, headers=None):
        if not api_url:
            raise ValueError("api_url is required")
        self.api_url = api_url
        self.basic_auth = (username, password) if username and password else None
        self.headers = headers or {"Content-Type": "application/json"}

    def request(self, url=None, params=None, headers=None, body=None, method="GET"):
        """Perform HTTP request with ServiceNow."""
        target_url = url or self.api_url
        try:
            resp = requests.request(
                method,
                target_url,
                params=params,
                headers=headers or self.headers,
                json=body if isinstance(body, dict) else None,
                data=body if isinstance(body, str) else None,
                auth=self.basic_auth,
                timeout=30,
            )
            resp.raise_for_status()
            return resp
        except Exception as e:
            log_to_postgres(f"Request failed: {e}", level=logging.WARNING)
            raise
