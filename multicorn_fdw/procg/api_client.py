import requests
from multicorn.utils import log_to_postgres
import logging


class RestApiClient:
    """Handles authentication, token management, and HTTP requests."""

    def __init__(self, base_url, username=None, password=None, login_url=None):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.login_url = login_url
        self._token = None

    # ------------------ AUTH ------------------
    def login(self):
        if self._token:
            return self._token
        if not self.login_url or not self.username or not self.password:
            return None

        payload = {"email_or_username": self.username, "password": self.password}
        resp = requests.post(self.login_url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        self._token = data.get("access_token")
        return self._token

    def headers(self):
        token = self.login()
        return {"Authorization": f"Bearer {token}"} if token else {}

    # ------------------ REQUEST ------------------
    def request(self, method, url=None, **kwargs):
        target_url = url or self.base_url
        headers = kwargs.pop("headers", self.headers())
        resp = requests.request(method, target_url, headers=headers, **kwargs)
        if resp.status_code == 401:
            log_to_postgres("Token expired, refreshing token.", level=logging.INFO)
            headers["Authorization"] = f"Bearer {self.login()}"
            resp = requests.request(method, target_url, headers=headers, **kwargs)
        resp.raise_for_status()
        return resp

    def fetch(self, url=None, params=None):
        resp = self.request("GET", url, params=params)
        try:
            return resp.json()
        except Exception:
            return []
