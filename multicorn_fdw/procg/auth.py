import requests
import logging
from multicorn.utils import log_to_postgres

class Auth:
    """Handles token login and headers for REST API FDWs."""

    def __init__(self, username=None, password=None, login_url=None):
        self.username = username
        self.password = password
        self.login_url = login_url
        self._token = None

    def login(self):
        if self._token:
            return self._token
        if not self.login_url or not self.username or not self.password:
            return None
        try:
            payload = {"email_or_username": self.username, "password": self.password}
            resp = requests.post(self.login_url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            self._token = data.get("access_token")
            return self._token
        except Exception as e:
            log_to_postgres(f"Auth login failed: {e}", level=logging.WARNING)
            return None

    def headers(self):
        token = self.login()
        return {"Authorization": f"Bearer {token}"} if token else {}
