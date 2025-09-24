import logging
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

# Imports at top
from .auth import basic_auth_headers
from .request import do_request
from .mapping import cast_row

class ServiceNowFDW(ForeignDataWrapper):
    def __init__(self, options, columns):
        super().__init__(options, columns)
        self._columns = columns
        self.api_url = options.get("api_url")
        self.username = options.get("username")
        self.password = options.get("password")
        self.primary_key = options.get("primary_key")

    @property
    def rowid_column(self):
        return self.primary_key

    def execute(self, quals, columns):
        headers = basic_auth_headers(self.username, self.password)
        params = {q.field_name: q.value for q in quals if getattr(q, "operator", None) == "="}
        resp = do_request("GET", self.api_url, headers, params)
        for row in resp.get("result", []):
            yield cast_row(row, self._columns)

    def insert(self, new_values):
        headers = basic_auth_headers(self.username, self.password)
        resp = do_request("POST", self.api_url, headers, body=new_values)
        return cast_row(resp.get("result", new_values), self._columns)

    def update(self, rowid, new_values):
        headers = basic_auth_headers(self.username, self.password)
        url = f"{self.api_url}/{rowid}"
        resp = do_request("PUT", url, headers, body=new_values)
        return cast_row(resp.get("result", new_values), self._columns)

    def delete(self, rowid):
        headers = basic_auth_headers(self.username, self.password)
        url = f"{self.api_url}/{rowid}"
        do_request("DELETE", url, headers=headers)
