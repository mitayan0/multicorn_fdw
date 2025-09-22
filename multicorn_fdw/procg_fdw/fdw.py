#procg_fdw/fdw.py

import json
import logging
import requests
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

class ApiFdw(ForeignDataWrapper):
    """
    Fully dynamic REST API FDW supporting optional PK, full CRUD,
    exact-match filters only, pagination, token authentication.
    """
    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns
        self.url = options["url"]
        self.username = options.get("username")
        self.password = options.get("password")
        self.login_url = "http://129.146.123.215:5000/login"
        self.primary_key = options.get("primary_key")
        if self.primary_key and self.primary_key not in columns:
            raise ValueError("primary_key must exist in columns")

        # Option: PK as query parameter
        self.pk_as_query_param = (
            options.get("pk_as_query_param", "false").lower() == "true"
        )

        page_opt = options.get("page")
        limit_opt = options.get("limit")
        self.paginated = page_opt is not None and limit_opt is not None
        if self.paginated:
            self.start_page = int(page_opt)
            self.limit = int(limit_opt)
            self.pagination_style = options.get("pagination_style", "path")
            self.only_first_page = (
                options.get("only_first_page", "false").lower() == "true"
            )

        self._token = None
        self._rowid_column = options.get("primary_key")

    # ------------------ Multicorn requirement ------------------
    @property
    def rowid_column(self):
        return self._rowid_column

    # ------------------ AUTH ------------------
    def _login(self):
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

    def _headers(self):
        token = self._login()
        return {"Authorization": f"Bearer {token}"} if token else {}

    # ------------------ REQUEST ------------------
    def _request(self, method, url, headers, **kwargs):
        resp = requests.request(method, url, headers=headers, **kwargs)
        if resp.status_code == 401:
            log_to_postgres("Token expired, refreshing token.", level=logging.INFO)
            headers["Authorization"] = f"Bearer {self._login()}"
            resp = requests.request(method, url, headers=headers, **kwargs)
        resp.raise_for_status()
        return resp

    def _fetch(self, url, headers, params=None):
        resp = self._request("GET", url, headers, params=params)
        try:
            return resp.json()
        except Exception:
            return []

    # ------------------ HELPERS ------------------
    def _normalize_items(self, response_json):
        if isinstance(response_json, list):
            return response_json
        if isinstance(response_json, dict):
            for key in ("items", "results", "data"):
                if key in response_json and isinstance(response_json[key], list):
                    return response_json[key]
            return [response_json]
        return [response_json]

    def _unwrap_object(self, data):
        """Unwrap API payload when object is nested inside a key like data/item/result."""
        if isinstance(data, dict):
            for key in ("data", "item", "result"):
                if key in data and isinstance(data[key], dict):
                    return data[key]
        return data

    def _map_row(self, item):
        row = {}
        for col in self.columns:
            val = item.get(col) if isinstance(item, dict) else None
            if isinstance(val, (dict, list)):
                row[col] = json.dumps(val)
            else:
                row[col] = val
        return row

    # ------------------ READ ------------------
    def execute(self, quals, columns, sortkeys=None):
        headers = self._headers()
        filters = {}
        for q in quals:
            if q.field_name in self.columns and q.operator == "=":
                filters[q.field_name] = q.value

        if self.paginated:
            page = self.start_page
            while True:
                if self.pagination_style == "path":
                    url = f"{self.url}/{page}/{self.limit}"
                    data = self._fetch(url, headers, params=filters)
                elif self.pagination_style == "params":
                    params = {"page": page, "limit": self.limit, **filters}
                    data = self._fetch(self.url, headers, params=params)
                else:
                    data = self._fetch(self.url, headers, params=filters)

                items = self._normalize_items(data)
                if not items:
                    break
                for item in items:
                    yield self._map_row(item)
                if len(items) < self.limit or self.only_first_page:
                    break
                page += 1
        else:
            data = self._fetch(self.url, headers, params=filters)
            items = self._normalize_items(data)
            for item in items:
                yield self._map_row(item)

    # ------------------ INSERT ------------------
    def insert(self, new_values):
        headers = self._headers()
        log_to_postgres(f"Inserting: {new_values}", level=logging.INFO)
        resp = self._request("POST", self.url, headers, json=new_values)
        try:
            data = self._unwrap_object(resp.json())
            return self._map_row(data)
        except Exception as e:
            log_to_postgres(
                f"Error parsing JSON response after INSERT: {e}", level=logging.WARNING,
            )
            return new_values

    # ------------------ UPDATE ------------------
    def update(self, rowid, new_values):
        headers = self._headers()
        if self.pk_as_query_param:
            url = self.url
            params = {self.primary_key: rowid}
            log_to_postgres(
                f"Updating ID '{rowid}' at {url} with query params: {params}",
                level=logging.INFO,
            )
            resp = self._request("PUT", url, headers, json=new_values, params=params)
        else:
            url = f"{self.url}/{rowid}"
            log_to_postgres(
                f"Updating ID '{rowid}' at {url} with values: {new_values}",
                level=logging.INFO,
            )
            resp = self._request("PUT", url, headers, json=new_values)
        try:
            data = self._unwrap_object(resp.json())
            return self._map_row(data)
        except Exception as e:
            log_to_postgres(
                f"Error parsing JSON response after UPDATE: {e}", level=logging.WARNING,
            )
            return new_values

    # ------------------ DELETE ------------------
    def delete(self, rowid):
        headers = self._headers()
        # 1) Try DELETE with path param
        try:
            url = f"{self.url}/{rowid}"
            log_to_postgres(f"DELETE path param -> {url}", level=logging.INFO)
            self._request("DELETE", url, headers)
            return None
        except requests.HTTPError as e:
            if e.response is None or e.response.status_code not in (404, 400, 500):
                raise
            log_to_postgres(
                f"Path param DELETE failed (status {e.response.status_code}), trying next style.",
                level=logging.INFO,
            )

        # 2) Try query param
        if self.pk_as_query_param:
            try:
                params = {self.primary_key: rowid}
                log_to_postgres(
                    f"DELETE query param -> {self.url} with {params}", level=logging.INFO
                )
                self._request("DELETE", self.url, headers, params=params)
                return None
            except requests.HTTPError as e:
                if e.response is None or e.response.status_code not in (404, 400, 500):
                    raise
                log_to_postgres(
                    f"Query param DELETE failed (status {e.response.status_code}), trying JSON body.",
                    level=logging.INFO,
                )

        # 3) Try JSON body
        try:
            payload = {"control_environment_ids": [rowid]}
            log_to_postgres(
                f"DELETE JSON body -> {self.url} with {payload}", level=logging.INFO
            )
            self._request("DELETE", self.url, headers, json=payload)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                log_to_postgres(
                    f"Row {rowid} not found (DELETE JSON body)", level=logging.INFO
                )
                return None
            raise
        return None