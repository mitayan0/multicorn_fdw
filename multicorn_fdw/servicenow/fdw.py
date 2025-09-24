import json
import logging
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

from .utils import map_row
from .api_client import ServiceNowApiClient


class ServiceNowFDW(ForeignDataWrapper):
    """Foreign Data Wrapper for ServiceNow Table API."""

    def __init__(self, options, columns):
        super().__init__(options, columns)
        self._columns = columns
        self.primary_key = options.get("primary_key")

        # Initialize API client
        self.api_client = ServiceNowApiClient(
            api_url=options.get("api_url"),
            username=options.get("username"),
            password=options.get("password"),
        )

    @property
    def rowid_column(self):
        return self.primary_key

    # ---------- FDW API ----------

    def execute(self, quals, columns):
        params = {}
        for q in quals or []:
            if getattr(q, "operator", None) == "=":
                params[q.field_name] = q.value
        try:
            resp = self.api_client.request(params=params)
            data = resp.json().get("result", [])
            for item in data:
                yield map_row(item, self._columns)
        except Exception as e:
            log_to_postgres(f"Execute failed: {e}", level=logging.WARNING)

    def insert(self, new_values):
        try:
            payload = {k: (v.isoformat() if hasattr(v, "isoformat") else v)
                       for k, v in new_values.items() if v is not None}
            resp = self.api_client.request(body=payload, method="POST")
            data = resp.json().get("result", payload) if resp.content else payload
            return map_row(data, self._columns)
        except Exception as e:
            log_to_postgres(f"Insert failed: {e}", level=logging.WARNING)
            return {c: new_values.get(c) for c in self._columns}

    def update(self, rowid, new_values):
        """
        Send only fields actually changed in the SQL UPDATE.
        """
        try:
            url = f"{self.api_client.api_url}/{rowid}"

            read_only = {
                "sys_id", "number", "sys_created_by",
                "sys_updated_on", "sys_updated_by", "opened_at",
            }

            payload = {}
            for k, v in new_values.items():
                if k in read_only or v is None:
                    continue
                if isinstance(v, dict) and v.get("value") == "":
                    continue
                if isinstance(v, str) and '{"value": ""}' in v:
                    continue
                payload[k] = v.isoformat() if hasattr(v, "isoformat") else v

            if not payload:
                log_to_postgres(f"No updatable fields for {rowid}", level=logging.INFO)
                return {c: new_values.get(c) for c in self._columns}

            log_to_postgres(f"PUT {url} with payload: {json.dumps(payload)}", level=logging.WARNING)
            resp = self.api_client.request(url=url, body=payload, method="PUT")
            data = resp.json().get("result", payload) if resp.content else payload
            return map_row(data, self._columns)
        except Exception as e:
            log_to_postgres(f"Update failed: {e}", level=logging.ERROR)
            return {c: new_values.get(c) for c in self._columns}

    def delete(self, rowid):
        try:
            url = f"{self.api_client.api_url}/{rowid}"
            self.api_client.request(url=url, method="DELETE")
        except Exception as e:
            log_to_postgres(f"Delete failed: {e}", level=logging.WARNING)
            return None

    def get_rel_size(self, quals, columns):
        try:
            resp = self.api_client.request()
            rows = resp.json().get("result", [])
            return (len(rows), len(columns) * 64)
        except Exception:
            return (100, 64)
