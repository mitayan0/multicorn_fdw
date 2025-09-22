#servicenow_fdw/fdw.py

import json
import decimal
import logging
import requests
from dateutil import parser as dateutil_parser
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres


def _ensure_json(val):
    """Coerce raw values into JSON-friendly objects."""
    if val is None:
        return None
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {"value": val}
    return val


def _cast_value(col, val, col_type=None):
    """Cast raw value into Python object matching the column type."""
    if val is None or val == "":
        return None
    t = (col_type or "").lower() if col_type else ""
    try:
        if "json" in t:
            return json.dumps(_ensure_json(val))
        if "bool" in t:
            return str(val).lower() in ("true", "t", "yes", "1")
        if any(x in t for x in ("int", "bigint", "smallint")):
            return int(val)
        if any(x in t for x in ("decimal", "numeric")):
            return decimal.Decimal(str(val))
        if any(x in t for x in ("float", "double", "real")):
            return float(val)
        if "timestamp" in t:
            from datetime import datetime
            return val if isinstance(val, datetime) else dateutil_parser.parse(str(val))
        if "date" in t:
            from datetime import date
            return val if isinstance(val, date) else dateutil_parser.parse(str(val)).date()
        return str(val)
    except Exception:
        return val


class ServiceNowFDW(ForeignDataWrapper):
    """Foreign Data Wrapper for ServiceNow Table API."""

    def __init__(self, options, columns):
        super().__init__(options, columns)
        self._columns = columns
        self.api_url = options.get("api_url")
        if not self.api_url:
            raise ValueError("api_url is required")
        self.username = options.get("username")
        self.password = options.get("password")
        self.basic_auth = (self.username, self.password) if self.username and self.password else None
        self.primary_key = options.get("primary_key")
        self.headers = {"Content-Type": "application/json"}

    @property
    def rowid_column(self):
        return self.primary_key

    def _do_request(self, url, params=None, headers=None, body=None, method="GET"):
        try:
            resp = requests.request(
                method,
                url,
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

    def _map_row(self, data):
        row = {}
        for col in self._columns:
            val = data.get(col) if isinstance(data, dict) else None
            row[col] = _cast_value(col, val, getattr(self._columns.get(col), "type_name", None))
        return row

    # ---------- FDW API ----------

    def execute(self, quals, columns):
        params = {}
        for q in quals or []:
            if getattr(q, "operator", None) == "=":
                params[q.field_name] = q.value
        try:
            resp = self._do_request(self.api_url, params=params)
            data = resp.json().get("result", [])
            for item in data:
                yield self._map_row(item)
        except Exception as e:
            log_to_postgres(f"Execute failed: {e}", level=logging.WARNING)

    def insert(self, new_values):
        try:
            payload = {k: v if not hasattr(v, "isoformat") else v.isoformat()
                       for k, v in new_values.items() if v is not None}
            resp = self._do_request(self.api_url, body=payload, method="POST")
            data = resp.json().get("result", payload) if resp.content else payload
            return self._map_row(data)
        except Exception as e:
            log_to_postgres(f"Insert failed: {e}", level=logging.WARNING)
            return {c: new_values.get(c) for c in self._columns}

    def update(self, rowid, new_values):
        """
        Send only fields actually changed in the SQL UPDATE.
        """
        try:
            url = f"{self.api_url}/{rowid}"

            read_only = {
                "sys_id",
                "number",
                "sys_created_by",
                "sys_updated_on",
                "sys_updated_by",
                "opened_at",
            }

            payload = {}
            for k, v in new_values.items():
                if k in read_only or v is None:
                    continue

                # Exclude fields with empty JSON payloads that might be causing the 403
                if isinstance(v, dict) and v.get("value") == "":
                    continue
                if isinstance(v, str) and '{"value": ""}' in v:
                    continue

                if hasattr(v, "isoformat"):
                    payload[k] = v.isoformat()
                else:
                    payload[k] = v

            if not payload:
                log_to_postgres(f"No updatable fields for {rowid}", level=logging.INFO)
                return {c: new_values.get(c) for c in self._columns}

            log_to_postgres(
                f"PUT {url} with payload: {json.dumps(payload)}",
                level=logging.WARNING,
            )
            resp = self._do_request(url, body=payload, method="PUT")
            data = resp.json().get("result", payload) if resp.content else payload
            return self._map_row(data)

        except Exception as e:
            log_to_postgres(f"Update failed: {e}", level=logging.ERROR)
            return {c: new_values.get(c) for c in self._columns}

    def delete(self, rowid):
        try:
            url = f"{self.api_url}/{rowid}"
            self._do_request(url, method="DELETE")
        except Exception as e:
            log_to_postgres(f"Delete failed: {e}", level=logging.WARNING)
            return None

    def get_rel_size(self, quals, columns):
        try:
            resp = self._do_request(self.api_url)
            rows = resp.json().get("result", [])
            return (len(rows), len(columns) * 64)
        except Exception:
            return (100, 64)