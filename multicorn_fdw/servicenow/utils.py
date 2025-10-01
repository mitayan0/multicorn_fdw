#multicorn_fdw/servicenow/utils.py
import json
import decimal
from dateutil import parser as dateutil_parser


def ensure_json(val):
    """Coerce raw values into JSON-friendly objects."""
    if val is None:
        return None
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {"value": val}
    return val


def cast_value(col, val, col_type=None):
    """Cast raw value into Python object matching the column type."""
    if val is None or val == "":
        return None

    t = (col_type or "").lower() if col_type else ""
    try:
        if "json" in t:
            return json.dumps(ensure_json(val))
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


def map_row(data, columns):
    """Map raw API data dict into a row compatible with FDW columns."""
    row = {}
    for col in columns:
        val = data.get(col) if isinstance(data, dict) else None
        row[col] = cast_value(col, val, getattr(columns.get(col), "type_name", None))
    return row


def _safe_json(resp):
    if not resp or not getattr(resp, "content", None):
        return None
    try:
        return resp.json()
    except Exception:
        return None


def result_list(resp, default=None):
    """Return a list of rows from any JSON shape."""
    if default is None:
        default = []
    payload = _safe_json(resp)
    if payload is None:
        return default

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for v in payload.values():
            if isinstance(v, list):
                return v
        return [payload]

    return default


def result_obj(resp, default=None):
    """Return a single row (dict-like) from any JSON shape."""
    if default is None:
        default = {}
    payload = _safe_json(resp)
    if payload is None:
        return default

    if isinstance(payload, dict):
        return payload

    if isinstance(payload, list):
        return payload[0] if payload else default

    return default