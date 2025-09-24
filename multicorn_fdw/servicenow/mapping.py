import json
import decimal
from dateutil import parser as dateutil_parser

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

def cast_row(data, columns):
    """
    Map API response to FDW columns, using the original _map_row logic
    """
    row = {}
    for col in columns:
        val = data.get(col) if isinstance(data, dict) else None
        col_type = getattr(columns.get(col), "type_name", None)
        row[col] = _cast_value(col, val, col_type)
    return row
