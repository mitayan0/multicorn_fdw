#multicorn_fdw/procg/utils.py
import json


def normalize_items(response_json):
    """Extract list of items - finds any array in the response."""
    if isinstance(response_json, list):
        return response_json
    if isinstance(response_json, dict):
        # Find the first key that contains a list
        for key, value in response_json.items():
            if isinstance(value, list):
                return value
        # No array found, treat as single item
        return [response_json]
    return [response_json]


def unwrap_object(data):
    """Unwrap API payload - finds any nested object."""
    if isinstance(data, dict):
        # Find the first key that contains a non-empty dict
        for key, value in data.items():
            if isinstance(value, dict) and value:
                return value
    return data


def map_row(item, columns):
    """Map API JSON item into FDW row dict."""
    row = {}
    for col in columns:
        val = item.get(col) if isinstance(item, dict) else None
        if isinstance(val, (dict, list)):
            row[col] = json.dumps(val)
        else:
            row[col] = val
    return row


def build_request(base_url, pk_value=None, pk_as_query_param=False, primary_key=None, *extra_segments):
    """
    Build URL and params for APIs that may use path-style or query-param style IDs.
    Returns: (url, params)
    """
    base = "/".join([base_url.rstrip("/")] + [str(s).strip("/") for s in extra_segments])

    if pk_value is None:
        return base, {}

    if pk_as_query_param and primary_key:
        return base, {primary_key: pk_value}
    else:
        return f"{base}/{pk_value}", {}
    



