import json
from multicorn.utils import log_to_postgres
from .utils import map_row


def execute(fdw, quals, columns):
    params = {}
    for q in quals or []:
        if getattr(q, "operator", None) == "=":
            params[q.field_name] = q.value
    try:
        resp = fdw.api_client.request(params=params)
        data = resp.json().get("result", [])
        for item in data:
            yield map_row(item, fdw._columns)
    except Exception as e:
        log_to_postgres(f"Execute failed: {e}", level=40)  # WARNING


def insert(fdw, new_values):
    try:
        payload = {
            k: (v.isoformat() if hasattr(v, "isoformat") else v)
            for k, v in new_values.items()
            if v is not None
        }
        resp = fdw.api_client.request(body=payload, method="POST")
        data = resp.json().get("result", payload) if resp.content else payload
        return map_row(data, fdw._columns)
    except Exception as e:
        log_to_postgres(f"Insert failed: {e}", level=40)
        return {c: new_values.get(c) for c in fdw._columns}


def update(fdw, rowid, new_values):
    try:
        url = f"{fdw.api_client.api_url}/{rowid}"

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
            log_to_postgres(f"No updatable fields for {rowid}", level=20)  # INFO
            return {c: new_values.get(c) for c in fdw._columns}

        log_to_postgres(
            f"PUT {url} with payload: {json.dumps(payload)}",
            level=40
        )
        resp = fdw.api_client.request(url=url, body=payload, method="PUT")
        data = resp.json().get("result", payload) if resp.content else payload
        return map_row(data, fdw._columns)
    except Exception as e:
        log_to_postgres(f"Update failed: {e}", level=50)  # ERROR
        return {c: new_values.get(c) for c in fdw._columns}


def delete(fdw, rowid):
    try:
        url = f"{fdw.api_client.api_url}/{rowid}"
        fdw.api_client.request(url=url, method="DELETE")
    except Exception as e:
        log_to_postgres(f"Delete failed: {e}", level=40)
        return None


def get_rel_size(fdw, quals, columns):
    try:
        resp = fdw.api_client.request()
        rows = resp.json().get("result", [])
        return (len(rows), len(columns) * 64)
    except Exception:
        return (100, 64)
