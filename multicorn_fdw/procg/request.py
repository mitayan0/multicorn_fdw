import requests
from multicorn.utils import log_to_postgres
from .mapping import unwrap_object, map_row
from .auth import login_token

def send_request(fdw, method, url, headers, **kwargs):
    resp = requests.request(method, url, headers=headers, **kwargs)
    if resp.status_code == 401:
        log_to_postgres("Token expired, refreshing token.", level=20)
        headers["Authorization"] = f"Bearer {login_token(fdw)}"
        resp = requests.request(method, url, headers=headers, **kwargs)
    resp.raise_for_status()
    return resp

def update_request(fdw, rowid, new_values, headers):
    url = f"{fdw.url}/{rowid}" if not fdw.pk_as_query_param else fdw.url
    params = {fdw.primary_key: rowid} if fdw.pk_as_query_param else None
    resp = send_request(fdw, "PUT", url, headers, json=new_values, params=params)
    return map_row(unwrap_object(resp.json()), fdw.columns)

def delete_request(fdw, rowid, headers):
    try:
        url = f"{fdw.url}/{rowid}"
        send_request(fdw, "DELETE", url, headers)
        return None
    except requests.HTTPError:
        return None
