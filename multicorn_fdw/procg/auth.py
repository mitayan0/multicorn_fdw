import requests

def login_token(fdw):
    if fdw._token:
        return fdw._token
    if not fdw.login_url or not fdw.username or not fdw.password:
        return None
    payload = {"email_or_username": fdw.username, "password": fdw.password}
    resp = requests.post(fdw.login_url, json=payload)
    resp.raise_for_status()
    data = resp.json()
    fdw._token = data.get("access_token")
    return fdw._token
