from requests.auth import HTTPBasicAuth

def basic_auth_headers(username, password):
    return {"Authorization": HTTPBasicAuth(username, password)}
