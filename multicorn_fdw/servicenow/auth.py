def get_basic_auth(username, password):
    """
    Simple helper to return a tuple suitable for requests auth.
    """
    if username and password:
        return (username, password)
    return None
