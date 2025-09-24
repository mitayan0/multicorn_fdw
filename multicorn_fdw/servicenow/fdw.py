import logging
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

from .auth import Auth
from .request import do_request
from .mapping import map_row, normalize_items, unwrap_object

class RestApiCrudFdw(ForeignDataWrapper):
    """
    Fully dynamic REST API FDW supporting optional PK, full CRUD,
    exact-match filters only, pagination, token authentication.
    """
    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns
        self.url = options["url"]
        self.primary_key = options.get("primary_key")
        if self.primary_key and self.primary_key not in columns:
            raise ValueError("primary_key must exist in columns")

        # Option: PK as query parameter
        self.pk_as_query_param = (
            options.get("pk_as_query_param", "false").lower() == "true"
        )

        page_opt = options.get("page")
        limit_opt = options.get("limit")
        self.paginated = page_opt is not None and limit_opt is not None
        if self.paginated:
            self.start_page = int(page_opt)
            self.limit = int(limit_opt)
            self.pagination_style = options.get("pagination_style", "path")
            self.only_first_page = (
                options.get("only_first_page", "false").lower() == "true"
            )

        self._auth = Auth(
            username=options.get("username"),
            password=options.get("password"),
            login_url="http://129.146.123.215:5000/login"
        )

        self._rowid_column = options.get("primary_key")

    # ------------------ Multicorn requirement ------------------
    @property
    def rowid_column(self):
        return self._rowid_column

    # ------------------ REQUEST ------------------
    def _request(self, method, url, headers, **kwargs):
        return do_request(method, url, headers=headers, **kwargs)

    def _fetch(self, url, headers, params=None):
        resp = self._request("GET", url, headers, params=params)
        try:
            return resp.json()
        except Exception:
            return []

    # ------------------ HELPERS ------------------
    def _normalize_items(self, response_json):
        return normalize_items(response_json)

    def _unwrap_object(self, data):
        return unwrap_object(data)

    def _map_row(self, item):
        return map_row(item, self.columns)

    # ------------------ READ, INSERT, UPDATE, DELETE ------------------
    # Copy all your original execute, insert, update, delete methods exactly
    # Replace self._headers() calls with self._auth.headers()
