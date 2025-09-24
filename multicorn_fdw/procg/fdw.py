import logging
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

# Imports at top
from .auth import login_token
from .request import send_request, update_request, delete_request
from .mapping import map_row, normalize_items, unwrap_object

class ApiFdw(ForeignDataWrapper):
    """
    Fully dynamic REST API FDW supporting optional PK, full CRUD,
    exact-match filters only, pagination, token authentication.
    """

    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns
        self.url = options["url"]
        self.username = options.get("username")
        self.password = options.get("password")
        self.login_url = "http://129.146.123.215:5000/login"
        self.primary_key = options.get("primary_key")
        if self.primary_key and self.primary_key not in columns:
            raise ValueError("primary_key must exist in columns")

        self.pk_as_query_param = options.get("pk_as_query_param", "false").lower() == "true"

        page_opt = options.get("page")
        limit_opt = options.get("limit")
        self.paginated = page_opt is not None and limit_opt is not None
        if self.paginated:
            self.start_page = int(page_opt)
            self.limit = int(limit_opt)
            self.pagination_style = options.get("pagination_style", "path")
            self.only_first_page = options.get("only_first_page", "false").lower() == "true"

        self._token = None
        self._rowid_column = options.get("primary_key")

    @property
    def rowid_column(self):
        return self._rowid_column

    def _headers(self):
        token = login_token(self)
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _request(self, method, url, headers, **kwargs):
        return send_request(self, method, url, headers, **kwargs)

    def execute(self, quals, columns, sortkeys=None):
        headers = self._headers()
        filters = {q.field_name: q.value for q in quals if q.operator == "="}

        if self.paginated:
            page = self.start_page
            while True:
                if self.pagination_style == "path":
                    url = f"{self.url}/{page}/{self.limit}"
                    data = self._request("GET", url, headers, params=filters)
                elif self.pagination_style == "params":
                    params = {"page": page, "limit": self.limit, **filters}
                    data = self._request("GET", self.url, headers, params=params)
                else:
                    data = self._request("GET", self.url, headers, params=filters)

                items = normalize_items(data)
                if not items:
                    break
                for item in items:
                    yield map_row(item, self.columns)
                if len(items) < self.limit or self.only_first_page:
                    break
                page += 1
        else:
            data = self._request("GET", self.url, headers, params=filters)
            items = normalize_items(data)
            for item in items:
                yield map_row(item, self.columns)

    def insert(self, new_values):
        headers = self._headers()
        log_to_postgres(f"Inserting: {new_values}", level=logging.INFO)
        resp = self._request("POST", self.url, headers, json=new_values)
        data = unwrap_object(resp.json())
        return map_row(data, self.columns)

    def update(self, rowid, new_values):
        headers = self._headers()
        return update_request(self, rowid, new_values, headers)

    def delete(self, rowid):
        headers = self._headers()
        return delete_request(self, rowid, headers)
