import logging
from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres

from .auth import Auth
from .request import do_request, fetch
from .mapping import map_row, normalize_items, unwrap_object

class ApiFdw(ForeignDataWrapper):
    """Fully dynamic REST API FDW supporting PK, CRUD, pagination, token auth."""

    def __init__(self, options, columns):
        super().__init__(options, columns)
        self.columns = columns
        self.url = options["url"]
        self.primary_key = options.get("primary_key")
        if self.primary_key and self.primary_key not in columns:
            raise ValueError("primary_key must exist in columns")

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

    @property
    def rowid_column(self):
        return self._rowid_column

    # ------------------ READ ------------------
    def execute(self, quals, columns, sortkeys=None):
        headers = self._auth.headers()
        # Wrap login for request module
        headers["Authorization"] = lambda: f"Bearer {self._auth.login()}"
        filters = {q.field_name: q.value for q in quals if q.field_name in self.columns and q.operator == "="}

        if self.paginated:
            page = self.start_page
            while True:
                if self.pagination_style == "path":
                    url = f"{self.url}/{page}/{self.limit}"
                    data = fetch("GET", url, headers=headers, params=filters)
                elif self.pagination_style == "params":
                    params = {"page": page, "limit": self.limit, **filters}
                    data = fetch("GET", self.url, headers=headers, params=params)
                else:
                    data = fetch("GET", self.url, headers=headers, params=filters)

                items = normalize_items(data)
                if not items:
                    break
                for item in items:
                    yield map_row(item, self.columns)
                if len(items) < self.limit or self.only_first_page:
                    break
                page += 1
        else:
            data = fetch("GET", self.url, headers=headers, params=filters)
            items = normalize_items(data)
            for item in items:
                yield map_row(item, self.columns)

    # ------------------ INSERT ------------------
    def insert(self, new_values):
        headers = self._auth.headers()
        headers["Authorization"] = lambda: f"Bearer {self._auth.login()}"
        log_to_postgres(f"Inserting: {new_values}", level=logging.INFO)
        resp = do_request("POST", self.url, headers=headers, json=new_values)
        try:
            data = unwrap_object(resp.json())
            return map_row(data, self.columns)
        except Exception:
            return new_values

    # ------------------ UPDATE ------------------
    def update(self, rowid, new_values):
        headers = self._auth.headers()
        headers["Authorization"] = lambda: f"Bearer {self._auth.login()}"
        if self.pk_as_query_param:
            url = self.url
            params = {self.primary_key: rowid}
            resp = do_request("PUT", url, headers=headers, json=new_values, params=params)
        else:
            url = f"{self.url}/{rowid}"
            resp = do_request("PUT", url, headers=headers, json=new_values)
        try:
            data = unwrap_object(resp.json())
            return map_row(data, self.columns)
        except Exception:
            return new_values

    # ------------------ DELETE ------------------
    def delete(self, rowid):
        headers = self._auth.headers()
        headers["Authorization"] = lambda: f"Bearer {self._auth.login()}"

        # 1) Try DELETE with path param
        try:
            url = f"{self.url}/{rowid}"
            log_to_postgres(f"DELETE path param -> {url}", level=logging.INFO)
            do_request("DELETE", url, headers=headers)
            return None
        except Exception as e:
            # Only handle HTTPError with specific codes
            if hasattr(e, "response") and e.response is not None and e.response.status_code not in (404, 400, 500):
                raise
            log_to_postgres(
                f"Path param DELETE failed (status {getattr(e.response, 'status_code', 'N/A')}), trying next style.",
                level=logging.INFO,
            )

        # 2) Try query param if PK as query parameter
        if self.pk_as_query_param:
            try:
                params = {self.primary_key: rowid}
                log_to_postgres(
                    f"DELETE query param -> {self.url} with {params}", level=logging.INFO
                )
                do_request("DELETE", self.url, headers=headers, params=params)
                return None
            except Exception as e:
                if hasattr(e, "response") and e.response is not None and e.response.status_code not in (404, 400, 500):
                    raise
                log_to_postgres(
                    f"Query param DELETE failed (status {getattr(e.response, 'status_code', 'N/A')}), trying JSON body.",
                    level=logging.INFO,
                )

        # 3) Try JSON body
        try:
            payload = {"control_environment_ids": [rowid]}
            log_to_postgres(
                f"DELETE JSON body -> {self.url} with {payload}", level=logging.INFO
            )
            do_request("DELETE", self.url, headers=headers, json=payload)
            return None
        except Exception as e:
            if hasattr(e, "response") and e.response is not None and e.response.status_code == 404:
                log_to_postgres(
                    f"Row {rowid} not found (DELETE JSON body)", level=logging.INFO
                )
                return None
            raise
