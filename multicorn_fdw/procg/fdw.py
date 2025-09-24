from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres
from .api_client import RestApiClient
from .utils import normalize_items, unwrap_object, map_row, build_request

class ProcgFdw(ForeignDataWrapper):
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
        self.login_url = options.get("login_url", "http://129.146.123.215:5000/login")

        self.primary_key = options.get("primary_key")
        if self.primary_key and self.primary_key not in columns:
            raise ValueError("primary_key must exist in columns")

        # Option: PK as query parameter
        self.pk_as_query_param = (
            options.get("pk_as_query_param", "false").lower() == "true"
        )

        # Pagination
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

        self._rowid_column = self.primary_key

        # API client
        self.client = RestApiClient(
            base_url=self.url,
            username=self.username,
            password=self.password,
            login_url=self.login_url,
        )

    @property
    def rowid_column(self):
        return self._rowid_column

    # ------------------ READ ------------------
    def execute(self, quals, columns, sortkeys=None):
        filters = {}
        for q in quals:
            if q.field_name in self.columns and q.operator == "=":
                filters[q.field_name] = q.value

        if self.paginated:
            page = self.start_page
            while True:
                if self.pagination_style == "path":
                    url, params = build_request(self.url, None, False, None, page, self.limit)
                    data = self.client.fetch(url, params={**filters, **params})
                elif self.pagination_style == "params":
                    params = {"page": page, "limit": self.limit, **filters}
                    data = self.client.fetch(self.url, params=params)
                else:
                    data = self.client.fetch(self.url, params=filters)

                items = normalize_items(data)
                if not items:
                    break
                for item in items:
                    yield map_row(item, self.columns)
                if len(items) < self.limit or self.only_first_page:
                    break
                page += 1
        else:
            data = self.client.fetch(self.url, params=filters)
            items = normalize_items(data)
            for item in items:
                yield map_row(item, self.columns)

    # ------------------ INSERT ------------------
    def insert(self, new_values):
        log_to_postgres(f"Inserting: {new_values}", level=10)
        resp = self.client.request("POST", self.url, json=new_values)
        try:
            data = unwrap_object(resp.json())
            return map_row(data, self.columns)
        except Exception as e:
            log_to_postgres(f"Error parsing JSON response after INSERT: {e}", level=20)
            return new_values

    # ------------------ UPDATE ------------------
    def update(self, rowid, new_values):
        url, params = build_request(self.url, rowid, self.pk_as_query_param, self.primary_key)
        log_to_postgres(f"Updating ID '{rowid}' at {url} with values: {new_values}", level=10)
        resp = self.client.request("PUT", url, json=new_values, params=params or None)
        try:
            data = unwrap_object(resp.json())
            return map_row(data, self.columns)
        except Exception as e:
            log_to_postgres(f"Error parsing JSON response after UPDATE: {e}", level=20)
            return new_values

    # ------------------ DELETE ------------------
    def delete(self, rowid):
        # Path/query param handled via build_request
        url, params = build_request(self.url, rowid, self.pk_as_query_param, self.primary_key)
        try:
            log_to_postgres(f"DELETE -> {url} with params={params}", level=10)
            self.client.request("DELETE", url, params=params or None)
            return None
        except Exception as e:
            log_to_postgres(f"DELETE failed: {e}, trying JSON body.", level=20)

        # Fallback: JSON body style
        try:
            payload = {"control_environment_ids": [rowid]}
            log_to_postgres(f"DELETE JSON body -> {self.url} with {payload}", level=10)
            self.client.request("DELETE", self.url, json=payload)
        except Exception as e:
            log_to_postgres(f"DELETE JSON body failed: {e}", level=20)
        return None
