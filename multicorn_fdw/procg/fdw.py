#multicorn_fdw/procg/fdw.py
from multicorn import ForeignDataWrapper
from .api_client import RestApiClient
from . import crud_handlers as crud


class ProcgFdw(ForeignDataWrapper):
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

        # Allow configuring delete JSON body key; default keeps current behavior.
        self.delete_paylod = options.get("delete_payload", "control_environment_ids")

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

    # Delegate CRUD methods
    def execute(self, quals, columns, sortkeys=None):
        return crud.execute(self, quals, columns, sortkeys)

    def insert(self, new_values):
        return crud.insert(self, new_values)

    def update(self, rowid, new_values):
        return crud.update(self, rowid, new_values)

    def delete(self, rowid):
        return crud.delete(self, rowid)
