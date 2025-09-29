#multicorn_fdw/servicenow/fdw.py

from multicorn import ForeignDataWrapper
from .api_client import ServiceNowApiClient
from . import crud_handlers as crud


class ServiceNowFDW(ForeignDataWrapper):
    """Foreign Data Wrapper for ServiceNow Table API."""

    def __init__(self, options, columns):
        super().__init__(options, columns)
        self._columns = columns
        self.primary_key = options.get("primary_key")

        # Initialize API client
        self.api_client = ServiceNowApiClient(
            api_url=options.get("api_url"),
            username=options.get("username"),
            password=options.get("password"),
        )

    @property
    def rowid_column(self):
        return self.primary_key


    def execute(self, quals, columns):
        return crud.execute(self, quals, columns)

    def insert(self, new_values):
        return crud.insert(self, new_values)

    def update(self, rowid, new_values):
        return crud.update(self, rowid, new_values)

    def delete(self, rowid):
        return crud.delete(self, rowid)

    def get_rel_size(self, quals, columns):
        return crud.get_rel_size(self, quals, columns)
