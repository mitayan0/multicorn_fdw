# multicorn_fdw/__init__.py

from .servicenow_fdw import ServiceNowFDW
from .procg_fdw import ApiFdw

__all__ = ["ServiceNowFDW", "ApiFdw"]
