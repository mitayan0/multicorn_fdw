# multicorn_fdw/__init__.py

from .servicenow_fdw.fdw import ServiceNowFDW
from .procg_fdw.fdw import ApiFdw

__all__ = ["ServiceNowFDW", "ApiFdw"]
