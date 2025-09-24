# multicorn_fdw/__init__.py

from .servicenow.fdw import ServiceNowFDW
from .procg.fdw import ApiFdw

__all__ = ["ServiceNowFDW", "ApiFdw"]

