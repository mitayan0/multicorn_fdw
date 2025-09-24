# multicorn_fdw/__init__.py

from .servicenow.fdw import ServiceNowFDW
from .procg.fdw import ProcgFdw

__all__ = ["ServiceNowFDW", "ProcgFdw"]

