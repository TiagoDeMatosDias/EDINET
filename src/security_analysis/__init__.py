"""Public security analysis package API."""

from types import ModuleType

from . import security_analysis as _security_analysis_module
from .common import discover_security_analysis_modules

DISCOVERED_SECURITY_ANALYSIS_MODULES = discover_security_analysis_modules()

for _name, _value in vars(_security_analysis_module).items():
    if _name.startswith("__") or isinstance(_value, ModuleType):
        continue
    globals()[_name] = _value

__all__ = [name for name in globals() if not name.startswith("__")]