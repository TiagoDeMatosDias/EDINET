"""Public screening package API."""

from types import ModuleType

from . import screening as _screening_module
from .common import discover_screening_modules

DISCOVERED_SCREENING_MODULES = discover_screening_modules()

for _name, _value in vars(_screening_module).items():
    if _name.startswith("__") or isinstance(_value, ModuleType):
        continue
    globals()[_name] = _value

__all__ = [name for name in globals() if not name.startswith("__")]