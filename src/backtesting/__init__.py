"""Public backtesting package API."""

from types import ModuleType

from . import backtesting as _backtesting_module
from .common import discover_backtesting_modules

DISCOVERED_BACKTESTING_MODULES = discover_backtesting_modules()

for _name, _value in vars(_backtesting_module).items():
    if _name.startswith("__") or isinstance(_value, ModuleType):
        continue
    globals()[_name] = _value

__all__ = [name for name in globals() if not name.startswith("__")]
