"""Public utilities package API and module discovery helpers."""

import importlib
import pkgutil
from types import ModuleType


def discover_utility_modules(package_name: str = "src.utilities") -> tuple[str, ...]:
    """Return importable utility module paths under the utilities package."""
    package = importlib.import_module(package_name)
    module_names: list[str] = []
    for module_info in sorted(pkgutil.iter_modules(package.__path__), key=lambda info: info.name):
        if module_info.name.startswith("_") or module_info.name == "__pycache__":
            continue
        module_names.append(f"{package_name}.{module_info.name}")
    return tuple(module_names)


DISCOVERED_UTILITY_MODULES = discover_utility_modules()

for _module_name in ("logger", "stock_prices", "utils"):
    _module = importlib.import_module(f"{__name__}.{_module_name}")
    globals()[_module_name] = _module
    for _name, _value in vars(_module).items():
        if _name.startswith("__") or isinstance(_value, ModuleType):
            continue
        globals().setdefault(_name, _value)

__all__ = [name for name in globals() if not name.startswith("__")]