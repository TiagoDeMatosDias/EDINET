import importlib
import pkgutil

_DISCOVERY_EXCLUDED_MODULES = frozenset({"common", "__pycache__"})


def discover_security_analysis_modules(
    package_name: str = "src.security_analysis",
) -> tuple[str, ...]:
    """Return importable module paths under the security analysis package."""
    package = importlib.import_module(package_name)
    module_names: list[str] = []
    for module_info in sorted(pkgutil.iter_modules(package.__path__), key=lambda info: info.name):
        if module_info.name.startswith("_") or module_info.name in _DISCOVERY_EXCLUDED_MODULES:
            continue
        module_names.append(f"{package_name}.{module_info.name}")
    return tuple(module_names)