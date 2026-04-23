import importlib
import logging
import pkgutil
from dataclasses import dataclass
from typing import Callable

logger = logging.getLogger(__name__)

_DISCOVERY_EXCLUDED_MODULES = frozenset({
    "common",
    "orchestrator",
    "services",
    "steps",
    "__pycache__",
})


@dataclass(frozen=True)
class StepDefinition:
    name: str
    handler: Callable
    aliases: tuple[str, ...] = ()
    required_keys: tuple[str, ...] = ()
    required_config_fields: tuple[tuple[str, str], ...] = ()


def _read_step_definitions(module) -> list[StepDefinition]:
    definitions = getattr(module, "STEP_DEFINITIONS", None)
    if definitions is not None:
        return list(definitions)

    single_definition = getattr(module, "STEP_DEFINITION", None)
    if single_definition is None:
        return []
    return [single_definition]


def iter_step_modules(package_name: str = "src.orchestrator"):
    importlib.invalidate_caches()
    orchestrator_package = importlib.import_module(package_name)
    module_infos = sorted(pkgutil.iter_modules(orchestrator_package.__path__), key=lambda info: info.name)

    for module_info in module_infos:
        module_name = module_info.name
        if module_name.startswith("_") or module_name in _DISCOVERY_EXCLUDED_MODULES:
            continue

        dotted_module_name = f"{package_name}.{module_name}"
        try:
            module = importlib.import_module(dotted_module_name)
        except Exception as exc:
            logger.warning("Failed to import orchestrator step module %s: %s", dotted_module_name, exc)
            continue

        definitions = _read_step_definitions(module)
        if definitions:
            yield dotted_module_name, definitions


def build_step_registry(package_name: str = "src.orchestrator"):
    """Discover step modules and build handler and validation registries."""
    handlers: dict[str, Callable] = {}
    required_keys: dict[str, list[str]] = {}
    required_config_fields: dict[str, list[tuple[str, str]]] = {}
    canonical_names: dict[str, str] = {}
    discovered_modules: list[str] = []

    for module_name, definitions in iter_step_modules(package_name=package_name):
        module_registered = False
        for definition in definitions:
            if not isinstance(definition, StepDefinition):
                logger.warning(
                    "Skipping invalid step definition %r from module %s.",
                    definition,
                    module_name,
                )
                continue
            if not callable(definition.handler):
                logger.warning(
                    "Skipping step '%s' from module %s because its handler is not callable.",
                    definition.name,
                    module_name,
                )
                continue

            required_keys[definition.name] = list(definition.required_keys)
            required_config_fields[definition.name] = list(definition.required_config_fields)

            for step_name in (definition.name, *definition.aliases):
                if step_name in handlers:
                    raise RuntimeError(
                        f"Duplicate orchestrator step registration for '{step_name}' from module {module_name}."
                    )
                handlers[step_name] = definition.handler
                canonical_names[step_name] = definition.name

            module_registered = True

        if module_registered:
            discovered_modules.append(module_name)

    return handlers, required_keys, required_config_fields, canonical_names, tuple(discovered_modules)