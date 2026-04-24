import copy
import importlib
import logging
import pkgutil
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)

_DISCOVERY_EXCLUDED_MODULES = frozenset({
    "common",
    "orchestrator",
    "services",
    "steps",
    "__pycache__",
})


def humanize_step_name(step_name: str) -> str:
    parts = [part for part in step_name.replace("_", " ").split() if part]
    if not parts:
        return step_name
    return " ".join(part[:1].upper() + part[1:] for part in parts)


@dataclass(frozen=True)
class StepFieldDefinition:
    key: str
    field_type: str
    default: Any = ""
    label: str | None = None
    filetypes: tuple[tuple[str, str], ...] = ()
    height: int = 3
    required: bool = False

    @property
    def display_label(self) -> str:
        return self.label if self.label is not None else self.key

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "field_type": self.field_type,
            "default": copy.deepcopy(self.default),
            "label": self.label,
            "display_label": self.display_label,
            "filetypes": [list(filetype) for filetype in self.filetypes],
            "height": self.height,
            "required": self.required,
        }


@dataclass(frozen=True)
class StepDefinition:
    name: str
    handler: Callable
    aliases: tuple[str, ...] = ()
    required_keys: tuple[str, ...] = ()
    config_key: str | None = None
    display_name: str | None = None
    supports_overwrite: bool = False
    input_fields: tuple[StepFieldDefinition, ...] = ()

    @property
    def resolved_config_key(self) -> str:
        return self.config_key if self.config_key is not None else f"{self.name}_config"

    @property
    def resolved_display_name(self) -> str:
        return self.display_name if self.display_name is not None else humanize_step_name(self.name)

    @property
    def required_input_fields(self) -> tuple[StepFieldDefinition, ...]:
        return tuple(
            field
            for field in self.input_fields
            if field.required
        )

    def build_default_config(self) -> dict[str, Any]:
        return {
            field.key: copy.deepcopy(field.default)
            for field in self.input_fields
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "display_name": self.resolved_display_name,
            "aliases": list(self.aliases),
            "config_key": self.resolved_config_key,
            "required_keys": list(self.required_keys),
            "required_config_fields": [
                [self.resolved_config_key, field.key]
                for field in self.required_input_fields
            ],
            "supports_overwrite": self.supports_overwrite,
            "input_fields": [field.to_dict() for field in self.input_fields],
        }


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
    """Discover step modules and build orchestrator registries."""
    handlers: dict[str, Callable] = {}
    canonical_names: dict[str, str] = {}
    step_definitions: dict[str, StepDefinition] = {}
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
            if definition.name in step_definitions:
                raise RuntimeError(
                    f"Duplicate orchestrator step registration for canonical step '{definition.name}' from module {module_name}."
                )

            step_definitions[definition.name] = definition

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

    return (
        handlers,
        canonical_names,
        step_definitions,
        tuple(discovered_modules),
    )