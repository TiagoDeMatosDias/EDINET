"""Every operator setting: its type, default, and validation.

Settings are rows in ``app.db`` (see ``store``). They are edited on the Admin
page or with ``python main.py config set KEY VALUE``; nothing is read from
environment variables or configuration files. How the server listens (host,
port, remote access) is a launch option instead, so a stored value can never
expose the server to the network.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

MiB = 1024 * 1024


class SettingError(ValueError):
    """A setting key is unknown or a value does not fit its setting."""


@dataclass(frozen=True)
class SettingSpec:
    key: str
    kind: str  # "text", "secret", "choice", "integer", "list", "path", or "internal"
    default: Any
    label: str
    description: str
    choices: tuple[str, ...] = ()
    minimum: int | None = None
    restart: bool = False

    @property
    def secret(self) -> bool:
        return self.kind in {"secret", "internal"}

    @property
    def editable(self) -> bool:
        return self.kind != "internal"


SETTINGS: tuple[SettingSpec, ...] = (
    SettingSpec(
        "edinet.api_key",
        "secret",
        "",
        "EDINET API key",
        "Subscription key for the EDINET API, used by every step that downloads documents or filings.",
    ),
    SettingSpec(
        "auth.mode",
        "choice",
        "accounts",
        "Sign-in",
        "accounts: everyone signs in. disabled: anyone who can reach the server acts as an "
        "administrator; only for a server bound to this machine.",
        choices=("accounts", "disabled"),
        restart=True,
    ),
    SettingSpec(
        "server.trusted_hosts",
        "list",
        [],
        "Trusted host names",
        "Host names browsers may use to reach the server when it is started with --allow-remote.",
        restart=True,
    ),
    SettingSpec(
        "pipeline.allowed_data_roots",
        "list",
        [],
        "Extra pipeline input folders",
        "Folders pipeline steps may read input files from, besides the data folder.",
        restart=True,
    ),
    SettingSpec(
        "limits.max_upload_bytes",
        "integer",
        10 * MiB,
        "Upload limit (bytes)",
        "Largest file accepted by an upload, other than pipeline inputs.",
        minimum=1,
        restart=True,
    ),
    SettingSpec(
        "limits.max_export_bytes",
        "integer",
        25 * MiB,
        "Export limit (bytes)",
        "Largest screening or portfolio export.",
        minimum=1,
        restart=True,
    ),
    SettingSpec(
        "limits.max_backtest_artifact_bytes",
        "integer",
        256 * MiB,
        "Backtest archive limit (bytes)",
        "Largest backtest archive the server generates.",
        minimum=1,
        restart=True,
    ),
    SettingSpec(
        "limits.max_report_artifact_bytes",
        "integer",
        128 * MiB,
        "Report archive limit (bytes)",
        "Largest report archive the server generates.",
        minimum=1,
        restart=True,
    ),
    SettingSpec(
        "jobs.retention_hours",
        "integer",
        24,
        "Job retention (hours)",
        "Finished pipeline jobs and their uploaded files are removed after this long.",
        minimum=1,
        restart=True,
    ),
    SettingSpec(
        "storage.market_db_path",
        "path",
        "",
        "Market database location",
        "Path of market.db. Empty keeps it in the data folder. Move the file yourself, "
        "with the server stopped, before changing this.",
        restart=True,
    ),
    SettingSpec(
        "storage.filings_db_path",
        "path",
        "",
        "Filing archive location",
        "Path of filings.db, for example on a larger disk. Empty keeps it in the data folder. "
        "Move the file yourself, with the server stopped, before changing this.",
        restart=True,
    ),
    SettingSpec(
        "chat.message_keys",
        "internal",
        None,
        "Chat key ring",
        "Keys that encrypt channel messages at rest; created on first use.",
    ),
)

_BY_KEY = {spec.key: spec for spec in SETTINGS}


def spec_for(key: str) -> SettingSpec:
    try:
        return _BY_KEY[key]
    except KeyError:
        raise SettingError(f"Unknown setting {key!r}") from None


def validate(spec: SettingSpec, value: Any) -> Any:
    """Return ``value`` normalized for ``spec``, or raise ``SettingError``."""
    if spec.kind == "internal":
        return value
    if spec.kind in {"text", "secret", "path"}:
        if not isinstance(value, str):
            raise SettingError(f"{spec.key} must be text")
        return value.strip()
    if spec.kind == "choice":
        normalized = str(value).strip().casefold()
        if normalized not in spec.choices:
            raise SettingError(f"{spec.key} must be one of: {', '.join(spec.choices)}")
        return normalized
    if spec.kind == "integer":
        if isinstance(value, bool):
            raise SettingError(f"{spec.key} must be a whole number")
        try:
            number = int(value)
        except (TypeError, ValueError):
            raise SettingError(f"{spec.key} must be a whole number") from None
        if spec.minimum is not None and number < spec.minimum:
            raise SettingError(f"{spec.key} must be at least {spec.minimum}")
        return number
    if spec.kind == "list":
        if isinstance(value, str):
            value = value.split(",")
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise SettingError(f"{spec.key} must be a list of text values")
        return [item.strip() for item in value if item.strip()]
    raise SettingError(f"{spec.key} has unsupported kind {spec.kind}")


def parse_text(spec: SettingSpec, text: str) -> Any:
    """Turn a command-line value into a setting value (lists are comma-separated)."""
    if spec.kind == "internal":
        raise SettingError(f"{spec.key} is managed by the application")
    return validate(spec, text)
