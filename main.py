"""Shade Research launcher.

``main.py`` starts the web workstation. ``main.py config`` reads and changes
the settings stored in ``app.db``; ``main.py migrate`` moves an installation
from the old storage layout (it also runs automatically on start).
"""

import argparse
import getpass
import logging
import os
import sys

# Environment variables earlier versions read, and what replaced them.
_RETIRED_ENVIRONMENT = {
    "API_KEY": "the edinet.api_key setting",
    "EDINET_API_TOKEN": "the edinet.api_key setting",
    "EDINET_AUTH_MODE": "the auth.mode setting",
    "EDINET_REGISTRATION_MODE": "the registration mode on the Admin page",
    "EDINET_TRUSTED_HOSTS": "the server.trusted_hosts setting",
    "EDINET_ALLOWED_DATA_ROOTS": "the pipeline.allowed_data_roots setting",
    "EDINET_MAX_UPLOAD_BYTES": "the limits.max_upload_bytes setting",
    "EDINET_MAX_EXPORT_BYTES": "the limits.max_export_bytes setting",
    "EDINET_MAX_BACKTEST_ARTIFACT_BYTES": "the limits.max_backtest_artifact_bytes setting",
    "EDINET_MAX_REPORT_ARTIFACT_BYTES": "the limits.max_report_artifact_bytes setting",
    "EDINET_JOB_RETENTION_HOURS": "the jobs.retention_hours setting",
    "EDINET_STATE_DIR": "EDINET_DATA_DIR",
    "EDINET_AUTH_DB": "EDINET_DATA_DIR",
    "EDINET_RESEARCH_DB": "EDINET_DATA_DIR",
    "EDINET_CHAT_DB": "EDINET_DATA_DIR",
    "EDINET_FILINGS_DB": "the storage.filings_db_path setting",
    "EDINET_CHAT_KEYS": "the key ring stored in app.db",
    "EDINET_CERT_DIR": "EDINET_DATA_DIR",
    "EDINET_BACKTEST_DIR": "EDINET_DATA_DIR",
    "EDINET_REPORT_DIR": "EDINET_DATA_DIR",
    "EDINET_JOB_WORKSPACE_ROOT": "EDINET_DATA_DIR",
    "EDINET_SQLITE_BUSY_TIMEOUT_MS": "a fixed 30 second timeout",
    "EDINET_APP_TOKEN": "personal API tokens on the Account page",
}


def _warn_about_retired_environment() -> None:
    for name, replacement in _RETIRED_ENVIRONMENT.items():
        if os.getenv(name):
            print(
                f"Warning: {name} is no longer read; use {replacement} instead "
                "(python main.py config --help).",
                file=sys.stderr,
            )


def _migrate(dry_run: bool = False) -> None:
    """Move an old installation into the data folder before anything opens it."""
    from src.orchestrator.common.migrate_layout import migrate_legacy_layout

    migrate_legacy_layout(dry_run=dry_run)


def _open_browser_when_ready(host: str, port: int) -> None:
    """Open the workstation in the default browser once the server accepts connections.

    A packaged build is started by double-clicking, with no terminal command
    to read the address from.
    """
    import socket
    import threading
    import time
    import webbrowser

    def wait_then_open() -> None:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                with socket.create_connection((host, port), timeout=1):
                    break
            except OSError:
                time.sleep(0.5)
        else:
            return
        try:
            webbrowser.open(f"https://{host}:{port}/")
        except Exception:  # noqa: BLE001 - no browser available; the log still shows the address
            pass

    threading.Thread(target=wait_then_open, daemon=True).start()


def _run_web(
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = True,
    allow_remote: bool = False,
) -> None:
    """Launch the web workstation server.

    Args:
        host: Host interface for the web server.
        port: Port for the web server.
        reload: Enable auto-reload for development. Forced to ``False``
                in frozen PyInstaller builds.
        allow_remote: Permit a non-loopback ``host``.
    """
    _migrate()
    _warn_about_retired_environment()

    import uvicorn

    from src.utilities.logger import setup_logging
    from src.web_app.security import AppSettings
    from src.web_app.tls import provision_tls

    settings = AppSettings.load(
        host=host,
        port=port,
        allow_remote=allow_remote,
    )
    # uvicorn may serve from a separate process; hand it the launch options.
    os.environ["EDINET_HOST"] = settings.host
    os.environ["EDINET_PORT"] = str(settings.port)
    os.environ["EDINET_ALLOW_REMOTE"] = str(settings.allow_remote).lower()

    setup_logging()
    logger = logging.getLogger(__name__)

    # PyInstaller one-file exe: reload spawns a child process that inherits
    # internal multiprocessing args, breaking argparse.
    if getattr(sys, "frozen", False):
        reload = False

    cert_path, key_path = provision_tls(host=settings.host)

    if getattr(sys, "frozen", False) and not os.getenv("EDINET_NO_BROWSER"):
        _open_browser_when_ready("127.0.0.1" if settings.host in ("0.0.0.0", "::") else settings.host, settings.port)

    logger.info(
        "Starting web workstation on https://%s:%s",
        settings.host,
        settings.port,
    )

    uvicorn.run(
        "src.web_app.server:app",
        host=settings.host,
        port=settings.port,
        reload=reload,
        ssl_certfile=cert_path,
        ssl_keyfile=key_path,
    )


def _format_value(item: dict) -> str:
    if item["kind"] == "secret":
        return "(set)" if item["is_set"] else "(not set)"
    value = item["value"]
    if isinstance(value, list):
        return ", ".join(value) or "(none)"
    return str(value) if value not in ("", None) else "(not set)"


def _run_config(args: argparse.Namespace) -> int:
    """Read or change the settings stored in app.db."""
    _migrate()
    from src.settings import (
        SettingError,
        describe_settings,
        parse_text,
        set_setting,
        spec_for,
        unset_setting,
    )

    try:
        if args.config_command in (None, "list"):
            for item in describe_settings():
                restart = "  [applies after restart]" if item["restart_required"] else ""
                print(f"{item['key']} = {_format_value(item)}{restart}")
                print(f"    {item['description']}")
            return 0
        spec = spec_for(args.key)
        if args.config_command == "get":
            item = next(entry for entry in describe_settings() if entry["key"] == spec.key)
            print(_format_value(item))
            return 0
        if args.config_command == "unset":
            unset_setting(spec.key)
            print(f"{spec.key} is back to its default.")
            return 0
        text = args.value
        if text is None:
            text = getpass.getpass(f"{spec.label}: ") if spec.secret else input(f"{spec.label}: ")
        set_setting(spec.key, parse_text(spec, text), updated_by="command line")
        note = " Restart the server to apply it." if spec.restart else ""
        print(f"{spec.key} saved.{note}")
        return 0
    except SettingError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed argparse namespace.
    """
    parser = argparse.ArgumentParser(description="Shade Research launcher")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host for the web server (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for the web server (default: 8000).",
    )
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help=(
            "Allow a non-loopback bind. Requires the auth.mode setting to be "
            "accounts and the server.trusted_hosts setting. HTTPS is always "
            "served with the certificate in data/certs/ (auto-generated when missing)."
        ),
    )
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable auto-reload.",
    )
    commands = parser.add_subparsers(dest="command")

    config = commands.add_parser("config", help="Show or change the settings stored in app.db.")
    config_commands = config.add_subparsers(dest="config_command")
    config_commands.add_parser("list", help="Show every setting (the default).")
    get = config_commands.add_parser("get", help="Show one setting.")
    get.add_argument("key")
    set_ = config_commands.add_parser(
        "set",
        help="Change one setting. Lists are comma-separated. Leave out the value to be prompted.",
    )
    set_.add_argument("key")
    set_.add_argument("value", nargs="?")
    unset = config_commands.add_parser("unset", help="Return one setting to its default.")
    unset.add_argument("key")

    migrate = commands.add_parser("migrate", help="Move an old installation into the data folder.")
    migrate.add_argument("--dry-run", action="store_true", help="List the steps without taking them.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        if args.command == "config":
            return _run_config(args)
        if args.command == "migrate":
            from src.orchestrator.common.migrate_layout import migrate_legacy_layout

            if not migrate_legacy_layout(dry_run=args.dry_run):
                print("Nothing to migrate: the data folder already uses the current layout.")
            return 0
        _run_web(
            host=args.host,
            port=args.port,
            reload=not args.no_reload,
            allow_remote=args.allow_remote,
        )
        return 0
    except Exception as exc:  # noqa: BLE001 - report configuration problems without a traceback
        from src.orchestrator.common.migrate_layout import LegacyLayoutError
        from src.settings import SettingError
        from src.web_app.security import SecurityConfigurationError

        if isinstance(exc, (LegacyLayoutError, SecurityConfigurationError, SettingError)):
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        raise


def _hold_window_open() -> None:
    """Keep a double-clicked console window readable after a failure."""
    if getattr(sys, "frozen", False) and os.name == "nt" and sys.stdin and sys.stdin.isatty():
        input("Press Enter to close this window...")


if __name__ == '__main__':
    try:
        code = main()
    except Exception:
        import traceback

        traceback.print_exc()
        _hold_window_open()
        raise SystemExit(1)
    if code:
        _hold_window_open()
    raise SystemExit(code)
