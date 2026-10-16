"""Process-wide setup shared by the supervisor and every job child.

Workers never open ``app.db``: settings come from the server's registration
response and the market/filings paths are fixed to the ones the server named.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from typing import Any

from src.jobs.protocol import RegisterResponse
from src.paths import logs_dir

_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_LOG_MAX_BYTES = 10 * 1024 * 1024
_LOG_BACKUP_COUNT = 5
_MARKER = "_shade_worker_handler"


def _coerce(response: RegisterResponse | dict[str, Any]) -> RegisterResponse:
    return response if isinstance(response, RegisterResponse) else RegisterResponse.model_validate(response)


def configure_logging(worker_id: str, *, child: bool = False) -> None:
    """Log to ``logs/worker-<worker_id>.log`` (rotating), in the server's log format.

    Only the supervisor rotates the file and writes to the console (and only when a
    terminal is attached, since the server redirects a launched worker's output to
    the same file). Children append, reopening the file if it was rotated.
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for handler in root.handlers[:]:
        if getattr(handler, _MARKER, False):
            root.removeHandler(handler)
            handler.close()
    directory = logs_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"worker-{worker_id}.log"
    formatter = logging.Formatter(fmt=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)
    file_handler: logging.Handler
    if child:
        # Plain append: several processes must not each try to rotate the same file.
        file_handler = (
            logging.FileHandler(path, encoding="utf-8")
            if os.name == "nt"
            else logging.handlers.WatchedFileHandler(path, encoding="utf-8")
        )
    else:
        file_handler = logging.handlers.RotatingFileHandler(
            path, maxBytes=_LOG_MAX_BYTES, backupCount=_LOG_BACKUP_COUNT, encoding="utf-8"
        )
    handlers: list[logging.Handler] = [file_handler]
    if not child and sys.stderr is not None and sys.stderr.isatty():
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        handlers.append(console)
    for handler in handlers:
        handler.setFormatter(formatter)
        setattr(handler, _MARKER, True)
        root.addHandler(handler)
    for noisy in ("chardet.charsetprober", "matplotlib", "PIL", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def install_worker_runtime(register_response: RegisterResponse | dict[str, Any], *, child: bool = False) -> None:
    """Install settings overrides, fixed database paths, and logging in this process."""
    from src.orchestrator.common.db_config import install_worker_paths
    from src.settings import install_overrides

    response = _coerce(register_response)
    install_overrides(dict(response.settings))
    install_worker_paths(response.paths.market_db, response.paths.filings_db)
    configure_logging(response.worker_id, child=child)


def uninstall_worker_runtime() -> None:
    """Undo ``install_worker_runtime`` (tests)."""
    from src.orchestrator.common.db_config import clear_worker_paths
    from src.settings import clear_overrides

    clear_overrides()
    clear_worker_paths()
    root = logging.getLogger()
    for handler in root.handlers[:]:
        if getattr(handler, _MARKER, False):
            root.removeHandler(handler)
            handler.close()
