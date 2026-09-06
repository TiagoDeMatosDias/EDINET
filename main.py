"""Shade Research launcher — starts the web workstation server."""

import argparse
import logging
import os
import sys


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
    """
    import uvicorn

    from src.utilities.logger import setup_logging
    from src.web_app.security import AppSettings
    from src.web_app.tls import provision_tls

    settings = AppSettings.from_env(
        host=host,
        port=port,
        allow_remote=allow_remote,
    )
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


def _parse_args() -> argparse.Namespace:
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
            "Allow a non-loopback bind. Requires EDINET_AUTH_MODE=accounts "
            "and EDINET_TRUSTED_HOSTS. HTTPS is always served with the "
            "certificate in data/certs/ (auto-generated when missing)."
        ),
    )
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable auto-reload.",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    _run_web(
        host=args.host,
        port=args.port,
        reload=not args.no_reload,
        allow_remote=args.allow_remote,
    )
