"""EDINET launcher — starts the web workstation server."""

import argparse
import logging


def _run_web(host: str = "127.0.0.1", port: int = 8000, reload: bool = True) -> None:
    """Launch the web workstation server.

    Args:
        host: Host interface for the web server.
        port: Port for the web server.
        reload: Enable auto-reload for development.
    """
    from src.utilities.logger import setup_logging
    import uvicorn

    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting web workstation on http://%s:%s", host, port)

    uvicorn.run("src.web_app.server:app", host=host, port=port, reload=reload)


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed argparse namespace.
    """
    parser = argparse.ArgumentParser(description="EDINET launcher")
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
        "--no-reload",
        action="store_true",
        help="Disable auto-reload.",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    _run_web(host=args.host, port=args.port, reload=not args.no_reload)
