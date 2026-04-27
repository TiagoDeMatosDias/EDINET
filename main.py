import sys
import threading
import logging
import argparse


def _run_gui(
    start_api_server: bool = True,
    api_host: str = "127.0.0.1",
    api_port: int = 8000,
) -> None:
    """Launch the Tkinter terminal-style GUI.

    Args:
        start_api_server: Whether to start the HTTP API server in background.
        api_host: Host for the API server (default: localhost).
        api_port: Port for the API server (default: 8000).
    """
    from src.utilities.logger import setup_logging
    
    # Setup logging before importing uvicorn to avoid conflicts
    setup_logging()
    logger = logging.getLogger(__name__)

    if start_api_server:
        logger.info("Starting HTTP API server in background...")
        try:
            from src.api.server import main as api_main
            
            # Parse arguments for the API server
            import argparse
            parser = argparse.ArgumentParser(add_help=False)
            parser.add_argument('--host', default=api_host)
            parser.add_argument('--port', type=int, default=api_port)
            args = parser.parse_args([])
            
            # Start API server in a background thread
            api_thread = threading.Thread(
                target=_start_api_server,
                args=(args.host, args.port),
                daemon=True  # Thread exits when main program exits
            )
            api_thread.start()
            logger.info(f"API server started on http://{api_host}:{api_port}")
        except Exception as e:
            logger.warning(
                f"Failed to start API server: {e}. "
                f"GUI will continue without API access."
            )
    
    # Launch the Tkinter GUI
    from ui_tk import run_tk_app
    run_tk_app()


def _start_api_server(host: str, port: int) -> None:
    """Start the FastAPI server in a background thread.

    This function runs the API server and keeps it alive until
    the main program exits or an error occurs.
    """
    import uvicorn
    from src.api.router import app
    
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="warning",
        access_log=False,  # Suppress default access logs
        reload=False,       # Disable auto-reload in background thread
    )
    server = uvicorn.Server(config)
    
    try:
        server.run()
    except Exception as e:
        logging.getLogger(__name__).error(f"API server error: {e}")


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
    """Parse command-line arguments for UI/server mode selection.

    Returns:
        Parsed argparse namespace.
    """
    parser = argparse.ArgumentParser(description="EDINET launcher")
    parser.add_argument(
        "--web",
        action="store_true",
        help="Run the web workstation instead of the Tk desktop UI.",
    )
    parser.add_argument(
        "--no-api",
        action="store_true",
        help="Tk mode only: do not start the API server in the background.",
    )
    parser.add_argument(
        "--api-host",
        default="127.0.0.1",
        help="Host for the API server (Tk mode) or web server (--web mode).",
    )
    parser.add_argument(
        "--api-port",
        type=int,
        default=8000,
        help="Port for the API server (Tk mode) or web server (--web mode).",
    )
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Web mode only: disable auto-reload.",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()

    if args.web:
        _run_web(host=args.api_host, port=args.api_port, reload=not args.no_reload)
    else:
        _run_gui(
            start_api_server=not args.no_api,
            api_host=args.api_host,
            api_port=args.api_port,
        )
