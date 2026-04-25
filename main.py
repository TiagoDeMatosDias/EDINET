import sys
import threading
import logging
from typing import Optional


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


def _parse_args() -> tuple[bool, str, int]:
    """Parse command-line arguments for API server configuration.

    Returns:
        Tuple of (start_api_server, host, port)
    """
    start_api = '--no-api' not in sys.argv
    api_host = '127.0.0.1'
    api_port = 8000
    
    for arg in sys.argv:
        if arg.startswith('--api-host='):
            api_host = arg.split('=', 1)[1]
        elif arg.startswith('--api-port='):
            try:
                api_port = int(arg.split('=', 1)[1])
            except ValueError:
                pass
    
    return start_api, api_host, api_port


if __name__ == '__main__':
    # Parse API server arguments
    start_api, api_host, api_port = _parse_args()
    
    # Launch the GUI (CLI mode is no longer supported)
    _run_gui(
        start_api_server=start_api,
        api_host=api_host,
        api_port=api_port
    )
