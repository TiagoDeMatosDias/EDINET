"""Stopping the server process, for the launchers and the Admin page's shutdown."""

from __future__ import annotations

import contextlib
import logging
import multiprocessing
import os
import signal
import threading
import time

logger = logging.getLogger(__name__)

# How long uvicorn waits for open connections once it is told to stop. Browsers
# keep idle HTTPS connections open, so without a limit every shutdown, Ctrl+C
# included, waits for them to give up.
GRACEFUL_SHUTDOWN_SECONDS = 3
# Time for the shutdown response to reach the browser before the listener closes.
RESPONSE_SECONDS = 0.5
# After this long the process is ended even if something still holds it open.
FORCED_EXIT_SECONDS = 10.0

_stopping = threading.Lock()


def _stop_process() -> None:
    """Stop the server as Ctrl+C does, then make sure the process ends."""
    time.sleep(RESPONSE_SECONDS)
    reloader = multiprocessing.parent_process()
    if reloader is not None and reloader.pid:
        # Started with auto-reload: uvicorn's reloader would outlive this
        # process and start the server again on the next file change.
        with contextlib.suppress(OSError):
            os.kill(reloader.pid, signal.SIGTERM if os.name == "nt" else signal.SIGINT)
    # uvicorn stops listening, finishes the responses in flight, and runs the
    # application shutdown.
    signal.raise_signal(signal.SIGINT)
    time.sleep(FORCED_EXIT_SECONDS)
    # A running pipeline step keeps the interpreter alive after uvicorn has
    # returned. The job is marked interrupted on the next start.
    logger.warning("Server did not stop within %.0f seconds; ending the process", FORCED_EXIT_SECONDS)
    os._exit(0)


def stop_server() -> None:
    """Begin stopping the server process. Returns at once; later calls do nothing."""
    if _stopping.acquire(blocking=False):
        threading.Thread(target=_stop_process, name="server-shutdown", daemon=True).start()
