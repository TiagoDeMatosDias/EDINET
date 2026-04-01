"""Background worker utilities: thread pool, event queue, log handler."""

import logging
import queue
import threading
from concurrent.futures import ThreadPoolExecutor

# ── Shared infrastructure ───────────────────────────────────────────────
executor = ThreadPoolExecutor(max_workers=2)
event_q: queue.Queue = queue.Queue()


def run_in_background(fn, args=(), on_done=None, on_error=None):
    """Submit *fn* to the thread pool.

    Callbacks *on_done* / *on_error* are posted to ``event_q`` so the Tk main
    thread can dispatch them safely via ``poll_events``.
    """
    def _callback(fut):
        try:
            result = fut.result()
            if on_done:
                event_q.put(("done", on_done, result))
        except Exception as exc:
            if on_error:
                event_q.put(("error", on_error, exc))

    fut = executor.submit(fn, *args)
    fut.add_done_callback(_callback)
    return fut


def poll_events(root):
    """Drain ``event_q`` and invoke callbacks on the Tk main thread."""
    try:
        while True:
            _kind, callback, payload = event_q.get_nowait()
            callback(payload)
    except queue.Empty:
        pass
    root.after(100, poll_events, root)


# ── Log handler that feeds a queue for the UI ───────────────────────────

class QueueLogHandler(logging.Handler):
    """Logging handler that posts formatted records onto a ``queue.Queue``.

    Each item is a tuple ``("log", level_name, formatted_message)``.
    """

    def __init__(self, log_queue: queue.Queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_queue.put(("log", record.levelname, msg))
        except Exception:
            self.handleError(record)
