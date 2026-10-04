"""Wakes long-polling chat clients when a message is stored.

Writes happen in FastAPI's worker threads; waiters are coroutines on the
event loop. ``notify`` is thread-safe and sets the event every current waiter
holds, then replaces it for the next round.
"""

from __future__ import annotations

import asyncio
import threading


class ChatNotifier:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._event: asyncio.Event | None = None

    def _current(self) -> asyncio.Event:
        loop = asyncio.get_running_loop()
        with self._lock:
            # A new loop (another server or test client) starts a fresh event.
            if self._loop is not loop or self._event is None:
                self._loop, self._event = loop, asyncio.Event()
            return self._event

    def _wake(self) -> None:
        with self._lock:
            event, self._event = self._event, asyncio.Event()
        if event is not None:
            event.set()

    def notify(self) -> None:
        with self._lock:
            loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(self._wake)
        except RuntimeError:  # the loop closed between the check and the call
            pass

    def waiter(self) -> asyncio.Event:
        """Take the event before checking for news, so a write in between still wakes the caller."""
        return self._current()

    @staticmethod
    async def wait(event: asyncio.Event, timeout: float) -> bool:
        try:
            await asyncio.wait_for(event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False


notifier = ChatNotifier()
