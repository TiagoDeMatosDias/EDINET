"""In-process change notifications for the job service.

``bump()`` is called from any thread whenever a job changes; waiters in
asyncio code and in plain threads are woken without polling. Cross-process
changes are not seen here, which is why waiters also re-check ``jobs.db``
at least once a second.
"""

from __future__ import annotations

import asyncio
import threading
import time


class JobNotifier:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._version = 0
        self._async_waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def bump(self) -> None:
        with self._lock:
            self._version += 1
            waiters, self._async_waiters = self._async_waiters, []
            self._condition.notify_all()
        for loop, future in waiters:
            try:
                loop.call_soon_threadsafe(_resolve, future)
            except RuntimeError:  # the waiter's loop is closed
                pass

    def wait_for_change_sync(self, since: int, timeout: float) -> int:
        """Block until the version differs from ``since`` or ``timeout`` passes."""
        deadline = time.monotonic() + max(0.0, timeout)
        with self._condition:
            while self._version == since:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)
            return self._version

    async def wait_for_change(self, since: int, timeout: float) -> int:
        """Await a version different from ``since``, or return after ``timeout``."""
        loop = asyncio.get_running_loop()
        with self._lock:
            if self._version != since:
                return self._version
            future: asyncio.Future[None] = loop.create_future()
            entry = (loop, future)
            self._async_waiters.append(entry)
        try:
            await asyncio.wait_for(future, max(0.0, timeout))
        except asyncio.TimeoutError:
            pass
        finally:
            with self._lock:
                if entry in self._async_waiters:
                    self._async_waiters.remove(entry)
        return self.version


def _resolve(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


__all__ = ["JobNotifier"]
