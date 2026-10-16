"""Starts, watches, and restarts the worker process that runs on the server's machine.

The server hands the worker its credentials on stdin (never on the command
line or in the environment) and restarts it with a growing delay if it exits.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

from src.paths import app_dir, logs_dir

from .protocol import LOCAL_WORKER_ID
from .registry import WorkerRegistry

logger = logging.getLogger(__name__)

BACKOFF_SECONDS: tuple[float, ...] = (1, 2, 4, 8, 16, 32, 60)
STABLE_AFTER_SECONDS = 300.0
STOP_TIMEOUT_SECONDS = 10.0
LOG_NAME = "worker-local.log"


def default_command() -> list[str]:
    """The command that starts a worker, or ``FileNotFoundError`` when it is not installed."""
    if getattr(sys, "frozen", False):
        executable = app_dir() / ("ShadeWorker.exe" if os.name == "nt" else "ShadeWorker")
        if not executable.is_file():
            raise FileNotFoundError(str(executable))
        return [str(executable), "run", "--stdin-credentials"]
    script = app_dir() / "shade_worker.py"
    if not script.is_file():
        raise FileNotFoundError(str(script))
    return [sys.executable, str(script), "run", "--stdin-credentials"]


def default_server_url(settings: Any) -> str:
    """The address workers on this machine use to reach the server."""
    host = str(getattr(settings, "host", "127.0.0.1")).strip().strip("[]")
    if host in {"", "0.0.0.0", "localhost"}:
        host = "127.0.0.1"
    elif host == "::":
        host = "::1"
    shown = f"[{host}]" if ":" in host else host
    return f"https://{shown}:{settings.port}"


def default_fingerprint() -> str:
    """SHA-256 fingerprint of the certificate the server presents (empty before one exists)."""
    from src.web_app.tls import certificate_fingerprint, server_certificate_path

    certificate = server_certificate_path()
    return certificate_fingerprint(certificate) if certificate else ""


class LocalWorkerLauncher:
    def __init__(
        self,
        registry: WorkerRegistry,
        *,
        server_url: str | Callable[[], str],
        fingerprint: str | Callable[[], str] = default_fingerprint,
        command: Callable[[], list[str]] | None = None,
        launch_local: str | Callable[[], str] | None = None,
        log_path: Path | None = None,
        backoff_seconds: tuple[float, ...] | None = None,
        stable_after_seconds: float = STABLE_AFTER_SECONDS,
        stop_timeout_seconds: float = STOP_TIMEOUT_SECONDS,
    ) -> None:
        self.registry = registry
        self._server_url = server_url
        self._fingerprint = fingerprint
        self._command = command or default_command
        self._launch_local = launch_local
        self._log_path = log_path
        self._backoff = tuple(backoff_seconds or BACKOFF_SECONDS)
        self._stable_after = stable_after_seconds
        self._stop_timeout = stop_timeout_seconds
        self._lock = threading.RLock()
        self._stopping = threading.Event()
        self._thread: threading.Thread | None = None
        self._process: subprocess.Popen[bytes] | None = None
        self._state = "stopped"
        self._restarts = 0
        self._last_exit_code: int | None = None
        self._last_command: list[str] | None = None
        self._missing_logged = False

    # -- public --------------------------------------------------------------

    def start(self) -> None:
        """Start the worker and its monitor unless ``workers.launch_local`` is off."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            if self._setting() == "off":
                self._state = "disabled"
                logger.info("The local worker is disabled (workers.launch_local = off)")
                return
            self._stopping.clear()
            self._state = "stopped"
            self._thread = threading.Thread(target=self._monitor, name="local-worker", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """Terminate the worker (kill after the timeout) and end the monitor."""
        with self._lock:
            thread, self._thread = self._thread, None
            self._stopping.set()
        self._terminate()
        if thread is not None:
            thread.join(timeout=self._stop_timeout + 5)
        with self._lock:
            if self._state != "disabled":
                self._state = "stopped"

    def stopping_soon(self) -> None:
        """The server is stopping: the worker's own exit is expected and not restarted.

        A Ctrl+C or a signal to the process group reaches the worker too. This only
        sets a flag, so a signal handler may call it; ``stop`` still ends the worker.
        """
        self._stopping.set()

    def restart(self) -> dict[str, Any]:
        self.stop()
        self.start()
        return self.status()

    def status(self) -> dict[str, Any]:
        with self._lock:
            process = self._process
            running = process is not None and process.poll() is None
            return {
                "state": self._state,
                "pid": process.pid if running and process is not None else None,
                "restarts": self._restarts,
                "last_exit_code": self._last_exit_code,
                "command": list(self._last_command) if self._last_command else None,
            }

    # -- internals -----------------------------------------------------------

    def _setting(self) -> str:
        if callable(self._launch_local):
            return self._launch_local()
        if self._launch_local is not None:
            return self._launch_local
        from src.settings import get_setting

        return str(get_setting("workers.launch_local"))

    def _credentials(self) -> str:
        token = self.registry.ensure_local_worker(LOCAL_WORKER_ID)
        url = self._server_url() if callable(self._server_url) else self._server_url
        fingerprint = self._fingerprint() if callable(self._fingerprint) else self._fingerprint
        return json.dumps(
            {
                "server_url": url,
                "fingerprint": fingerprint,
                "worker_id": LOCAL_WORKER_ID,
                "token": token,
                "parent_pid": os.getpid(),
            },
            separators=(",", ":"),
        ) + "\n"

    def _spawn(self) -> subprocess.Popen[bytes]:
        command = self._command()
        self._last_command = list(command)
        environment = dict(os.environ)
        if getattr(sys, "frozen", False):
            environment["PYINSTALLER_RESET_ENVIRONMENT"] = "1"
        log_path = self._log_path or (logs_dir() / LOG_NAME)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) if os.name == "nt" else 0
        with log_path.open("ab") as log:
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=log,
                stderr=log,
                env=environment,
                creationflags=flags,
            )
        try:
            assert process.stdin is not None
            process.stdin.write(self._credentials().encode("utf-8"))
            process.stdin.close()
        except OSError:
            logger.warning("The local worker closed its input before reading credentials")
        return process

    def _monitor(self) -> None:
        delay_index = 0
        while not self._stopping.is_set():
            started = time.monotonic()
            try:
                process = self._spawn()
            except (FileNotFoundError, NotADirectoryError, PermissionError) as exc:
                with self._lock:
                    self._state = "missing"
                if not self._missing_logged:
                    self._missing_logged = True
                    logger.warning("The local worker cannot be started (%s not found)", exc)
                return
            except Exception:
                logger.exception("Starting the local worker failed")
                process = None
            if process is not None:
                with self._lock:
                    self._process = process
                    self._state = "running"
                logger.info("Started the local worker (pid %s)", process.pid)
                if self._stopping.is_set():  # stop() raced with the spawn
                    self._terminate()
                    return
                while process.poll() is None and not self._stopping.is_set():
                    try:
                        process.wait(timeout=0.2)
                    except subprocess.TimeoutExpired:
                        pass
                if self._stopping.is_set():
                    return
                with self._lock:
                    self._last_exit_code = process.returncode
                logger.warning("The local worker exited with code %s", process.returncode)
                if time.monotonic() - started >= self._stable_after:
                    delay_index = 0
            delay = self._backoff[min(delay_index, len(self._backoff) - 1)]
            delay_index += 1
            with self._lock:
                self._state = "backoff"
                self._restarts += 1
            if self._stopping.wait(delay):
                return

    def _terminate(self) -> None:
        with self._lock:
            process, self._process = self._process, None
        if process is None or process.poll() is not None:
            return
        _ask_to_stop(process)
        try:
            process.wait(timeout=self._stop_timeout)
        except subprocess.TimeoutExpired:
            logger.warning("The local worker did not stop in time; killing it")
            _kill_tree(process)
            process.wait()


def _ask_to_stop(process: subprocess.Popen[bytes]) -> None:
    """Ask the worker to stop: SIGTERM, or CTRL_BREAK_EVENT to its process group on Windows.

    On Windows ``terminate()`` is TerminateProcess, which ends the onefile launcher
    but not the worker process it unpacked and started; that one would run on with
    its jobs. The break event reaches both (the job children ignore it).
    """
    if sys.platform == "win32":
        try:
            process.send_signal(signal.CTRL_BREAK_EVENT)
        except OSError:  # no console to send it through: end the tree instead
            _kill_tree(process)
        return
    process.terminate()


def _kill_tree(process: subprocess.Popen[bytes]) -> None:
    """Kill the worker; on Windows with its whole process tree, not only the onefile launcher."""
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(process.pid)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    process.kill()


__all__ = ["LocalWorkerLauncher", "default_command", "default_fingerprint", "default_server_url"]
