"""A Cloudflare tunnel that publishes the workstation, run as a child process.

``cloudflared`` connects out to Cloudflare and relays visitors to the local
server, so the server keeps listening on this machine only and no port,
router, or firewall changes. The ``tunnel.enabled`` setting turns it on. With
a ``tunnel.token`` it runs that Cloudflare account's tunnel, whose address is
set in Cloudflare and stays the same. With a ``tunnel.api_token`` and a
``tunnel.hostname`` it creates its own tunnel on first use, stores the token
in ``app.db``, and reuses it on every start so the address stays the same; a
reset in the admin panel clears it and starts a fresh one. Without either,
Cloudflare assigns a temporary ``trycloudflare.com`` address that changes on
every start.

The tunnel never opens while sign-in is disabled or before the first
(administrator) account exists: the caller passes those checks as ``blocked``.

A release carries ``cloudflared`` inside the executable (``EDINET.spec`` puts
it at ``bundled_cloudflared()``). A source checkout uses the copy the build
left there, else one on ``PATH``, else downloads the official release once
into the data folder.
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

import requests  # type: ignore[import-untyped]

from src.paths import bundle_dir, tools_dir
from src.settings import get_setting
from src.settings.store import SettingsStore
from src.web_app.tunnel_api import TunnelApiError, create_tunnel, delete_tunnel

logger = logging.getLogger(__name__)

_RELEASES = "https://github.com/cloudflare/cloudflared/releases/latest/download/"
_INSTALL_GUIDE = "https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/"
_QUICK_URL = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")
_ARCHITECTURES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
    "armv7l": "arm",
    "armv6l": "arm",
    "i386": "386",
    "i686": "386",
    "x86": "386",
}

# A failed start is tried again after this long, doubling up to the maximum.
RETRY_SECONDS = 5.0
MAX_RETRY_SECONDS = 300.0
# What cloudflared gets to close its connections before it is killed.
STOP_SECONDS = 3.0

_download_lock = threading.Lock()


class TunnelError(RuntimeError):
    """The tunnel could not be started; the message is shown to the administrator."""


def _release_asset() -> str | None:
    """The official release file for this machine, where it is a bare executable."""
    architecture = _ARCHITECTURES.get(platform.machine().lower())
    if sys.platform.startswith("linux") and architecture:
        return f"cloudflared-linux-{architecture}"
    if sys.platform == "win32" and architecture in {"amd64", "386"}:
        return f"cloudflared-windows-{architecture}.exe"
    return None


def _download(url: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + ".part")
    try:
        with requests.get(url, stream=True, timeout=(10, 60)) as response:
            response.raise_for_status()
            with partial.open("wb") as handle:
                for chunk in response.iter_content(1 << 20):
                    handle.write(chunk)
        partial.chmod(0o755)
        os.replace(partial, target)
    except (requests.RequestException, OSError) as exc:
        partial.unlink(missing_ok=True)
        raise TunnelError(
            f"cloudflared could not be downloaded ({type(exc).__name__}). Check the internet "
            "connection, or install cloudflared so that it is on PATH."
        ) from exc


def _program() -> str:
    return "cloudflared.exe" if os.name == "nt" else "cloudflared"


def bundled_cloudflared() -> Path:
    """Where a release carries cloudflared; in a source checkout, where the build keeps it."""
    return bundle_dir() / "tools" / "bin" / _program()


def download_cloudflared(target: Path) -> None:
    """Fetch the official cloudflared release for this machine to ``target``."""
    asset = _release_asset()
    if asset is None:
        raise TunnelError(
            "cloudflared is not installed, and Cloudflare publishes no ready-to-run "
            f"download for this machine. Install it so that it is on PATH: {_INSTALL_GUIDE}"
        )
    _download(_RELEASES + asset, target)


def find_cloudflared() -> tuple[str, Path] | None:
    """The cloudflared at hand and where it comes from, without downloading one."""
    bundled = bundled_cloudflared()
    if bundled.is_file():
        return "bundled", bundled
    installed = shutil.which("cloudflared")
    if installed:
        return "installed", Path(installed)
    downloaded = tools_dir() / _program()
    if downloaded.is_file():
        return "downloaded", downloaded
    return None


def ensure_cloudflared(on_download: Callable[[], None] = lambda: None) -> Path:
    """Path of the cloudflared at hand, downloading one into the data folder when there is none."""
    with _download_lock:
        found = find_cloudflared()
        if found:
            return found[1]
        target = tools_dir() / _program()
        logger.info("Downloading cloudflared to %s", target)
        on_download()
        download_cloudflared(target)
    return target


def _token() -> str:
    # The Cloudflare dashboard shows the token at the end of a command line;
    # accept the whole line pasted in.
    words = str(get_setting("tunnel.token") or "").split()
    return words[-1] if words else ""


def _app_db() -> Path:
    # Imported late: src.orchestrator discovers the pipeline steps on import,
    # and the steps import src.settings.
    from src.orchestrator.common.db_config import get_app_db

    return Path(get_app_db())


def _stored_credentials() -> dict[str, Any] | None:
    """The tunnel the server created for itself, read from app.db."""
    value = get_setting("tunnel.credentials")
    return value if isinstance(value, dict) else None


def _store_credentials(credentials: dict[str, Any]) -> None:
    SettingsStore(_app_db()).set("tunnel.credentials", credentials)


def _clear_credentials() -> None:
    SettingsStore(_app_db()).delete("tunnel.credentials")


def _public_url(config: object, origin: str) -> str | None:
    """The address of an account's tunnel, from the configuration Cloudflare sends it."""
    try:
        rules = json.loads(config)["ingress"] if isinstance(config, str) else None
    except (ValueError, TypeError, KeyError):
        return None
    if not isinstance(rules, list):
        return None
    named = [rule for rule in rules if isinstance(rule, dict) and rule.get("hostname")]
    port = f":{urlsplit(origin).port}"
    # A tunnel can publish several services; prefer the rule that points at this server.
    ours = [rule for rule in named if str(rule.get("service", "")).rstrip("/").endswith(port)]
    hostname = str((ours or named or [{}])[0].get("hostname", ""))
    return f"https://{hostname}" if hostname and "*" not in hostname else None


def _entry(line: str) -> tuple[str, str, dict[str, Any]]:
    """One line of cloudflared output as (level, message, fields); plain text has no level."""
    try:
        fields = json.loads(line)
    except ValueError:
        fields = None
    if not isinstance(fields, dict):
        return "", line.strip(), {}
    return str(fields.get("level", "info")), str(fields.get("message", "")), fields


class TunnelManager:
    """Keeps cloudflared running while the ``tunnel.enabled`` setting is on."""

    def __init__(
        self,
        origin: str,
        *,
        blocked: Callable[[], str | None] = lambda: None,
        locate: Callable[[Callable[[], None]], list[str]] = lambda on_download: [
            str(ensure_cloudflared(on_download))
        ],
    ) -> None:
        self.origin = origin
        self._blocked = blocked
        self._locate = locate
        self._control = threading.Lock()  # one apply() or stop() at a time
        self._lock = threading.Lock()  # guards the fields below
        self._state = "off"
        self._url: str | None = None
        self._message: str | None = None
        self._process: subprocess.Popen[str] | None = None
        self._stop: threading.Event | None = None
        self._thread: threading.Thread | None = None
        # The lifespan stops the tunnel; this covers an exit that skips it.
        atexit.register(self.stop)

    def status(self) -> dict[str, Any]:
        with self._lock:
            state, url, message = self._state, self._url, self._message
        return {
            "enabled": get_setting("tunnel.enabled") == "on",
            "kind": "named" if (_token() or (_stored_credentials() or {}).get("token")) else "quick",
            "state": state,
            "url": url,
            "message": message,
            "origin": self.origin,
            "blocked_reason": self._blocked(),
            # bundled, installed, downloaded, or missing (downloaded on first use)
            "cloudflared": (find_cloudflared() or ("missing",))[0],
        }

    def _token_for_run(self) -> str:
        """The token to run: a pasted one, the stored one, or a freshly created one."""
        token = _token()
        if token:
            return token
        stored = _stored_credentials()
        if stored and stored.get("token"):
            return str(stored["token"])
        api_token = str(get_setting("tunnel.api_token") or "")
        hostname = str(get_setting("tunnel.hostname") or "")
        if api_token and hostname:
            name = "edinet-" + datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            try:
                token, tunnel_id = create_tunnel(api_token, name, hostname, self.origin)
            except TunnelApiError as exc:
                raise TunnelError(str(exc)) from exc
            _store_credentials({
                "token": token,
                "tunnel_id": tunnel_id,
                "name": name,
                "hostname": hostname,
                "created_at": datetime.now(timezone.utc).isoformat(),
            })
            logger.info("Created a new Cloudflare tunnel %s at %s", name, hostname)
            return token
        return ""

    def apply(self) -> None:
        """Bring the tunnel in line with the settings: start it again when on, stop it when off."""
        with self._control:
            self._halt()
            if get_setting("tunnel.enabled") != "on":
                return
            stop = threading.Event()
            self._stop = stop
            self._set(stop, "starting")
            self._thread = threading.Thread(
                target=self._supervise, args=(stop,), name="cloudflare-tunnel", daemon=True
            )
            self._thread.start()

    def stop(self) -> None:
        with self._control:
            self._halt()

    def reset(self) -> None:
        """Discard the tunnel the server created and start a fresh one.

        The old tunnel is deleted from Cloudflare (best effort) and its stored
        credentials are cleared, so the next start creates a new tunnel. A
        pasted ``tunnel.token`` is left alone: it is not one the server made.
        """
        stored = _stored_credentials()
        api_token = str(get_setting("tunnel.api_token") or "")
        if stored and api_token:
            delete_tunnel(api_token, str(stored.get("tunnel_id") or ""))
        if stored:
            _clear_credentials()
        self.apply()

    def _halt(self) -> None:
        stop, thread = self._stop, self._thread
        if stop is not None:
            stop.set()
        with self._lock:
            process = self._process
            self._state, self._url, self._message = "off", None, None
        if process is not None:
            process.terminate()
            try:
                process.wait(STOP_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
        if thread is not None:
            # A download in progress is not interrupted; that thread ends by itself.
            thread.join(STOP_SECONDS)
        self._stop = self._thread = None

    def _set(self, stop: threading.Event, state: str, *, url: str | None = None, message: str | None = None) -> None:
        with self._lock:
            if not stop.is_set():  # a run that was told to stop no longer speaks for the tunnel
                self._state, self._url, self._message = state, url, message

    def _supervise(self, stop: threading.Event) -> None:
        delay = RETRY_SECONDS
        while not stop.is_set():
            reason = self._blocked()
            if reason:
                self._set(stop, "blocked", message=reason)
                stop.wait(RETRY_SECONDS)
                continue
            try:
                connected, problem = self._serve(stop)
            except TunnelError as exc:
                connected, problem = False, str(exc)
            except Exception:  # noqa: BLE001 - keep retrying rather than end the thread silently
                logger.exception("Cloudflare tunnel failed unexpectedly")
                connected, problem = False, "The tunnel failed unexpectedly; see the server log."
            if stop.is_set():
                return
            logger.warning("Cloudflare tunnel stopped: %s", problem)
            self._set(stop, "failed", message=problem)
            if connected:
                delay = RETRY_SECONDS
            stop.wait(delay)
            delay = min(delay * 2, MAX_RETRY_SECONDS)

    def _serve(self, stop: threading.Event) -> tuple[bool, str]:
        """Run cloudflared until it exits; return whether it connected and why it ended."""
        token = self._token_for_run()
        command = [*self._locate(lambda: self._set(stop, "downloading")), "tunnel", "--no-autoupdate", "--output", "json"]
        environment = dict(os.environ)
        if token:
            command.append("run")
            # Not on the command line, where other users of the machine could read it.
            environment["TUNNEL_TOKEN"] = token
        else:
            # The server's own certificate is self-signed, and the hop stays on this machine.
            command += ["--url", self.origin, "--no-tls-verify"]
        if stop.is_set():
            return False, ""
        self._set(stop, "starting")
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=environment,
            )
        except OSError as exc:
            raise TunnelError(f"cloudflared could not be started ({exc}).") from exc
        with self._lock:
            stopping = stop.is_set()  # stop() ran between the check above and the start
            if not stopping:
                self._process = process
        if stopping:
            process.terminate()

        connected, url, said, problem = False, None, "", ""
        assert process.stdout is not None
        for line in process.stdout:
            level, message, fields = _entry(line)
            if not message:
                continue
            serious = level != "info" and not stop.is_set()
            logger.log(logging.WARNING if serious else logging.DEBUG, "cloudflared: %s", message)
            if not level:
                said = said or message  # a usage error, such as an invalid token
            elif level != "info":
                problem = message
            found = None
            if token:
                found = _public_url(fields.get("config"), self.origin)
            elif level == "info" and (match := _QUICK_URL.search(message)):
                found = match.group(0)
            changed = bool(found) and found != url
            url = found or url
            if message == "Registered tunnel connection" and not connected:
                connected = changed = True
            if connected and changed:
                logger.info("Cloudflare tunnel open%s", f" at {url}" if url else "")
                self._set(stop, "running", url=url)
        code = process.wait()
        with self._lock:
            if self._process is process:
                self._process = None
        return connected, said or problem or f"cloudflared stopped (exit code {code})."
