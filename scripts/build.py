#!/usr/bin/env python3
"""Build, assemble, and smoke-test the release for the platform running this script.

PyInstaller cannot cross-compile, so run this once on Windows and once on
Linux (``.github/workflows/release.yml`` does both). Each run fills its own
folder under ``release/``: ``release/windows/`` or ``release/linux/``; the
other platform's folder is left alone.

The release is two executables side by side: ``ShadeResearch`` (the server)
and ``ShadeWorker`` (the job worker, which carries the translation engine).
The server starts the worker beside it. On first start it creates ``data/``
beside itself with every database, the TLS certificate, and logs; settings
such as the EDINET API key are entered on the Admin page or with
``ShadeResearch.exe config set``.

A successful run leaves nothing else behind: PyInstaller's ``build/`` and
``dist/`` are removed once the release has passed its smoke test.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.version import __version__  # noqa: E402

DIST_DIR = PROJECT_ROOT / "dist"
BUILD_DIR = PROJECT_ROOT / "build"
PRODUCT_SLUG = "ShadeResearch"
PLATFORM = "windows" if os.name == "nt" else "linux"
WORKER_SLUG = "ShadeWorker"
EXE_NAME = f"{PRODUCT_SLUG}.exe" if os.name == "nt" else PRODUCT_SLUG
WORKER_EXE_NAME = f"{WORKER_SLUG}.exe" if os.name == "nt" else WORKER_SLUG
EXE_SOURCE = DIST_DIR / EXE_NAME
WORKER_EXE_SOURCE = DIST_DIR / WORKER_EXE_NAME
RELEASE_DIR = PROJECT_ROOT / "release"
PLATFORM_DIR = RELEASE_DIR / PLATFORM
SPEC_FILE = PROJECT_ROOT / "EDINET.spec"
WORKER_SPEC_FILE = PROJECT_ROOT / "ShadeWorker.spec"
FRONTEND_ROOT = PROJECT_ROOT / "frontend-v2"
ORCHESTRATOR_ROOT = PROJECT_ROOT / "src" / "orchestrator"
WORKER_START_TIMEOUT = 60
DATA_FILES = ("app.db", "chat.db", "jobs.db", "market.db", "filings.db", "certs/cert.pem", "certs/key.pem")


def _terminate_tree(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                check=False,
                timeout=10,
            )
        except subprocess.TimeoutExpired:
            process.kill()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        return
    # Let the application shut down (it stops its worker and releases its
    # jobs) before anything removes the folder it runs in.
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def run(command: list[str], *, cwd: Path, timeout: int) -> None:
    """Run one command with live output and a hard process-tree timeout."""
    print("  > " + " ".join(command), flush=True)
    kwargs = {"cwd": str(cwd), "env": os.environ.copy()}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    process = subprocess.Popen(command, **kwargs)
    try:
        return_code = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _terminate_tree(process)
        raise RuntimeError(
            f"Command exceeded {timeout} seconds: {' '.join(command)}"
        ) from exc
    if return_code:
        raise RuntimeError(
            f"Command failed with exit code {return_code}: {' '.join(command)}"
        )


def preflight() -> None:
    """Fail before mutation when the build environment is incomplete."""
    if not ((3, 12) <= sys.version_info[:2] < (3, 14)):
        raise RuntimeError("Packaging requires supported Python 3.12 or 3.13")
    for required in (SPEC_FILE, WORKER_SPEC_FILE, FRONTEND_ROOT / "package-lock.json"):
        if not required.is_file():
            raise RuntimeError(f"Required build input is missing: {required}")
    try:
        import PyInstaller  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "PyInstaller is missing; install the build extra with "
            "python -m pip install -e .[build]"
        ) from exc
    import torch  # reached through argostranslate -> stanza; packaged in ShadeWorker only

    if torch.version.cuda:
        raise RuntimeError(
            f"torch {torch.__version__} is a CUDA build; packaging it adds several "
            "gigabytes of GPU libraries that the CPU-only translator never loads. "
            "Install the CPU build with python -m pip install torch "
            "--index-url https://download.pytorch.org/whl/cpu"
        )
    npm = "npm.cmd" if os.name == "nt" else "npm"
    if shutil.which(npm) is None:
        raise RuntimeError("npm is missing; install Node.js 22, which includes it")
    run([npm, "--version"], cwd=FRONTEND_ROOT, timeout=15)
    print(f"Build preflight passed for Shade Research {__version__}")


def _safe_remove(directory: Path) -> None:
    resolved = directory.resolve(strict=False)
    if resolved.parent != PROJECT_ROOT or resolved.name not in {"build", "dist"}:
        raise RuntimeError(f"Refusing to remove unexpected directory: {resolved}")
    if resolved.exists():
        shutil.rmtree(resolved)


def build_executable(command_timeout: int) -> None:
    npm = "npm.cmd" if os.name == "nt" else "npm"
    run([npm, "ci"], cwd=FRONTEND_ROOT, timeout=min(command_timeout, 300))
    run([npm, "run", "build"], cwd=FRONTEND_ROOT, timeout=min(command_timeout, 180))
    if not (FRONTEND_ROOT / "dist" / "index.html").is_file():
        raise RuntimeError("Frontend build did not create dist/index.html")
    _safe_remove(BUILD_DIR)
    _safe_remove(DIST_DIR)
    for spec_file, source in ((SPEC_FILE, EXE_SOURCE), (WORKER_SPEC_FILE, WORKER_EXE_SOURCE)):
        run(
            [
                sys.executable,
                "-m",
                "PyInstaller",
                "--log-level",
                "WARN",
                str(spec_file),
            ],
            cwd=PROJECT_ROOT,
            timeout=command_timeout,
        )
        if not source.is_file():
            raise RuntimeError(f"PyInstaller did not create {source}")


def assemble_distribution() -> None:
    """Fill the release folder: the server and worker executables and nothing else.

    ``release/<platform>/`` is replaced wholesale, so a stale executable from
    an earlier build never ships; the other platform's folder is untouched.
    """
    RELEASE_DIR.mkdir(exist_ok=True)
    if PLATFORM_DIR.exists():
        shutil.rmtree(PLATFORM_DIR)
    PLATFORM_DIR.mkdir()
    for source, name in ((EXE_SOURCE, EXE_NAME), (WORKER_EXE_SOURCE, WORKER_EXE_NAME)):
        shutil.copy2(source, PLATFORM_DIR / name)
        (PLATFORM_DIR / name).chmod(0o755)
        print(f"Created {PLATFORM_DIR / name}")


def _free_loopback_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _expected_steps() -> set[str]:
    """Pipeline step packages in the source tree, which the exe must all offer."""
    return {
        path.parent.name
        for path in ORCHESTRATOR_ROOT.glob("*/__init__.py")
        if path.parent.name != "common"
    }


def _cli(executable: Path, *arguments: str) -> str:
    result = subprocess.run(
        [str(executable), *arguments],
        cwd=str(executable.parent),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"{executable.name} {' '.join(arguments)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def _fetch(
    context: ssl.SSLContext,
    url: str,
    *,
    body: dict | None = None,
    timeout: float = 10,
) -> tuple[int, dict | list | None]:
    """GET (or POST ``body`` as JSON) and return ``(status, json)``; HTTP errors are returned, not raised."""
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"} if data else {})
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=context) as response:
            raw = response.read()
            return response.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw) if raw else None
        except ValueError:
            return exc.code, {"detail": raw.decode(errors="replace")[:300]}


def _wait_for_local_worker(context: ssl.SSLContext, base: str, process: subprocess.Popen, timeout: int) -> None:
    """Wait until the server-launched ``local`` worker is online with data access and its launcher is running."""
    deadline = time.monotonic() + timeout
    last: object = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Packaged application exited while waiting for its worker")
        status, payload = _fetch(context, f"{base}/api/admin/workers")
        if status == 200 and isinstance(payload, dict):
            last = payload
            local = next((w for w in payload.get("workers", []) if w.get("kind") == "local"), None)
            if (
                local
                and local.get("online")
                and local.get("data_access")
                and payload.get("launcher", {}).get("state") == "running"
            ):
                return
        time.sleep(1)
    raise RuntimeError(f"The local worker did not come online within {timeout}s (last: {last})")


def _check_diagnostic_job(context: ssl.SSLContext, base: str) -> None:
    status, payload = _fetch(
        context,
        f"{base}/api/admin/workers/echo",
        body={"wait": True, "result": "smoke", "artifact_text": "x"},
        timeout=60,
    )
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"The diagnostic job did not complete: HTTP {status} {payload}")
    if payload.get("status") != "completed" or payload.get("result") != {"echo": "smoke"}:
        raise RuntimeError(f"The diagnostic job returned an unexpected result: {payload}")


def _check_translation(context: ssl.SSLContext, base: str, timeout: int) -> None:
    """Translate one sentence through the worker; the first call loads (or installs) the models."""
    request = {"texts": ["本日は晴天なり。"], "force": False}
    deadline = time.monotonic() + timeout
    last: object = None
    while time.monotonic() < deadline:
        status, payload = _fetch(context, f"{base}/api/filings/translate", body=request, timeout=60)
        last = payload
        if status == 200 and isinstance(payload, dict):
            items = payload.get("translations") or []
            translated = str(items[0].get("translated", "")) if items else ""
            if translated and translated != request["texts"][0]:
                return
            raise RuntimeError(f"The translation came back untranslated: {payload}")
        if status == 202 and isinstance(payload, dict) and payload.get("job_id"):
            # Queued: wait for the job, then ask again (the answer is now cached).
            while time.monotonic() < deadline:
                state, job = _fetch(context, f"{base}/api/my-jobs/{payload['job_id']}")
                if state == 200 and isinstance(job, dict) and job.get("status") in {"completed", "failed", "cancelled"}:
                    if job["status"] != "completed":
                        raise RuntimeError(f"The translation job {job['status']}: {job.get('error') or job}")
                    break
                time.sleep(1)
            continue
        raise RuntimeError(f"Translation failed: HTTP {status} {payload}")
    raise RuntimeError(f"Translation did not finish within {timeout}s (last: {last})")


def _worker_processes_left(executable: Path) -> list[int]:
    """PIDs of any running process whose executable is the worker copy beside ``executable``."""
    worker = (executable.parent / WORKER_EXE_NAME).resolve()
    found: list[int] = []
    if os.name == "nt":
        output = subprocess.run(
            ["tasklist", "/FI", f"IMAGENAME eq {WORKER_EXE_NAME}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout
        return [int(line.split(",")[1].strip('"')) for line in output.splitlines() if WORKER_EXE_NAME in line]
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if (entry / "exe").resolve() == worker:
                found.append(int(entry.name))
        except OSError:
            continue
    return found


def _serve_once(executable: Path, timeout: int, *, with_worker: bool = False, translation_timeout: int = 120) -> None:
    """Start the packaged app, check health, the SPA, every step, and cloudflared, then stop it.

    With ``with_worker`` it also waits for the worker the server launches
    beside itself, runs a diagnostic job and a translation through it, and
    checks that no worker process outlives the server.
    """
    port = _free_loopback_port()
    base = f"https://127.0.0.1:{port}"
    kwargs: dict = {"cwd": str(executable.parent), "env": {**os.environ, "EDINET_NO_BROWSER": "1"}}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    process = subprocess.Popen([str(executable), "--no-reload", "--port", str(port)], **kwargs)
    # The app serves TLS with a self-signed certificate from data/certs/ next
    # to the executable; skip verification, this is a loopback smoke test.
    context = ssl._create_unverified_context()
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Packaged application exited during smoke test")
            try:
                with urllib.request.urlopen(f"{base}/health", timeout=2, context=context) as response:
                    health = json.load(response)
                if health.get("version") == __version__:
                    break
            except OSError:
                time.sleep(0.25)
        else:
            raise RuntimeError(f"Packaged application did not start within {timeout}s")
        with urllib.request.urlopen(f"{base}/", timeout=5, context=context) as response:
            if response.status != 200:
                raise RuntimeError("Smoke request failed: /")
        with urllib.request.urlopen(f"{base}/api/steps", timeout=5, context=context) as response:
            payload = json.load(response)
        steps = payload if isinstance(payload, list) else payload.get("steps", [])
        missing = _expected_steps() - {step["name"] for step in steps}
        if missing:
            raise RuntimeError(f"The packaged app is missing pipeline steps: {', '.join(sorted(missing))}")
        with urllib.request.urlopen(f"{base}/api/admin/server/tunnel", timeout=5, context=context) as response:
            cloudflared = json.load(response).get("cloudflared")
        if cloudflared != "bundled":
            raise RuntimeError(f"The packaged app does not carry cloudflared for its tunnel (found: {cloudflared})")
        if with_worker:
            _wait_for_local_worker(context, base, process, WORKER_START_TIMEOUT)
            _check_diagnostic_job(context, base)
            _check_translation(context, base, translation_timeout)
    finally:
        _terminate_tree(process)
        if with_worker:
            _expect_worker_gone(executable, process)


def _expect_worker_gone(executable: Path, process: subprocess.Popen) -> None:
    """After the server stops, its worker (and the job children) must exit on their own."""
    try:
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        left = _worker_processes_left(executable)
        if not left:
            return
        time.sleep(0.5)
    for pid in left:
        try:
            os.kill(pid, signal.SIGKILL if os.name != "nt" else signal.SIGTERM)
        except OSError:
            pass
    raise RuntimeError(f"The worker kept running after the server stopped (pids {left})")


def smoke_test(timeout: int, *, folder: Path | None = None, translation_timeout: int = 120) -> None:
    """Run a copy of the release in an empty folder, twice.

    Both executables are copied, because the server starts the worker from the
    folder it sits in. The copy is told through its settings to run without
    sign-in (it only listens on loopback), so reading the step list on both
    starts proves the setting was read from ``data/app.db`` beside the
    executable each time, not from the folder a one-file build unpacks into
    and deletes on exit. Every database and the certificate must be there
    too. The first start also runs a diagnostic job and a translation through
    the worker; the second only checks that the server comes back.

    ``folder`` is the release folder to test (default: this platform's).
    """
    source_dir = folder or PLATFORM_DIR
    with tempfile.TemporaryDirectory(prefix="shade-smoke-", ignore_cleanup_errors=True) as work:
        for name in (EXE_NAME, WORKER_EXE_NAME):
            shutil.copy2(source_dir / name, Path(work) / name)
        executable = Path(work) / EXE_NAME
        _cli(executable, "config", "set", "auth.mode", "disabled")
        _serve_once(executable, timeout, with_worker=True, translation_timeout=translation_timeout)
        missing = [name for name in DATA_FILES if not (executable.parent / "data" / name).is_file()]
        if missing:
            raise RuntimeError(f"The packaged app did not create data/{', data/'.join(missing)} beside itself")
        _serve_once(executable, timeout)
        if _cli(executable, "config", "get", "auth.mode") != "disabled":
            raise RuntimeError("A saved setting did not survive restarting the packaged app")


def remove_intermediates() -> None:
    """Drop PyInstaller's work and output folders; the release holds the result."""
    _safe_remove(BUILD_DIR)
    _safe_remove(DIST_DIR)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Run preflight only")
    parser.add_argument("--command-timeout", type=int, default=600)
    parser.add_argument("--smoke-timeout", type=int, default=45)
    parser.add_argument("--translation-timeout", type=int, default=120)
    parser.add_argument("--smoke-only", type=Path, metavar="FOLDER", help="Smoke-test the executables in FOLDER and stop")
    args = parser.parse_args()
    if args.command_timeout < 1 or args.smoke_timeout < 1:
        parser.error("timeouts must be positive")
    if args.smoke_only:
        smoke_test(args.smoke_timeout, folder=args.smoke_only.resolve(), translation_timeout=args.translation_timeout)
        return 0
    preflight()
    if args.check:
        return 0
    build_executable(args.command_timeout)
    assemble_distribution()
    smoke_test(args.smoke_timeout, translation_timeout=args.translation_timeout)
    remove_intermediates()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
