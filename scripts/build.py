#!/usr/bin/env python3
"""Build, assemble, and smoke-test the release for the platform running this script.

PyInstaller cannot cross-compile, so run this once on Windows and once on
Linux (``.github/workflows/release.yml`` does both). Each run fills its own
folder under ``release/``: ``release/windows/ShadeResearch.exe`` or
``release/linux/ShadeResearch``; the other platform's folder is left alone.

The release is the executable alone. On first start it creates ``data/``
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
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.version import __version__  # noqa: E402

DIST_DIR = PROJECT_ROOT / "dist"
BUILD_DIR = PROJECT_ROOT / "build"
PRODUCT_SLUG = "ShadeResearch"
PLATFORM = "windows" if os.name == "nt" else "linux"
EXE_NAME = f"{PRODUCT_SLUG}.exe" if os.name == "nt" else PRODUCT_SLUG
EXE_SOURCE = DIST_DIR / EXE_NAME
RELEASE_DIR = PROJECT_ROOT / "release"
PLATFORM_DIR = RELEASE_DIR / PLATFORM
SPEC_FILE = PROJECT_ROOT / "EDINET.spec"
FRONTEND_ROOT = PROJECT_ROOT / "frontend-v2"
ORCHESTRATOR_ROOT = PROJECT_ROOT / "src" / "orchestrator"
DATA_FILES = ("app.db", "chat.db", "market.db", "filings.db", "certs/cert.pem", "certs/key.pem")


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
    os.killpg(process.pid, signal.SIGTERM)


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
    for required in (SPEC_FILE, FRONTEND_ROOT / "package-lock.json"):
        if not required.is_file():
            raise RuntimeError(f"Required build input is missing: {required}")
    try:
        import PyInstaller  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "PyInstaller is missing; install the build extra with "
            "python -m pip install -e .[build]"
        ) from exc
    import torch  # reached through argostranslate -> stanza

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
    run(
        [
            sys.executable,
            "-m",
            "PyInstaller",
            "--log-level",
            "WARN",
            str(SPEC_FILE),
        ],
        cwd=PROJECT_ROOT,
        timeout=command_timeout,
    )
    if not EXE_SOURCE.is_file():
        raise RuntimeError(f"PyInstaller did not create {EXE_SOURCE}")


def assemble_distribution() -> None:
    """Fill the release folder: the executable and nothing else.

    ``release/<platform>/`` is replaced wholesale, so a stale executable from
    an earlier build never ships; the other platform's folder is untouched.
    """
    RELEASE_DIR.mkdir(exist_ok=True)
    if PLATFORM_DIR.exists():
        shutil.rmtree(PLATFORM_DIR)
    PLATFORM_DIR.mkdir()
    shutil.copy2(EXE_SOURCE, PLATFORM_DIR / EXE_NAME)
    (PLATFORM_DIR / EXE_NAME).chmod(0o755)
    print(f"Created {PLATFORM_DIR / EXE_NAME}")


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


def _serve_once(executable: Path, timeout: int) -> None:
    """Start the packaged app, check health, the SPA, and every step, then stop it."""
    port = _free_loopback_port()
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
                with urllib.request.urlopen(
                    f"https://127.0.0.1:{port}/health",
                    timeout=2,
                    context=context,
                ) as response:
                    health = json.load(response)
                if health.get("version") == __version__:
                    break
            except OSError:
                time.sleep(0.25)
        else:
            raise RuntimeError(f"Packaged application did not start within {timeout}s")
        with urllib.request.urlopen(f"https://127.0.0.1:{port}/", timeout=5, context=context) as response:
            if response.status != 200:
                raise RuntimeError("Smoke request failed: /")
        with urllib.request.urlopen(f"https://127.0.0.1:{port}/api/steps", timeout=5, context=context) as response:
            payload = json.load(response)
        steps = payload if isinstance(payload, list) else payload.get("steps", [])
        missing = _expected_steps() - {step["name"] for step in steps}
        if missing:
            raise RuntimeError(f"The packaged app is missing pipeline steps: {', '.join(sorted(missing))}")
    finally:
        _terminate_tree(process)


def smoke_test(timeout: int) -> None:
    """Run a copy of the release in an empty folder, twice.

    The copy is told through its settings to run without sign-in (it only
    listens on loopback), so reading the step list on both starts proves
    the setting was read from ``data/app.db`` beside the executable each
    time, not from the folder a one-file build unpacks into and deletes on
    exit. Every database and the certificate must be there too.
    """
    with tempfile.TemporaryDirectory(prefix="shade-smoke-", ignore_cleanup_errors=True) as folder:
        executable = Path(folder) / EXE_NAME
        shutil.copy2(PLATFORM_DIR / EXE_NAME, executable)
        _cli(executable, "config", "set", "auth.mode", "disabled")
        _serve_once(executable, timeout)
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
    args = parser.parse_args()
    if args.command_timeout < 1 or args.smoke_timeout < 1:
        parser.error("timeouts must be positive")
    preflight()
    if args.check:
        return 0
    build_executable(args.command_timeout)
    assemble_distribution()
    smoke_test(args.smoke_timeout)
    remove_intermediates()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
