#!/usr/bin/env python3
"""Build, assemble, and smoke-test the versioned Windows release archive."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.version import __version__  # noqa: E402

DIST_DIR = PROJECT_ROOT / "dist"
BUILD_DIR = PROJECT_ROOT / "build"
STAGING_DIR = DIST_DIR / f"EDINET-{__version__}"
EXE_SOURCE = DIST_DIR / "EDINET.exe"
ZIP_DESTINATION = DIST_DIR / f"EDINET-{__version__}-Release.zip"
SPEC_FILE = PROJECT_ROOT / "EDINET.spec"
FRONTEND_ROOT = PROJECT_ROOT / "frontend-v2"
DATABASE_PATHS = {
    "db1": "data/databases/Base.db",
    "db2": "data/databases/Standardized.db",
    "db3": "data/databases/Portfolio.db",
}


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
    npm = "npm.cmd" if os.name == "nt" else "npm"
    run([npm, "--version"], cwd=FRONTEND_ROOT, timeout=15)
    print(f"Build preflight passed for EDINET {__version__}")


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
    STAGING_DIR.mkdir(parents=True, exist_ok=False)
    shutil.copy2(EXE_SOURCE, STAGING_DIR / "EDINET.exe")
    config_dir = STAGING_DIR / "config"
    config_dir.mkdir()
    (config_dir / "database_paths.json").write_text(
        json.dumps(DATABASE_PATHS, indent=2) + "\n",
        encoding="utf-8",
    )
    database_dir = STAGING_DIR / "data" / "databases"
    database_dir.mkdir(parents=True)
    for filename in ("Base.db", "Standardized.db", "Portfolio.db"):
        sqlite3.connect(database_dir / filename).close()
    (STAGING_DIR / ".env").write_text(
        "# EDINET API key: https://disclosure.edinet-fsa.go.jp/\n"
        "API_KEY=your_api_key_here\n"
        "# Maximum generated backtest artifact size in bytes (default 256 MiB)\n"
        "# EDINET_MAX_BACKTEST_ARTIFACT_BYTES=268435456\n",
        encoding="utf-8",
    )


def _free_loopback_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def smoke_test(timeout: int) -> None:
    """Start the packaged app and verify health, SPA, and a read-only API."""
    executable = STAGING_DIR / "EDINET.exe"
    port = _free_loopback_port()
    process = subprocess.Popen(
        [str(executable), "--no-reload", "--port", str(port)],
        cwd=str(STAGING_DIR),
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
    )
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("Packaged application exited during smoke test")
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/health",
                    timeout=2,
                ) as response:
                    health = json.load(response)
                if health.get("version") == __version__:
                    break
            except OSError:
                time.sleep(0.25)
        else:
            raise RuntimeError(f"Packaged application did not start within {timeout}s")
        for path in ("/", "/api/steps"):
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}{path}",
                timeout=5,
            ) as response:
                if response.status != 200:
                    raise RuntimeError(f"Smoke request failed: {path}")
    finally:
        _terminate_tree(process)


def create_archive() -> None:
    with zipfile.ZipFile(ZIP_DESTINATION, "w", zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(STAGING_DIR.rglob("*")):
            if file_path.is_file():
                archive.write(file_path, file_path.relative_to(STAGING_DIR))
    print(f"Created {ZIP_DESTINATION}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Run preflight only")
    parser.add_argument("--command-timeout", type=int, default=180)
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
    create_archive()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
