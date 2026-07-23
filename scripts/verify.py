#!/usr/bin/env python3
"""Run repository checks sequentially with hard per-stage timeouts."""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FRONTEND_ROOT = PROJECT_ROOT / "frontend-v2"


@dataclass(frozen=True)
class Stage:
    name: str
    command: tuple[str, ...]
    timeout_seconds: int
    cwd: Path = PROJECT_ROOT
    cleanup_paths: tuple[Path, ...] = ()


def _python_executable() -> str:
    candidates = (
        PROJECT_ROOT / ".venv3" / "Scripts" / "python.exe",
        PROJECT_ROOT / ".venv3" / "bin" / "python",
    )
    return str(next((path for path in candidates if path.is_file()), Path(sys.executable)))


def _npm_executable() -> str:
    return "npm.cmd" if os.name == "nt" else "npm"


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
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)


def _cleanup_temp_paths(stage: Stage) -> bool:
    tests_root = (PROJECT_ROOT / "tests").resolve()
    for raw_path in stage.cleanup_paths:
        path = raw_path.resolve(strict=False)
        if (
            path.parent != tests_root
            or not path.name.startswith(".pytest-tmp-verify-")
        ):
            print(f"ERROR: refusing unsafe cleanup path: {path}", flush=True)
            return False
        if not path.exists():
            continue
        try:
            shutil.rmtree(path)
        except OSError as exc:
            print(f"ERROR: could not clean test workspace {path}: {exc}", flush=True)
            return False
    return True


def _run_stage(stage: Stage, timeout_override: int | None) -> bool:
    timeout = timeout_override or stage.timeout_seconds
    print(f"\n[{stage.name}] timeout={timeout}s", flush=True)
    print("  " + " ".join(stage.command), flush=True)
    environment = os.environ.copy()
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    kwargs = {
        "cwd": str(stage.cwd),
        "env": environment,
    }
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    process = subprocess.Popen(stage.command, **kwargs)
    passed = False
    try:
        passed = process.wait(timeout=timeout) == 0
    except subprocess.TimeoutExpired:
        print(f"ERROR: {stage.name} exceeded {timeout}s; terminating process tree", flush=True)
        _terminate_tree(process)
    cleanup_ok = _cleanup_temp_paths(stage)
    return passed and cleanup_ok


def _stages() -> tuple[Stage, ...]:
    python = _python_executable()
    npm = _npm_executable()
    ruff_targets = (
        "src/api",
        "src/pipeline_jobs",
        "src/web_app/security.py",
        "src/portfolio/models.py",
        "src/screening/formatting.py",
        "src/screening/persistence.py",
        "scripts/build.py",
        "scripts/check_docs.py",
        "scripts/sync_requirements.py",
        "scripts/verify.py",
        "main.py",
    )
    token = uuid4().hex[:8]
    unit_temp = f"tests/.pytest-tmp-verify-{token}-unit"
    integration_temp = f"tests/.pytest-tmp-verify-{token}-integration"
    return (
        Stage("python-preflight", (python, "-B", "-c", "import fastapi, pytest, requests"), 15),
        Stage("requirements", (python, "-B", "scripts/sync_requirements.py", "--check"), 15),
        Stage("documentation", (python, "-B", "scripts/check_docs.py"), 20),
        Stage(
            "static-ruff",
            (python, "-B", "-m", "ruff", "check", *ruff_targets),
            45,
        ),
        Stage(
            "static-mypy",
            (
                python,
                "-B",
                "-m",
                "mypy",
                "src/api",
                "src/pipeline_jobs",
                "src/web_app/security.py",
            ),
            60,
        ),
        Stage("package-check", (python, "-B", "scripts/build.py", "--check"), 20),
        Stage(
            "unit",
            (python, "-B", "-m", "pytest", "tests/unit", "-q", "--basetemp", unit_temp, "-p", "no:cacheprovider"),
            180,
            cleanup_paths=(PROJECT_ROOT / unit_temp,),
        ),
        Stage(
            "integration",
            (python, "-B", "-m", "pytest", "tests/integration", "-q", "--basetemp", integration_temp, "-p", "no:cacheprovider"),
            60,
            cleanup_paths=(PROJECT_ROOT / integration_temp,),
        ),
        Stage("frontend-test", (npm, "test", "--", "--reporter=dot"), 60, FRONTEND_ROOT),
        Stage("frontend-lint", (npm, "run", "lint"), 60, FRONTEND_ROOT),
        Stage("frontend-build", (npm, "run", "build"), 90, FRONTEND_ROOT),
    )


def main() -> int:
    stages = _stages()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage",
        action="append",
        choices=[stage.name for stage in stages],
        help="Run only this stage; repeat to select more than one.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        help="Override every selected stage timeout with a positive value.",
    )
    args = parser.parse_args()
    if args.timeout_seconds is not None and args.timeout_seconds < 1:
        parser.error("--timeout-seconds must be positive")
    selected = set(args.stage or ())
    for stage in stages:
        if selected and stage.name not in selected:
            continue
        if not _run_stage(stage, args.timeout_seconds):
            print(f"FAILED: {stage.name}", flush=True)
            return 1
    print("\nAll selected verification stages passed.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
