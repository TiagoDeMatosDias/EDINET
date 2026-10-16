"""``ShadeWorker`` command line: ``run``, ``enroll`` and ``status``.

Also reachable as ``python main.py worker ...``. Exit codes: 0 normal, 1 the
server cannot be reached (``status``), 2 configuration or enrollment problems,
3 rejected by the server (revoked, disabled, or a version mismatch).
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import socket
import sys
from pathlib import Path
from typing import Any

from src.jobs.protocol import ENROLLMENT_PREFIX, EnrollRequest

EXIT_OK = 0
EXIT_UNREACHABLE = 1
EXIT_CONFIGURATION = 2
EXIT_REJECTED = 3


class EnrollmentStringError(ValueError):
    """The enrollment string is malformed."""


def encode_enrollment(server_url: str, fingerprint: str, code: str) -> str:
    """Build an enrollment string (the server does this; kept here for tests and tooling)."""
    body = json.dumps({"u": server_url, "f": fingerprint, "c": code}, separators=(",", ":"))
    return ENROLLMENT_PREFIX + base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")


def decode_enrollment(text: str) -> tuple[str, str, str]:
    """``(server_url, fingerprint, code)`` from an ``SRW1.`` enrollment string."""
    text = text.strip()
    if not text.startswith(ENROLLMENT_PREFIX):
        raise EnrollmentStringError(f"An enrollment string starts with {ENROLLMENT_PREFIX}")
    encoded = text[len(ENROLLMENT_PREFIX):]
    try:
        raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        data = json.loads(raw)
        url, fingerprint, code = str(data["u"]), str(data["f"]), str(data["c"])
    except (binascii.Error, ValueError, KeyError, TypeError) as exc:
        raise EnrollmentStringError("The enrollment string is damaged; copy it again from the server") from exc
    if not url.startswith("https://") or not fingerprint or not code:
        raise EnrollmentStringError("The enrollment string is incomplete; copy it again from the server")
    return url, fingerprint.replace(":", "").lower(), code


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="ShadeWorker", description="Shade Research job worker")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Connect to the server and run jobs until stopped.")
    run.add_argument("--config", help="worker.db path (default: beside the program).")
    run.add_argument(
        "--stdin-credentials",
        action="store_true",
        help="Read the connection details as one JSON line on stdin (used by the server-launched worker).",
    )
    run.add_argument("--interactive-slots", type=int, help="Override the interactive slot count for this run.")
    run.add_argument("--batch-slots", type=int, help="Override the batch slot count for this run.")

    enroll = commands.add_parser("enroll", help="Enroll this machine using the string from Admin > Workers.")
    enroll.add_argument("code", help="Enrollment string starting with SRW1.")
    enroll.add_argument("--config", help="worker.db path (default: beside the program).")
    enroll.add_argument("--name", help="Name shown on the Admin page (default: this machine's name).")

    status = commands.add_parser("status", help="Show the enrollment and whether the server is reachable.")
    status.add_argument("--config", help="worker.db path (default: beside the program).")
    return parser.parse_args(argv)


def _read_stdin_credentials() -> tuple[Any, int | None]:
    from src.worker.local_config import WorkerCredentials

    line = sys.stdin.readline()
    try:
        data = json.loads(line)
        credentials = WorkerCredentials(
            server_url=str(data["server_url"]),
            fingerprint=str(data["fingerprint"]),
            worker_id=str(data["worker_id"]),
            token=str(data["token"]),
        )
        parent = data.get("parent_pid")
        parent_pid = int(parent) if parent else None
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError("--stdin-credentials needs one JSON line with server_url, fingerprint, worker_id, token") from exc
    return credentials, parent_pid


def _run(args: argparse.Namespace) -> int:
    from src.worker import local_config
    from src.worker.client import (
        CertificateMismatchError,
        VersionMismatchError,
        WorkerClient,
        WorkerRejectedError,
    )
    from src.worker.runtime import configure_logging
    from src.worker.supervisor import Supervisor

    parent_pid: int | None = None
    if args.stdin_credentials:
        try:
            credentials, parent_pid = _read_stdin_credentials()
        except ValueError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return EXIT_CONFIGURATION
    else:
        credentials = local_config.load(args.config)
        if credentials is None:
            path = args.config or local_config.default_path()
            print(
                f"Error: this worker is not enrolled ({path} has no connection details). "
                "Run: ShadeWorker enroll <enrollment string from Admin > Workers>",
                file=sys.stderr,
            )
            return EXIT_CONFIGURATION

    configure_logging(credentials.worker_id)
    client = WorkerClient(credentials.server_url, credentials.token, credentials.fingerprint)
    supervisor = Supervisor(
        client,
        interactive_slots=args.interactive_slots,
        batch_slots=args.batch_slots,
        parent_pid=parent_pid,
    )
    try:
        return supervisor.run()
    except VersionMismatchError as exc:
        print(f"Error: {exc} Update the worker to the server's version.", file=sys.stderr)
        return EXIT_REJECTED
    except WorkerRejectedError as exc:
        print(f"Error: the server rejected this worker: {exc}", file=sys.stderr)
        return EXIT_REJECTED
    except CertificateMismatchError as exc:
        print(f"Error: {exc}. Enroll again if the server's certificate was replaced.", file=sys.stderr)
        return EXIT_CONFIGURATION


def _enroll(args: argparse.Namespace) -> int:
    from src.version import __version__
    from src.worker import local_config
    from src.worker.client import WorkerApiError, WorkerClient, WorkerClientError

    try:
        server_url, fingerprint, code = decode_enrollment(args.code)
    except EnrollmentStringError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_CONFIGURATION
    hostname = socket.gethostname()
    name = args.name or hostname
    try:
        response = WorkerClient.enroll(
            server_url, fingerprint, EnrollRequest(code=code, name=name, hostname=hostname, version=__version__)
        )
    except WorkerApiError as exc:
        print(f"Error: enrollment was refused: {exc}", file=sys.stderr)
        return EXIT_CONFIGURATION
    except WorkerClientError as exc:
        print(f"Error: enrollment failed: {exc}", file=sys.stderr)
        return EXIT_CONFIGURATION
    path = local_config.save(
        args.config,
        local_config.WorkerCredentials(
            server_url=server_url,
            fingerprint=fingerprint,
            worker_id=response.worker_id,
            token=response.token,
            name=name,
        ),
    )
    print(f"Enrolled as worker {response.worker_id} with {server_url}. Connection details saved in {path}.")
    return EXIT_OK


def _status(args: argparse.Namespace) -> int:
    from src.worker import local_config
    from src.worker.client import normalize_fingerprint, probe_certificate_fingerprint

    credentials = local_config.load(args.config)
    if credentials is None:
        path = Path(args.config) if args.config else local_config.default_path()
        print(f"Not enrolled: {path} has no connection details.")
        print("Run: ShadeWorker enroll <enrollment string from Admin > Workers>")
        return EXIT_CONFIGURATION
    print(f"Server:    {credentials.server_url}")
    print(f"Worker id: {credentials.worker_id}")
    if credentials.name:
        print(f"Name:      {credentials.name}")
    try:
        presented = probe_certificate_fingerprint(credentials.server_url)
    except OSError as exc:
        print(f"Reachable: no ({exc})")
        return EXIT_UNREACHABLE
    if presented != normalize_fingerprint(credentials.fingerprint):
        print("Reachable: yes, but its certificate is not the one pinned at enrollment")
        return EXIT_UNREACHABLE
    print("Reachable: yes (certificate matches)")
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.command == "run":
        return _run(args)
    if args.command == "enroll":
        return _enroll(args)
    return _status(args)
