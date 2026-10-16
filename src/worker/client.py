"""HTTP client for ``/api/worker/*`` with certificate pinning.

The server's certificate is self-signed, so the client does not verify a chain or
a host name; instead it requires the certificate's SHA-256 fingerprint (pinned at
enrollment) to match, which urllib3 checks during the handshake.
"""

from __future__ import annotations

import logging
import mimetypes
import socket
import ssl
import threading
import time
from pathlib import Path
from typing import Any, Callable, TypeVar
from urllib.parse import quote, urlsplit

import requests  # type: ignore[import-untyped]
from requests.adapters import HTTPAdapter  # type: ignore[import-untyped]

from src.jobs.protocol import (
    CLAIM_WAIT_SECONDS,
    ArtifactResponse,
    ClaimRequest,
    ClaimResponse,
    CompleteRequest,
    EnrollRequest,
    EnrollResponse,
    FailRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    JobAssignment,
    ProgressRequest,
    ProgressResponse,
    RegisterRequest,
    RegisterResponse,
    ReleaseRequest,
    TerminalResponse,
    UnregisterRequest,
)

logger = logging.getLogger(__name__)

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 30
UPLOAD_TIMEOUT = 600
BACKOFF_SECONDS = (1, 2, 4, 8, 16, 30)

T = TypeVar("T")


class WorkerClientError(Exception):
    """Base class for every error the client raises."""


class WorkerApiError(WorkerClientError):
    """The server answered with an error envelope."""

    def __init__(self, message: str, *, status: int = 0, code: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.code = code


class LeaseLostError(WorkerApiError):
    """The job is no longer leased to this worker (409 ``lease_lost``)."""


class UnknownSessionError(WorkerApiError):
    """The server does not know this session; register again (409 ``unknown_session``)."""


class NoDataAccessError(WorkerApiError):
    """The server refused a claim because the worker reported no data access."""


class WorkerRejectedError(WorkerApiError):
    """The worker is not allowed to talk to the server (401/403: revoked, disabled, wrong token)."""


class VersionMismatchError(WorkerRejectedError):
    """The worker and server versions differ (409 ``version_mismatch``)."""


class ServerUnavailableError(WorkerClientError):
    """The server could not be reached or answered 5xx; the caller retries with backoff."""


class CertificateMismatchError(WorkerClientError):
    """The server presented a certificate other than the pinned one."""


def normalize_fingerprint(value: str) -> str:
    return value.replace(":", "").strip().lower()


class _PinnedAdapter(HTTPAdapter):
    """Pins the server certificate by fingerprint instead of verifying a chain."""

    def __init__(self, fingerprint: str) -> None:
        self._fingerprint = fingerprint
        super().__init__(pool_connections=4, pool_maxsize=32)

    def init_poolmanager(self, connections: int, maxsize: int, block: bool = False, **pool_kwargs: Any) -> None:
        super().init_poolmanager(
            connections,
            maxsize,
            block,
            assert_fingerprint=self._fingerprint,
            cert_reqs="CERT_NONE",
            **pool_kwargs,
        )


def _make_session(fingerprint: str) -> requests.Session:
    session = requests.Session()
    session.trust_env = False  # never route the pinned connection through an environment proxy
    session.mount("https://", _PinnedAdapter(normalize_fingerprint(fingerprint)))
    return session


def _error_details(response: requests.Response) -> tuple[str, str]:
    try:
        body = response.json()
    except ValueError:
        return "", response.text[:300]
    if isinstance(body, dict):
        code = body.get("code") or ""
        detail = body.get("detail")
        if isinstance(detail, dict):  # FastAPI wraps unhandled HTTPException payloads
            code = code or detail.get("code") or ""
            detail = detail.get("detail")
        return str(code), str(detail or "")
    return "", str(body)[:300]


def _raise_for_status(response: requests.Response) -> None:
    status = response.status_code
    if status < 400:
        return
    code, detail = _error_details(response)
    message = detail or f"The server answered {status}"
    if status >= 500:
        raise ServerUnavailableError(f"{message} ({status})")
    if code == "version_mismatch":
        raise VersionMismatchError(message, status=status, code=code)
    if status in (401, 403):
        raise WorkerRejectedError(message, status=status, code=code)
    if code == "lease_lost":
        raise LeaseLostError(message, status=status, code=code)
    if code == "unknown_session":
        raise UnknownSessionError(message, status=status, code=code)
    if code == "no_data_access":
        raise NoDataAccessError(message, status=status, code=code)
    raise WorkerApiError(message, status=status, code=code)


class WorkerClient:
    def __init__(
        self,
        server_url: str,
        token: str | None,
        fingerprint: str,
        *,
        claim_wait_seconds: int = CLAIM_WAIT_SECONDS,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self._token = token
        self.fingerprint = normalize_fingerprint(fingerprint)
        self.claim_wait_seconds = claim_wait_seconds
        self._session = _make_session(self.fingerprint)

    # --- plumbing ---------------------------------------------------------

    def close(self) -> None:
        self._session.close()

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        # No idle keep-alive connections: the server's TLS shutdown would wait for one
        # until its graceful-shutdown timeout. The worker sends a few requests a minute.
        headers = {"Accept": "application/json", "Connection": "close"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        if extra:
            headers.update(extra)
        return headers

    def _send(
        self,
        method: str,
        path: str,
        *,
        timeout: tuple[float, float] = (CONNECT_TIMEOUT, READ_TIMEOUT),
        json: Any = None,
        data: Any = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        try:
            response = self._session.request(
                method,
                f"{self.server_url}{path}",
                json=json,
                data=data,
                headers=self._headers(headers),
                timeout=timeout,
                verify=False,  # replaced by fingerprint pinning in the adapter
            )
        except requests.exceptions.SSLError as exc:
            if "ingerprint" in str(exc):
                raise CertificateMismatchError(
                    "The server certificate does not match the one pinned at enrollment"
                ) from exc
            raise ServerUnavailableError(f"TLS error talking to the server: {exc}") from exc
        except requests.exceptions.RequestException as exc:
            raise ServerUnavailableError(f"Cannot reach the server: {exc}") from exc
        _raise_for_status(response)
        return response

    def _post(self, path: str, body: Any, **kwargs: Any) -> requests.Response:
        return self._send("POST", path, json=body.model_dump(mode="json"), **kwargs)

    # --- enrollment and registration --------------------------------------

    @staticmethod
    def enroll(server_url: str, fingerprint: str, request: EnrollRequest) -> EnrollResponse:
        client = WorkerClient(server_url, None, fingerprint)
        try:
            return EnrollResponse.model_validate(client._post("/api/worker/enroll", request).json())
        finally:
            client.close()

    def register(self, request: RegisterRequest) -> RegisterResponse:
        response = RegisterResponse.model_validate(self._post("/api/worker/register", request).json())
        self.claim_wait_seconds = response.claim_wait_seconds
        return response

    def unregister(self, request: UnregisterRequest) -> None:
        self._post("/api/worker/unregister", request, timeout=(CONNECT_TIMEOUT, 10))

    # --- jobs -------------------------------------------------------------

    def claim(self, request: ClaimRequest) -> JobAssignment | None:
        response = self._post(
            "/api/worker/claim", request, timeout=(CONNECT_TIMEOUT, self.claim_wait_seconds + 10)
        )
        if response.status_code == 204 or not response.content:
            return None
        return ClaimResponse.model_validate(response.json()).job

    def heartbeat(self, request: HeartbeatRequest) -> HeartbeatResponse:
        return HeartbeatResponse.model_validate(self._post("/api/worker/heartbeat", request).json())

    def progress(self, job_id: str, request: ProgressRequest) -> ProgressResponse:
        path = f"/api/worker/jobs/{quote(job_id, safe='')}/progress"
        return ProgressResponse.model_validate(self._post(path, request).json())

    def upload_artifact(self, job_id: str, lease_token: str, path: str | Path) -> ArtifactResponse:
        file_path = Path(path)
        content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
        url_path = f"/api/worker/jobs/{quote(job_id, safe='')}/artifacts/{quote(file_path.name, safe='')}"
        with file_path.open("rb") as stream:  # streamed, not read into memory
            response = self._send(
                "PUT",
                url_path,
                data=stream,
                headers={"X-Lease-Token": lease_token, "Content-Type": content_type},
                timeout=(CONNECT_TIMEOUT, UPLOAD_TIMEOUT),
            )
        return ArtifactResponse.model_validate(response.json())

    def complete(self, job_id: str, request: CompleteRequest) -> TerminalResponse:
        return self._terminal(job_id, "complete", request)

    def fail(self, job_id: str, request: FailRequest) -> TerminalResponse:
        return self._terminal(job_id, "fail", request)

    def release(self, job_id: str, request: ReleaseRequest) -> TerminalResponse:
        return self._terminal(job_id, "release", request)

    def _terminal(self, job_id: str, action: str, request: Any) -> TerminalResponse:
        path = f"/api/worker/jobs/{quote(job_id, safe='')}/{action}"
        return TerminalResponse.model_validate(self._post(path, request).json())


def retry_unavailable(
    call: Callable[[], T],
    *,
    stop: threading.Event | None = None,
    deadline: float | None = None,
    what: str = "request",
) -> T:
    """Run ``call``, retrying ``ServerUnavailableError`` with backoff 1, 2, 4 ... 30 s.

    Gives up (re-raising) once ``stop`` is set or ``deadline`` (monotonic) has passed.
    """
    attempt = 0
    while True:
        try:
            return call()
        except ServerUnavailableError as exc:
            delay = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            attempt += 1
            if (stop is not None and stop.is_set()) or (
                deadline is not None and time.monotonic() + delay > deadline
            ):
                raise
            logger.warning("%s failed (%s); retrying in %ss", what, exc, delay)
            if stop is not None:
                stop.wait(delay)
            else:
                time.sleep(delay)


def probe_certificate_fingerprint(server_url: str, timeout: float = 5.0) -> str:
    """The SHA-256 fingerprint of the certificate the server presents (no token needed)."""
    import hashlib

    parts = urlsplit(server_url)
    host = parts.hostname or "127.0.0.1"
    port = parts.port or 443
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, port), timeout=timeout) as raw:
        with context.wrap_socket(raw, server_hostname=host) as tls:
            der = tls.getpeercert(binary_form=True)
    if not der:
        raise OSError("The server presented no certificate")
    return hashlib.sha256(der).hexdigest()
