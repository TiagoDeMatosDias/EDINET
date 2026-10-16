"""WorkerClient against a fake protocol server over pinned HTTPS."""

from __future__ import annotations

import warnings

import pytest

from src.jobs.protocol import (
    ClaimRequest,
    CompleteRequest,
    EnrollRequest,
    FailRequest,
    HeartbeatRequest,
    ProgressRequest,
    RegisterRequest,
    UnregisterRequest,
)
from src.version import __version__
from src.worker.client import (
    CertificateMismatchError,
    LeaseLostError,
    NoDataAccessError,
    ServerUnavailableError,
    UnknownSessionError,
    VersionMismatchError,
    WorkerClient,
    WorkerRejectedError,
    retry_unavailable,
)
from tests.worker_fakes import TOKEN, FakeServer

SESSION = "session-0001"


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    instance = FakeServer(tmp_path_factory.mktemp("fake-server")).start()
    yield instance
    instance.stop()


@pytest.fixture
def client(server):
    instance = WorkerClient(server.url, TOKEN, server.fingerprint)
    yield instance
    instance.close()


def _register(client):
    return client.register(RegisterRequest(session_id=SESSION, version=__version__, kinds=["echo"]))


def test_pinned_fingerprint_connects_without_warnings(server, client):
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        response = _register(client)

    assert response.worker_id == "w-test" and response.paths.market_db.endswith("market.db")
    assert client.claim_wait_seconds == 1


def test_fingerprint_with_colons_and_uppercase_is_accepted(server):
    spaced = ":".join(server.fingerprint[i:i + 2].upper() for i in range(0, 64, 2))
    client = WorkerClient(server.url, TOKEN, spaced)
    try:
        assert _register(client).worker_id == "w-test"
    finally:
        client.close()


def test_wrong_fingerprint_fails_and_is_not_retried_as_an_outage(server):
    client = WorkerClient(server.url, TOKEN, "00" * 32)
    try:
        with pytest.raises(CertificateMismatchError):
            _register(client)
    finally:
        client.close()


def test_unreachable_server_is_unavailable(server):
    client = WorkerClient("https://127.0.0.1:1", TOKEN, server.fingerprint)
    try:
        with pytest.raises(ServerUnavailableError):
            _register(client)
    finally:
        client.close()


def test_claim_returns_none_on_204_and_an_assignment_otherwise(server, client):
    _register(client)
    server.state.data_access = True
    request = ClaimRequest(session_id=SESSION, lanes=["interactive"], kinds=["echo"])

    assert client.claim(request) is None
    job = server.state.enqueue("echo", {"result": 5})
    assignment = client.claim(request)

    assert assignment is not None and assignment.job_id == job.job_id and assignment.payload == {"result": 5}
    assert client.progress(job.job_id, ProgressRequest(lease_token=assignment.lease_token, progress_percent=50)).cancel == "none"
    assert client.complete(job.job_id, CompleteRequest(lease_token=assignment.lease_token, result={"x": 1})).status == "completed"
    assert server.state.jobs[job.job_id].result == {"x": 1}


def test_error_envelopes_map_to_exceptions(server, client):
    _register(client)
    server.state.data_access = False

    with pytest.raises(NoDataAccessError):
        client.claim(ClaimRequest(session_id=SESSION, lanes=["interactive"]))
    with pytest.raises(UnknownSessionError):
        client.heartbeat(HeartbeatRequest(session_id="unknown-session"))
    with pytest.raises(LeaseLostError):
        client.fail("nope", FailRequest(lease_token="x"))
    with pytest.raises(LeaseLostError):
        client.progress("nope", ProgressRequest(lease_token="x"))

    bad = WorkerClient(server.url, "srw_wrong", server.fingerprint)
    try:
        with pytest.raises(WorkerRejectedError):
            _register(bad)
    finally:
        bad.close()

    with pytest.raises(VersionMismatchError):
        client.register(RegisterRequest(session_id=SESSION, version="0.0.0"))

    server.state.register_error = (403, "worker_disabled")
    try:
        with pytest.raises(WorkerRejectedError):
            _register(client)
    finally:
        server.state.register_error = None


def test_server_errors_are_unavailable(server, client):
    server.state.register_error = (503, "unavailable")
    try:
        with pytest.raises(ServerUnavailableError):
            _register(client)
    finally:
        server.state.register_error = None


def test_enroll_needs_no_token(server):
    response = WorkerClient.enroll(
        server.url, server.fingerprint, EnrollRequest(code="good-code-1", name="n", hostname="h", version=__version__)
    )
    assert response.token == TOKEN

    with pytest.raises(WorkerRejectedError):
        WorkerClient.enroll(
            server.url, server.fingerprint, EnrollRequest(code="nope-code-1", name="n", hostname="h", version=__version__)
        )


def test_upload_streams_the_file_with_lease_token_and_content_type(server, client, tmp_path):
    _register(client)
    server.state.data_access = True
    job = server.state.enqueue("echo")
    assignment = client.claim(ClaimRequest(session_id=SESSION, lanes=["interactive"], kinds=["echo"]))
    assert assignment is not None
    payload = tmp_path / "result.json"
    payload.write_bytes(b'{"a": 1}' * 100_000)

    uploaded = client.upload_artifact(job.job_id, assignment.lease_token, payload)

    assert uploaded.name == "result.json" and uploaded.size_bytes == 800_000
    assert server.state.jobs[job.job_id].artifacts["result.json"] == payload.read_bytes()
    assert {"artifact": "result.json", "content_type": "application/json"} in server.state.jobs[job.job_id].progress
    with pytest.raises(LeaseLostError):
        client.upload_artifact(job.job_id, "stale", payload)


def test_unregister_returns_nothing(server, client):
    _register(client)

    assert client.unregister(UnregisterRequest(session_id=SESSION)) is None
    assert server.state.unregistered


def test_retry_unavailable_backs_off_then_succeeds(monkeypatch):
    attempts = []
    sleeps = []
    monkeypatch.setattr("time.sleep", sleeps.append)

    def flaky():
        attempts.append(1)
        if len(attempts) < 4:
            raise ServerUnavailableError("down")
        return "ok"

    assert retry_unavailable(flaky) == "ok"
    assert sleeps == [1, 2, 4]


def test_retry_unavailable_gives_up_when_stopped():
    import threading

    stop = threading.Event()
    stop.set()

    def down():
        raise ServerUnavailableError("down")

    with pytest.raises(ServerUnavailableError):
        retry_unavailable(down, stop=stop)
