"""JobService: enqueue, run/wait, worker-side transitions, finalizers, notifier."""

from __future__ import annotations

import asyncio
import io
import threading
import time
from pathlib import Path
from uuid import uuid4

import pytest

from src.jobs import kinds as kind_registry
from src.jobs.errors import (
    ArtifactTooLarge,
    FinalizeError,
    InvalidArtifactName,
    JobCancelled,
    JobFailed,
    JobPending,
    LeaseLost,
    NoDataAccess,
    UnknownSession,
    VersionMismatch,
    WorkerDisabled,
)
from src.jobs.kinds import UnknownJobKind
from src.jobs.notifier import JobNotifier
from src.jobs.protocol import (
    ClaimRequest,
    CompleteRequest,
    FailRequest,
    HeartbeatRequest,
    JobLease,
    ProgressRequest,
    RegisterRequest,
    ReleaseRequest,
    UnregisterRequest,
)
from src.jobs.registry import WorkerRegistry
from src.jobs.service import (
    JobMaintenance,
    JobRecord,
    JobService,
    get_job_service,
    set_job_service,
)
from src.jobs.store import JobsStore
from src.paths import jobs_dir
from src.version import __version__


@pytest.fixture
def service(tmp_path):
    registry = WorkerRegistry(tmp_path / "app.db")
    registry.ensure_local_worker()
    return JobService(JobsStore(tmp_path / "jobs.db"), registry)


def connect(service, worker_id="local", *, data_access=True) -> str:
    session = str(uuid4())
    service.register(worker_id, RegisterRequest(session_id=session, version=__version__, hostname="h"))
    service.heartbeat(worker_id, HeartbeatRequest(session_id=session, data_access=data_access))
    return session


def claim_one(service, session, lanes=("interactive",), worker="local"):
    return service.claim_now(worker, ClaimRequest(session_id=session, lanes=list(lanes)))


def finish_later(service, session, *, how="complete", delay=0.15, worker="local"):
    """Claim the next job from another thread after ``delay`` and finish it."""

    def work():
        time.sleep(delay)
        deadline = time.monotonic() + 5
        job = None
        while job is None and time.monotonic() < deadline:
            job = claim_one(service, session, ("batch", "interactive"), worker)
            time.sleep(0.01)
        assert job is not None
        if how == "complete":
            service.complete(worker, job.job_id, CompleteRequest(lease_token=job.lease_token, result={"v": 1}))
        elif how == "fail":
            service.fail(worker, job.job_id, FailRequest(lease_token=job.lease_token, error="It broke"))
        elif how == "cancel":
            service.cancel(job.job_id)
            service.fail(worker, job.job_id, FailRequest(lease_token=job.lease_token, cancelled=True))
        elif how == "interrupt":
            service.release(worker, job.job_id, ReleaseRequest(lease_token=job.lease_token, reason="Stopped"))

    thread = threading.Thread(target=work)
    thread.start()
    return thread


# -- enqueue ---------------------------------------------------------------------


def test_enqueue_uses_the_kind_defaults(service):
    record = service.enqueue(
        "pipeline", {"steps": []}, owner_user_id="u1", title="Nightly",
        steps=[{"name": "a", "overwrite": True}, {"name": "b"}],
    )

    assert isinstance(record, JobRecord)
    assert (record.kind, record.lane, record.status) == ("pipeline", "batch", "open")
    assert record.exclusive == "filings,market"
    assert record.max_attempts == 1 and record.notify is True
    assert record.payload == {"steps": []} and record.result is None and record.progress is None
    assert record.owner_user_id == "u1" and record.title == "Nightly"
    assert record.public_status == "pending" and not record.is_terminal
    assert (jobs_dir() / record.job_id).is_dir()
    assert [(s["step_name"], s["overwrite"]) for s in service.steps(record.job_id)] == [("a", 1), ("b", 0)]


def test_enqueue_overrides_and_retry_defaults(service):
    record = service.enqueue("echo", {}, owner_user_id=None, title="t", notify=True, priority=4)
    assert (record.lane, record.max_attempts, record.notify, record.priority) == ("interactive", 3, True, 4)
    assert record.exclusive == ""
    assert service.enqueue("echo", {}, owner_user_id=None, title="t").notify is False


def test_enqueue_rejects_unknown_kinds_and_bad_ids(service):
    with pytest.raises(UnknownJobKind):
        service.enqueue("nope", {}, owner_user_id=None, title="t")
    with pytest.raises(ValueError):
        service.enqueue("echo", {}, owner_user_id=None, title="t", job_id="../../etc")
    given = str(uuid4())
    assert service.enqueue("echo", {}, owner_user_id=None, title="t", job_id=given).job_id == given


def test_enqueue_dedupes_active_jobs(service):
    first = service.enqueue("description_fetch", {}, owner_user_id=None, title="t", dedupe_key="d:1")
    second = service.enqueue("description_fetch", {}, owner_user_id=None, title="t", dedupe_key="d:1")
    assert first.job_id == second.job_id


def test_titles_are_redacted(service):
    record = service.enqueue("echo", {}, owner_user_id=None, title="Run api_key=abc123 now")
    assert "abc123" not in record.title


def test_get_and_list(service):
    a = service.enqueue("echo", {}, owner_user_id="u1", title="a")
    b = service.enqueue("echo", {}, owner_user_id="u2", title="b")
    c = service.enqueue("echo", {}, owner_user_id=None, title="c")
    p = service.enqueue("pipeline", {}, owner_user_id="u3", title="p")

    with pytest.raises(KeyError):
        service.get("missing")
    assert service.get(a.job_id).title == "a"
    ids = lambda **kw: {r.job_id for r in service.list_jobs(**kw)}  # noqa: E731
    assert ids() == {a.job_id, b.job_id, c.job_id, p.job_id}
    assert ids(owner_user_id="u1") == {a.job_id}
    assert ids(owner_user_id="u1", include_system=True) == {a.job_id, c.job_id}
    assert ids(owner_user_id="u1", include_system=True, also_kinds=["pipeline"]) == {a.job_id, c.job_id, p.job_id}
    assert ids(owner_user_id=None) == {c.job_id}
    assert ids(kinds=["pipeline"]) == {p.job_id}
    assert ids(limit=2, offset=0) <= ids()
    assert len(service.list_jobs(limit=2)) == 2
    service.cancel(a.job_id)
    assert a.job_id not in ids(active_only=True)
    assert [r.job_id for r in service.list_jobs()][0] == p.job_id  # newest first


# -- run / wait -------------------------------------------------------------------


def test_run_sync_without_a_worker_is_pending_at_once(service):
    started = time.monotonic()
    with pytest.raises(JobPending) as raised:
        service.run_sync("echo", {}, owner_user_id="u", title="t", timeout=30)

    assert time.monotonic() - started < 2
    assert raised.value.waiting_for_worker is True
    assert service.get(raised.value.job_id).notify is True


def test_run_sync_completed(service):
    session = connect(service)
    thread = finish_later(service, session)

    record = service.run_sync("echo", {"x": 1}, owner_user_id="u", title="t", timeout=10)
    thread.join()

    assert record.status == "completed" and record.result == {"v": 1}
    assert record.notify is False


def test_run_sync_failed_cancelled_and_interrupted(service):
    session = connect(service)
    for how, expected in (("fail", JobFailed), ("cancel", JobCancelled), ("interrupt", JobFailed)):
        kind = "price_refresh" if how == "interrupt" else "echo"  # not retryable: release interrupts
        thread = finish_later(service, session, how=how)
        with pytest.raises(expected) as raised:
            service.run_sync(kind, {}, owner_user_id="u", title="t", timeout=10)
        thread.join()
        if how == "fail":
            assert raised.value.message == "It broke"
        if how == "interrupt":
            assert raised.value.message == "Stopped"
        assert raised.value.job_id


def test_run_sync_timeout_marks_notify(service):
    connect(service)

    with pytest.raises(JobPending) as raised:
        service.run_sync("echo", {}, owner_user_id="u", title="t", timeout=0.3)

    assert raised.value.waiting_for_worker is False  # a worker is online, just busy
    assert service.get(raised.value.job_id).notify is True
    assert service.get(raised.value.job_id).status == "open"


def test_run_async_outcomes(service):
    session = connect(service)

    async def scenario():
        thread = finish_later(service, session)
        done = await service.run("echo", {}, owner_user_id="u", title="t", timeout=10)
        thread.join()
        thread = finish_later(service, session, how="fail")
        with pytest.raises(JobFailed):
            await service.run("echo", {}, owner_user_id="u", title="t", timeout=10)
        thread.join()
        thread = finish_later(service, session, how="cancel")
        with pytest.raises(JobCancelled):
            await service.run("echo", {}, owner_user_id="u", title="t", timeout=10)
        thread.join()
        with pytest.raises(JobPending) as pending:
            await service.run("echo", {}, owner_user_id="u", title="t", timeout=0.3)
        return done, pending.value

    done, pending = asyncio.run(scenario())
    assert done.status == "completed"
    assert pending.waiting_for_worker is False
    assert service.get(pending.job_id).notify is True


def test_run_async_without_a_worker_is_pending_at_once(service):
    async def scenario():
        with pytest.raises(JobPending) as pending:
            await service.run("echo", {}, owner_user_id="u", title="t", timeout=30)
        return pending.value

    started = time.monotonic()
    pending = asyncio.run(scenario())
    assert time.monotonic() - started < 2 and pending.waiting_for_worker is True


def test_wait_returns_the_current_record_on_timeout(service):
    record = service.enqueue("echo", {}, owner_user_id="u", title="t")
    assert service.wait_sync(record.job_id, 0.2).status == "open"
    assert asyncio.run(service.wait(record.job_id, 0.2)).status == "open"


def test_lane_served_rules(service, tmp_path):
    assert not service.lane_served("interactive")
    session = connect(service)
    assert service.lane_served("interactive") and service.lane_served("batch")

    service.registry.update("local", batch_slots=0)
    connect(service)  # re-register picks up the new slots
    assert service.lane_served("interactive") and not service.lane_served("batch")

    service.registry.update("local", interactive_slots=0, batch_slots=2)
    connect(service)
    assert service.lane_served("interactive") and service.lane_served("batch")  # batch slots serve both

    service.registry.update("local", enabled=False)
    assert not service.lane_served("interactive")
    assert session


def test_stale_sessions_are_offline(service):
    connect(service)
    assert service.capacity()["online"] == 1
    service.heartbeat_seconds = 0
    time.sleep(0.01)
    assert service.capacity() == {"online": 0, "interactive_slots": 0, "batch_slots": 0}


def test_capacity_and_health_summary(service):
    connect(service)
    service.enqueue("echo", {}, owner_user_id=None, title="queued")
    running = service.enqueue("echo", {}, owner_user_id=None, title="running")
    service.cancel(running.job_id)

    assert service.capacity() == {"online": 1, "interactive_slots": 1, "batch_slots": 2}
    summary = service.health_summary()
    assert summary == {"queue_depth": 1, "active": 0, "counts_by_status": {"open": 1, "cancelled": 1},
                       "workers_online": 1}


# -- worker side -------------------------------------------------------------------


def test_register_checks_version_and_worker_state(service):
    with pytest.raises(VersionMismatch):
        service.register("local", RegisterRequest(session_id="session-1", version="0.0.0"))
    with pytest.raises(WorkerDisabled):
        service.register("unknown", RegisterRequest(session_id="session-1", version=__version__))
    service.registry.update("local", enabled=False)
    with pytest.raises(WorkerDisabled):
        service.register("local", RegisterRequest(session_id="session-1", version=__version__))


def test_register_response(service):
    response = service.register("local", RegisterRequest(session_id="session-1", version=__version__))

    assert response.worker_id == "local" and response.server_version == __version__
    assert (response.lease_seconds, response.heartbeat_seconds, response.claim_wait_seconds) == (60, 15, 25)
    assert (response.interactive_slots, response.batch_slots) == (1, 2)
    assert response.paths.jobs_dir == str(jobs_dir())
    assert response.paths.market_db.endswith("market.db") and response.paths.filings_db.endswith("filings.db")
    assert set(response.settings) == {
        "edinet.api_key", "limits.max_backtest_artifact_bytes",
        "limits.max_upload_bytes", "limits.max_export_bytes", "limits.max_report_artifact_bytes",
    }


def test_heartbeat_reports_leases_and_shutdown(service):
    session = connect(service)
    service.enqueue("echo", {}, owner_user_id=None, title="t")
    job = claim_one(service, session)

    response = service.heartbeat(
        "local",
        HeartbeatRequest(
            session_id=session, data_access=True,
            jobs=[JobLease(job_id=job.job_id, lease_token=job.lease_token), JobLease(job_id="x", lease_token="y")],
        ),
    )
    assert [s.lease_ok for s in response.jobs] == [True, False]
    assert response.shutdown is False
    service.registry.update("local", enabled=False)
    again = service.heartbeat("local", HeartbeatRequest(session_id=session, data_access=True))
    assert again.shutdown is True and again.config_version > response.config_version
    with pytest.raises(UnknownSession):
        service.heartbeat("local", HeartbeatRequest(session_id="unknown", data_access=True))


def test_claim_needs_data_access_and_an_enabled_worker(service):
    session = connect(service, data_access=False)
    service.enqueue("echo", {}, owner_user_id=None, title="t")
    with pytest.raises(NoDataAccess):
        claim_one(service, session)
    service.heartbeat("local", HeartbeatRequest(session_id=session, data_access=True))
    service.registry.update("local", enabled=False)
    with pytest.raises(WorkerDisabled):
        claim_one(service, session)


def test_assignment_carries_the_job(service):
    session = connect(service)
    queued = service.enqueue("echo", {"a": [1]}, owner_user_id="u1", title="Hello")

    job = claim_one(service, session)

    assert (job.job_id, job.kind, job.lane, job.title, job.attempt) == (queued.job_id, "echo", "interactive", "Hello", 1)
    assert job.payload == {"a": [1]} and job.owner_user_id == "u1"
    assert job.workspace == str(jobs_dir() / queued.job_id) and Path(job.workspace).is_dir()
    assert service.get(queued.job_id).status == "in_progress"


def test_progress_response_reports_cancel_requests(service):
    session = connect(service)
    service.enqueue("echo", {}, owner_user_id=None, title="t")
    job = claim_one(service, session)

    first = service.progress("local", job.job_id, ProgressRequest(
        lease_token=job.lease_token, progress_percent=30, status_message="Working", progress={"phase": "x"}))
    assert first.cancel == "none"
    record = service.get(job.job_id)
    assert (record.progress_percent, record.status_message, record.progress) == (30, "Working", {"phase": "x"})
    service.cancel(job.job_id, force=True)
    assert service.progress("local", job.job_id, ProgressRequest(lease_token=job.lease_token)).cancel == "hard"
    with pytest.raises(LeaseLost):
        service.progress("local", job.job_id, ProgressRequest(lease_token="stale"))


def test_unregister_ends_the_session(service):
    session = connect(service)
    service.unregister("local", UnregisterRequest(session_id=session))
    assert service.capacity()["online"] == 0
    with pytest.raises(UnknownSession):
        service.heartbeat("local", HeartbeatRequest(session_id=session, data_access=True))


def test_claim_long_poll_hands_out_a_job_enqueued_during_the_wait(service):
    session = connect(service)

    async def scenario():
        request = ClaimRequest(session_id=session, lanes=["interactive"])
        claiming = asyncio.create_task(service.claim("local", request, wait_seconds=5))
        await asyncio.sleep(0.3)
        enqueued_at = time.monotonic()
        await asyncio.to_thread(service.enqueue, "echo", {}, owner_user_id=None, title="late")
        job = await claiming
        return job, time.monotonic() - enqueued_at

    job, latency = asyncio.run(scenario())
    assert job is not None and job.title == "late"
    assert latency < 0.25


def test_claim_long_poll_gives_up_after_the_wait(service):
    session = connect(service)
    started = time.monotonic()
    job = asyncio.run(service.claim("local", ClaimRequest(session_id=session, lanes=["interactive"]), wait_seconds=0.4))

    assert job is None and 0.35 < time.monotonic() - started < 2


def test_claim_long_poll_stops_when_the_caller_is_gone(service):
    session = connect(service)

    async def gone():
        return True

    started = time.monotonic()
    job = asyncio.run(service.claim("local", ClaimRequest(session_id=session, lanes=["interactive"]),
                                    wait_seconds=10, should_stop=gone))
    assert job is None and time.monotonic() - started < 2


def test_a_stopping_server_hands_out_nothing_and_ends_waiting_claims(service):
    session = connect(service)
    request = ClaimRequest(session_id=session, lanes=["interactive"])

    async def scenario():
        claiming = asyncio.create_task(service.claim("local", request, wait_seconds=10))
        await asyncio.sleep(0.2)
        service.begin_shutdown()
        started = time.monotonic()
        return await claiming, time.monotonic() - started

    job, waited = asyncio.run(scenario())
    assert job is None and waited < 1.5
    service.enqueue("echo", {}, owner_user_id=None, title="late")
    assert service.claim_now("local", request) is None


def test_release_worker_jobs_settles_what_a_stopped_worker_held(service):
    session = connect(service)
    retryable = service.enqueue("echo", {}, owner_user_id=None, title="a").job_id
    one_shot = service.enqueue("price_refresh", {"tickers": []}, owner_user_id=None, title="b").job_id
    cancelling = service.enqueue("echo", {}, owner_user_id=None, title="c").job_id
    for _ in range(3):
        assert claim_one(service, session) is not None
    service.cancel(cancelling)

    assert service.release_worker_jobs("local") == 3

    assert service.get(retryable).status == "open" and service.get(retryable).attempt == 0
    assert service.get(one_shot).status == "interrupted"
    assert service.get(one_shot).error_message == "The server was stopped"
    assert service.get(cancelling).status == "cancelled"
    assert service.capacity()["online"] == 0


def test_a_released_exclusive_resource_wakes_waiting_claims(service):
    session = connect(service)
    first = service.enqueue("split_detection", {}, owner_user_id=None, title="one")
    service.enqueue("split_detection", {}, owner_user_id=None, title="two")
    held = claim_one(service, session, ("batch",))
    assert held.job_id == first.job_id

    async def scenario():
        request = ClaimRequest(session_id=session, lanes=["batch"])
        waiting = asyncio.create_task(service.claim("local", request, wait_seconds=5))
        await asyncio.sleep(0.2)
        await asyncio.to_thread(
            service.complete, "local", held.job_id, CompleteRequest(lease_token=held.lease_token))
        return await waiting

    assert asyncio.run(scenario()).title == "two"


# -- complete / finalizers --------------------------------------------------------------


def _claimed(service, kind="echo", **kw):
    session = connect(service)
    service.enqueue(kind, {}, owner_user_id="u", title="t", **kw)
    lanes = ("batch", "interactive")
    return session, claim_one(service, session, lanes)


def test_complete_without_a_finalizer_stores_the_worker_result(service):
    _, job = _claimed(service)

    response = service.complete("local", job.job_id, CompleteRequest(lease_token=job.lease_token, result={"echo": 1}))

    assert response.status == "completed"
    record = service.get(job.job_id)
    assert record.result == {"echo": 1} and record.progress_percent == 100 and record.completed_at


def test_complete_is_idempotent(service):
    _, job = _claimed(service)
    request = CompleteRequest(lease_token=job.lease_token, result={"echo": 1})

    assert service.complete("local", job.job_id, request).status == "completed"
    assert service.complete("local", job.job_id, request).status == "completed"
    assert service.get(job.job_id).result == {"echo": 1}
    with pytest.raises(LeaseLost):
        service.complete("local", job.job_id, CompleteRequest(lease_token="other"))


def test_finalizer_result_replaces_the_worker_result(service, monkeypatch):
    seen = {}

    def finalize(job, result, artifacts):
        seen.update(job=job, result=result, artifacts=artifacts)
        return {"public": True}

    monkeypatch.setattr(kind_registry, "finalizer_for", lambda kind: finalize)
    _, job = _claimed(service)
    service.store_artifact("local", job.job_id, job.lease_token, "out.txt", io.BytesIO(b"abc"))

    service.complete("local", job.job_id, CompleteRequest(lease_token=job.lease_token, result={"raw": 1}))

    assert service.get(job.job_id).result == {"public": True}
    assert isinstance(seen["job"], JobRecord) and seen["job"].job_id == job.job_id
    assert seen["result"] == {"raw": 1}
    assert seen["artifacts"] == {"out.txt": jobs_dir() / job.job_id / "artifacts" / "out.txt"}
    assert seen["artifacts"]["out.txt"].read_bytes() == b"abc"


def test_finalize_error_fails_the_job_with_its_message(service, monkeypatch):
    def finalize(job, result, artifacts):
        raise FinalizeError("The backtest archive was invalid")

    monkeypatch.setattr(kind_registry, "finalizer_for", lambda kind: finalize)
    _, job = _claimed(service)

    response = service.complete("local", job.job_id, CompleteRequest(lease_token=job.lease_token))

    assert response.status == "failed"
    assert service.get(job.job_id).error_message == "The backtest archive was invalid"


def test_finalizer_crash_fails_the_job_generically(service, monkeypatch, caplog):
    def finalize(job, result, artifacts):
        raise RuntimeError("secret internals")

    monkeypatch.setattr(kind_registry, "finalizer_for", lambda kind: finalize)
    _, job = _claimed(service)

    with caplog.at_level("ERROR"):
        response = service.complete("local", job.job_id, CompleteRequest(lease_token=job.lease_token))

    assert response.status == "failed"
    record = service.get(job.job_id)
    assert record.error_message == "The result could not be saved"
    assert "secret internals" not in (record.error_message or "")
    assert "secret internals" in caplog.text


def test_an_unimportable_finalizer_fails_the_job(service, monkeypatch):
    def broken(kind):
        raise ModuleNotFoundError("nope")

    monkeypatch.setattr(kind_registry, "finalizer_for", broken)
    _, job = _claimed(service)

    assert service.complete("local", job.job_id, CompleteRequest(lease_token=job.lease_token)).status == "failed"


def test_oversized_results_fail_the_job(service, monkeypatch):
    monkeypatch.setattr("src.jobs.service.MAX_RESULT_BYTES", 100)
    _, job = _claimed(service)

    response = service.complete("local", job.job_id, CompleteRequest(lease_token=job.lease_token, result={"x": "y" * 500}))

    assert response.status == "failed"
    assert service.get(job.job_id).error_message == "The result was too large"


def test_fail_retry_and_release_via_the_service(service):
    session, job = _claimed(service)
    assert service.fail("local", job.job_id, FailRequest(lease_token=job.lease_token, retryable=True)).status == "open"
    again = claim_one(service, session)
    assert again.attempt == 2
    assert service.release("local", again.job_id, ReleaseRequest(lease_token=again.lease_token)).status == "open"
    third = claim_one(service, session)
    assert third.attempt == 2  # the release did not use an attempt
    failed = service.fail("local", third.job_id, FailRequest(lease_token=third.lease_token, error="x" * 1000))
    assert failed.status == "failed"
    assert len(service.get(third.job_id).error_message) <= 1000


# -- artifacts ------------------------------------------------------------------------------


def test_store_artifact_from_a_stream_and_a_path(service, tmp_path):
    _, job = _claimed(service)

    first = service.store_artifact("local", job.job_id, job.lease_token, "a.bin", io.BytesIO(b"hello"), "text/plain")
    source = tmp_path / "upload.part"
    source.write_bytes(b"replacement!")
    second = service.store_artifact("local", job.job_id, job.lease_token, "a.bin", source)

    import hashlib

    assert first.size_bytes == 5 and first.sha256 == hashlib.sha256(b"hello").hexdigest()
    assert second.size_bytes == 12 and not source.exists()
    stored = jobs_dir() / job.job_id / "artifacts" / "a.bin"
    assert stored.read_bytes() == b"replacement!"
    assert service.artifacts(job.job_id) == {"a.bin": stored}
    assert not list(stored.parent.glob("*.part"))


def test_store_artifact_validates(service, tmp_path):
    _, job = _claimed(service)
    for name in ("../x", ".hidden", "a/b", "", "x" * 101, "a b"):
        with pytest.raises(InvalidArtifactName):
            service.store_artifact("local", job.job_id, job.lease_token, name, io.BytesIO(b"x"))
    with pytest.raises(ArtifactTooLarge):
        service.store_artifact("local", job.job_id, job.lease_token, "big", io.BytesIO(b"x" * 20), max_bytes=10)
    big = tmp_path / "big.part"
    big.write_bytes(b"x" * 20)
    with pytest.raises(ArtifactTooLarge):
        service.store_artifact("local", job.job_id, job.lease_token, "big", big, max_bytes=10)
    assert not big.exists() and not (jobs_dir() / job.job_id / "artifacts" / "big").exists()
    with pytest.raises(LeaseLost):
        service.store_artifact("local", job.job_id, "stale", "ok", io.BytesIO(b"x"))
    with pytest.raises(KeyError):
        service.store_artifact("local", str(uuid4()), "t", "ok", io.BytesIO(b"x"))


# -- maintenance ----------------------------------------------------------------------------


def test_sweep_requeues_after_lease_expiry(service):
    session, job = _claimed(service)
    assert service.sweep() == 0  # the lease is still live
    import src.jobs.store as store_module

    assert service.store.sweep(now=store_module.plus_seconds(store_module.utc_now(), 120)) == 1
    assert service.get(job.job_id).status == "open"
    assert session


def test_cleanup_removes_old_finished_jobs_and_their_folders(service, tmp_path):
    old = service.enqueue("echo", {}, owner_user_id=None, title="old")
    fresh = service.enqueue("echo", {}, owner_user_id=None, title="fresh")
    active = service.enqueue("echo", {}, owner_user_id=None, title="active")
    import src.jobs.store as store_module

    service.store.request_cancel(old.job_id, now=store_module.plus_seconds(store_module.utc_now(), -3 * 3600))
    service.cancel(fresh.job_id)
    (jobs_dir() / old.job_id / "artifacts").mkdir(parents=True, exist_ok=True)
    (jobs_dir() / old.job_id / "artifacts" / "f").write_text("x")
    stray = jobs_dir() / "not-a-uuid-folder"
    stray.mkdir(parents=True, exist_ok=True)

    assert service.cleanup(1) == 1

    with pytest.raises(KeyError):
        service.get(old.job_id)
    assert not (jobs_dir() / old.job_id).exists()
    assert (jobs_dir() / fresh.job_id).exists() and (jobs_dir() / active.job_id).exists()
    assert stray.exists()


def test_maintenance_thread_sweeps(service):
    session, job = _claimed(service)
    with service.store.transaction() as conn:
        conn.execute("UPDATE jobs SET lease_expires_at = '2000-01-01T00:00:00+00:00'")
    maintenance = JobMaintenance(service, 24, sweep_interval=0.05)
    maintenance.start()
    try:
        deadline = time.monotonic() + 5
        while service.get(job.job_id).status != "open" and time.monotonic() < deadline:
            time.sleep(0.05)
    finally:
        maintenance.stop()
    assert service.get(job.job_id).status == "open" and session


def test_singleton_can_be_replaced(service):
    set_job_service(service)
    try:
        assert get_job_service() is service
    finally:
        set_job_service(None)


# -- notifier ---------------------------------------------------------------------------------


def test_notifier_wakes_threads_from_another_thread():
    notifier = JobNotifier()
    since = notifier.version
    threading.Timer(0.1, notifier.bump).start()
    started = time.monotonic()

    assert notifier.wait_for_change_sync(since, 5) == since + 1
    assert time.monotonic() - started < 1


def test_notifier_times_out_and_skips_missed_changes():
    notifier = JobNotifier()
    started = time.monotonic()
    assert notifier.wait_for_change_sync(notifier.version, 0.2) == notifier.version
    assert time.monotonic() - started >= 0.15
    since = notifier.version
    notifier.bump()
    assert notifier.wait_for_change_sync(since, 5) == since + 1  # no wait: already changed
    assert asyncio.run(notifier.wait_for_change(since, 5)) == since + 1


def test_notifier_wakes_asyncio_waiters_from_a_thread():
    notifier = JobNotifier()

    async def scenario():
        since = notifier.version
        waiters = [asyncio.create_task(notifier.wait_for_change(since, 5)) for _ in range(3)]
        await asyncio.sleep(0.05)
        started = time.monotonic()
        threading.Thread(target=notifier.bump).start()
        results = await asyncio.gather(*waiters)
        return results, time.monotonic() - started, since

    results, elapsed, since = asyncio.run(scenario())
    assert results == [since + 1] * 3 and elapsed < 1


def test_notifier_async_timeout():
    notifier = JobNotifier()

    async def scenario():
        started = time.monotonic()
        value = await notifier.wait_for_change(notifier.version, 0.2)
        return value, time.monotonic() - started

    value, elapsed = asyncio.run(scenario())
    assert value == notifier.version and elapsed >= 0.15
