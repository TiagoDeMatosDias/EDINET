"""The jobs.db store: claiming, leases, sweeping, cancelling, cleanup, legacy import."""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from src.jobs.errors import InvalidJobState, LeaseLost, NoDataAccess, UnknownSession
from src.jobs.legacy import import_legacy_pipeline_jobs
from src.jobs.protocol import StepEvent
from src.jobs.store import JobsStore, plus_seconds, utc_now

LEASE = 60


@pytest.fixture
def store(tmp_path):
    return JobsStore(tmp_path / "jobs.db")


def add_job(store, kind="echo", lane="interactive", *, priority=0, exclusive="", max_attempts=3,
            dedupe_key=None, steps=None, now=None):
    row, _ = store.insert_job(
        job_id=str(uuid4()), kind=kind, lane=lane, title=kind, owner_user_id="u1",
        priority=priority, payload={"a": 1}, exclusive=exclusive, max_attempts=max_attempts,
        notify=False, dedupe_key=dedupe_key, steps=steps, now=now,
    )
    return row["job_id"]


def add_session(store, worker="w1", session=None, *, data_access=True):
    session = session or str(uuid4())
    store.upsert_session(
        worker_id=worker, session_id=session, hostname="h", pid=1, platform="p", version="1",
        interactive_slots=1, batch_slots=2,
    )
    if data_access:
        beat(store, worker, session)
    return session


def beat(store, worker, session, leases=(), data_access=True, now=None):
    return store.heartbeat(
        worker_id=worker, session_id=session, data_access=data_access, data_access_detail=None,
        interactive_busy=0, batch_busy=0, leases=list(leases), lease_seconds=LEASE, now=now,
    )


def claim(store, session, lanes=("interactive",), worker="w1", kinds=(), now=None):
    return store.claim(
        worker_id=worker, session_id=session, lanes=list(lanes), kinds=kinds,
        lease_seconds=LEASE, now=now,
    )


def test_claim_orders_by_priority_then_age(store):
    session = add_session(store)
    old = add_job(store, now="2026-01-01T00:00:00.000000+00:00")
    new = add_job(store, now="2026-01-02T00:00:00.000000+00:00")
    urgent = add_job(store, priority=5, now="2026-01-03T00:00:00.000000+00:00")

    order = [claim(store, session)["job_id"] for _ in range(3)]

    assert order == [urgent, old, new]
    assert claim(store, session) is None


def test_claim_sets_the_lease_fields(store):
    session = add_session(store)
    job_id = add_job(store)
    row = claim(store, session)

    assert row["job_id"] == job_id
    assert row["status"] == "in_progress"
    assert row["attempt"] == 1
    assert row["worker_id"] == "w1" and row["session_id"] == session
    assert row["lease_token"] and row["lease_expires_at"] > utc_now()
    assert row["started_at"] == row["claimed_at"]


def test_lane_preference_follows_the_request_order(store):
    session = add_session(store)
    interactive = add_job(store, lane="interactive", kind="echo")
    batch = add_job(store, lane="batch", kind="backtest_rolling")

    assert claim(store, session, ("batch", "interactive"))["job_id"] == batch
    assert claim(store, session, ("batch", "interactive"))["job_id"] == interactive
    assert claim(store, session, ("batch", "interactive")) is None


def test_interactive_only_session_never_gets_batch_jobs(store):
    session = add_session(store)
    add_job(store, lane="batch", kind="backtest_rolling")

    assert claim(store, session, ("interactive",)) is None


def test_kind_filter(store):
    session = add_session(store)
    add_job(store, kind="echo")
    wanted = add_job(store, kind="screening")

    assert claim(store, session, kinds=["screening"])["job_id"] == wanted
    assert claim(store, session, kinds=["screening"]) is None


def test_exclusive_resources_block_overlapping_claims(store):
    session = add_session(store)
    first = add_job(store, kind="pipeline", lane="batch", exclusive="filings,market")
    second = add_job(store, kind="filing_acquire", lane="batch", exclusive="filings")
    free = add_job(store, kind="echo", lane="batch")

    lanes = ("batch",)
    assert claim(store, session, lanes)["job_id"] == first
    # the second needs "filings", which the running pipeline holds; the free one is not blocked
    assert claim(store, session, lanes)["job_id"] == free
    assert claim(store, session, lanes) is None
    store.fail(first, "w1", store.get(first)["lease_token"], error="x")
    assert claim(store, session, lanes)["job_id"] == second


def test_cancelling_jobs_still_hold_their_resources(store):
    session = add_session(store)
    first = add_job(store, lane="batch", exclusive="market")
    waiting = add_job(store, lane="batch", exclusive="market")
    claim(store, session, ("batch",))
    store.request_cancel(first)

    assert store.get(first)["status"] == "cancelling"
    assert claim(store, session, ("batch",)) is None
    assert store.get(waiting)["status"] == "open"


def test_claim_is_refused_without_data_access(store):
    session = add_session(store, data_access=False)
    add_job(store)

    with pytest.raises(NoDataAccess):
        claim(store, session)


def test_claim_needs_a_live_session_of_that_worker(store):
    session = add_session(store)
    add_job(store)
    with pytest.raises(UnknownSession):
        claim(store, "missing-session")
    with pytest.raises(UnknownSession):
        claim(store, session, worker="someone-else")
    store.end_session("w1", session)
    with pytest.raises(UnknownSession):
        claim(store, session)


def test_concurrent_claims_never_double_assign(store):
    sessions = [add_session(store, worker=f"w{i}") for i in range(8)]
    job_ids = {add_job(store) for _ in range(20)}
    won: list[tuple[str, str]] = []
    guard = threading.Lock()

    def work(index: int) -> None:
        while True:
            row = claim(store, sessions[index], worker=f"w{index}")
            if row is None:
                return
            with guard:
                won.append((row["job_id"], row["worker_id"]))

    threads = [threading.Thread(target=work, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert sorted(job for job, _ in won) == sorted(job_ids)
    assert len({job for job, _ in won}) == 20


def test_heartbeat_extends_leases_and_reports_cancel_modes(store):
    session = add_session(store)
    soft, hard, plain = (add_job(store) for _ in range(3))
    rows = {job: claim(store, session) for job in (soft, hard, plain)}
    rows = {row["job_id"]: row for row in rows.values()}
    store.request_cancel(soft)
    store.request_cancel(hard, force=True)

    later = plus_seconds(utc_now(), 30)
    states = beat(
        store, "w1", session,
        leases=[(job, rows[job]["lease_token"]) for job in rows] + [("gone", "x")],
        now=later,
    )
    by_id = {state["job_id"]: state for state in states}

    assert by_id[soft]["cancel"] == "soft"
    assert by_id[hard]["cancel"] == "hard"
    assert by_id[plain]["cancel"] == "none"
    assert by_id[plain]["lease_ok"] and by_id[plain]["lease_expires_at"] == plus_seconds(later, LEASE)
    assert store.get(plain)["lease_expires_at"] == plus_seconds(later, LEASE)
    assert by_id["gone"]["lease_ok"] is False


def test_heartbeat_rejects_a_wrong_token(store):
    session = add_session(store)
    job_id = add_job(store)
    claim(store, session)

    assert beat(store, "w1", session, leases=[(job_id, "wrong")])[0]["lease_ok"] is False


def test_heartbeat_for_an_unknown_session(store):
    with pytest.raises(UnknownSession):
        beat(store, "w1", "nope")


def _claimed(store, kind="echo", lane="interactive", **kw):
    session = add_session(store)
    job_id = add_job(store, kind=kind, lane=lane, **kw)
    return session, job_id, claim(store, session, (lane,))["lease_token"]


def _expire(store, job_id=None):
    return store.sweep(now=plus_seconds(utc_now(), LEASE + 5))


def test_sweep_reopens_a_retryable_job_with_attempts_left(store):
    _, job_id, _ = _claimed(store)

    assert _expire(store) == 1
    row = store.get(job_id)
    assert row["status"] == "open"
    assert row["worker_id"] is None and row["lease_token"] is None and row["attempt"] == 1


def test_sweep_fails_a_retryable_job_that_ran_out_of_attempts(store):
    _, job_id, _ = _claimed(store, max_attempts=1)

    _expire(store)

    row = store.get(job_id)
    assert row["status"] == "failed"
    assert row["error_message"] == "The worker stopped responding"


def test_sweep_interrupts_a_job_that_cannot_be_retried(store):
    _, job_id, _ = _claimed(store, kind="pipeline", lane="batch", exclusive="filings,market",
                            steps=[{"name": "a"}, {"name": "b"}], max_attempts=1)
    token = store.get(job_id)["lease_token"]
    store.progress(job_id, "w1", token, percent=10, message=None, progress=None,
                   step=StepEvent(ordinal=0, status="running"), lease_seconds=LEASE)

    _expire(store)

    assert store.get(job_id)["status"] == "interrupted"
    assert [step["status"] for step in store.steps(job_id)] == ["interrupted", "pending"]


def test_sweep_cancels_a_cancelling_job(store):
    _, job_id, _ = _claimed(store)
    store.request_cancel(job_id)

    _expire(store)

    assert store.get(job_id)["status"] == "cancelled"


def test_sweep_leaves_live_leases_alone(store):
    _, job_id, _ = _claimed(store)

    assert store.sweep() == 0
    assert store.get(job_id)["status"] == "in_progress"


def test_sweep_forgets_old_sessions(store):
    session = add_session(store)
    store.sweep(now=plus_seconds(utc_now(), 2 * 86_400))
    assert store.get_session(session) is None


def test_release_returns_a_retryable_job_without_using_an_attempt(store):
    _, job_id, token = _claimed(store)

    row = store.release(job_id, "w1", token)

    assert row["status"] == "open" and row["attempt"] == 0


def test_release_interrupts_a_job_that_cannot_be_retried(store):
    _, job_id, token = _claimed(store, kind="pipeline", lane="batch", max_attempts=1)

    row = store.release(job_id, "w1", token, reason="Shutting down")

    assert row["status"] == "interrupted" and row["error_message"] == "Shutting down"


def test_complete_is_idempotent_for_the_same_lease(store):
    _, job_id, token = _claimed(store)

    first = store.complete(job_id, "w1", token, '{"ok":true}')
    again = store.complete(job_id, "w1", token, '{"ok":true}')

    assert first["status"] == again["status"] == "completed"
    assert first["completed_at"] == again["completed_at"]
    assert first["progress_percent"] == 100


def test_a_stale_lease_is_refused(store):
    session, job_id, token = _claimed(store)
    _expire(store)
    claim(store, session)  # a new lease, so the old token is stale

    with pytest.raises(LeaseLost):
        store.complete(job_id, "w1", token, "{}")
    with pytest.raises(LeaseLost):
        store.progress(job_id, "w1", token, percent=1, message=None, progress=None, step=None,
                       lease_seconds=LEASE)
    with pytest.raises(LeaseLost):
        store.check_lease(job_id, "w1", token)
    with pytest.raises(LeaseLost):
        store.complete(job_id, "other-worker", store.get(job_id)["lease_token"], "{}")


def test_complete_after_the_job_was_finished_by_someone_else(store):
    _, job_id, token = _claimed(store)
    store.fail(job_id, "w1", token, error="boom")

    with pytest.raises(LeaseLost):
        store.complete(job_id, "w1", "not-the-token", "{}")


def test_fail_paths(store):
    session = add_session(store)
    ids = [add_job(store, max_attempts=2) for _ in range(4)]
    tokens = {}
    for _ in ids:
        row = claim(store, session)
        tokens[row["job_id"]] = row["lease_token"]
    cancelled, retry, plain, exhausted = ids

    assert store.fail(cancelled, "w1", tokens[cancelled], error="", cancelled=True)["status"] == "cancelled"
    assert store.fail(retry, "w1", tokens[retry], error="x", retryable=True)["status"] == "open"
    plain_row = store.fail(plain, "w1", tokens[plain], error="No\nsecret=abc123 here")
    assert plain_row["status"] == "failed"
    assert "abc123" not in plain_row["error_message"] and "\n" not in plain_row["error_message"]
    # second attempt of ``retry`` exhausts max_attempts=2
    again = claim(store, session)
    assert again["job_id"] == retry and again["attempt"] == 2
    assert store.fail(retry, "w1", again["lease_token"], error="x", retryable=True)["status"] == "failed"
    store.request_cancel(exhausted)
    assert store.fail(exhausted, "w1", tokens[exhausted], error="x", retryable=True)["status"] == "cancelled"


def test_cancel_transitions(store):
    session = add_session(store)
    open_job = add_job(store)
    running = add_job(store)

    assert store.request_cancel(open_job)["status"] == "cancelled"
    claimed = claim(store, session)
    assert claimed["job_id"] == running
    soft = store.request_cancel(running)
    assert soft["status"] == "cancelling" and soft["cancel_requested"] == "soft"
    assert store.request_cancel(running)["cancel_requested"] == "soft"
    assert store.request_cancel(running, force=True)["cancel_requested"] == "hard"
    store.fail(running, "w1", claimed["lease_token"], error="", cancelled=True)
    with pytest.raises(InvalidJobState):
        store.request_cancel(running)
    with pytest.raises(KeyError):
        store.request_cancel("missing")


def test_a_job_cancelled_while_running_that_completes_anyway_is_completed(store):
    _, job_id, token = _claimed(store)
    store.request_cancel(job_id)

    assert store.complete(job_id, "w1", token, "{}")["status"] == "completed"


def test_dedupe_returns_the_active_job(store):
    first, created = store.insert_job(
        job_id=str(uuid4()), kind="description_fetch", lane="interactive", title="t", owner_user_id=None,
        priority=0, payload={}, exclusive="", max_attempts=3, notify=False, dedupe_key="description:7203",
    )
    second, created_again = store.insert_job(
        job_id=str(uuid4()), kind="description_fetch", lane="interactive", title="t", owner_user_id=None,
        priority=0, payload={}, exclusive="", max_attempts=3, notify=False, dedupe_key="description:7203",
    )

    assert created and not created_again
    assert second["job_id"] == first["job_id"]
    store.request_cancel(first["job_id"])
    third, created_third = store.insert_job(
        job_id=str(uuid4()), kind="description_fetch", lane="interactive", title="t", owner_user_id=None,
        priority=0, payload={}, exclusive="", max_attempts=3, notify=False, dedupe_key="description:7203",
    )
    assert created_third and third["job_id"] != first["job_id"]


def test_progress_updates_job_and_steps(store):
    _, job_id, token = _claimed(store, kind="pipeline", lane="batch",
                                steps=[{"name": "a", "overwrite": True}, {"name": "b"}], max_attempts=1)

    def step(**kw):
        return store.progress(job_id, "w1", token, percent=kw.pop("percent", None),
                              message=kw.pop("message", None), progress=None,
                              step=StepEvent(**kw), lease_seconds=LEASE)

    step(ordinal=0, status="running", message="Reading")
    assert store.steps(job_id)[0]["status"] == "running"
    assert store.steps(job_id)[0]["started_at"]
    step(ordinal=0, status="running", progress_percent=40, message="Halfway", percent=20)
    assert store.steps(job_id)[0]["progress_percent"] == 40
    row = step(ordinal=0, status="completed", result={"rows": 3, "api_key": "k"}, duration_ms=12, percent=50)
    first = store.steps(job_id)[0]
    assert first["status"] == "completed" and first["progress_percent"] == 100
    assert first["duration_ms"] == 12 and "REDACTED" in first["result_json"]
    assert first["overwrite"] == 1
    assert row["progress_percent"] == 50 and row["cancel_requested"] == "none"
    step(ordinal=1, status="failed", error_message="Bad input")
    assert store.steps(job_id)[1]["error_message"] == "Bad input"
    step(ordinal=99, status="running")  # an unknown step is ignored


def test_oversized_progress_is_dropped(store, caplog):
    _, job_id, token = _claimed(store)

    store.progress(job_id, "w1", token, percent=5, message="m", progress={"x": 1}, step=None,
                   lease_seconds=LEASE)
    with caplog.at_level("WARNING"):
        row = store.progress(job_id, "w1", token, percent=6, message=None,
                             progress={"blob": "x" * 20_000}, step=None, lease_seconds=LEASE)

    assert row["progress_json"] == '{"x":1}'
    assert row["progress_percent"] == 6
    assert "oversized" in caplog.text


def test_status_message_is_redacted_and_capped(store):
    _, job_id, token = _claimed(store)
    row = store.progress(job_id, "w1", token, percent=None, progress=None, step=None, lease_seconds=LEASE,
                         message="Reading C:\\Users\\me\\file.csv token=abc " + "x" * 800)

    assert "Users" not in row["status_message"] and "abc" not in row["status_message"]
    assert len(row["status_message"]) <= 500


def test_delete_terminal_before_removes_rows_and_cascades(store):
    session = add_session(store)
    old = add_job(store, steps=[{"name": "s"}])
    keep = add_job(store)
    running = add_job(store)
    store.request_cancel(old, now=plus_seconds(utc_now(), -7200))
    store.record_artifact(old, name="a.txt", size_bytes=1, sha256="x", content_type=None, stored_path="p")
    claim(store, session)  # claims ``keep`` (oldest open)
    assert store.get(running)["status"] == "open"

    cutoff = plus_seconds(utc_now(), -3600)
    assert store.delete_terminal_before(cutoff) == [old]
    with pytest.raises(KeyError):
        store.get(old)
    assert store.steps(old) == [] and store.artifacts(old) == []
    assert store.get(keep)


def _legacy_app_db(path):
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE pipeline_jobs (job_id TEXT PRIMARY KEY, status TEXT NOT NULL, created_at TEXT NOT NULL,
              started_at TEXT, completed_at TEXT, current_step TEXT, progress_percent REAL NOT NULL DEFAULT 0,
              step_count INTEGER NOT NULL DEFAULT 0, completed_step_count INTEGER NOT NULL DEFAULT 0,
              error_message TEXT, status_message TEXT);
            CREATE TABLE pipeline_job_steps (job_id TEXT NOT NULL, ordinal INTEGER NOT NULL, step_name TEXT NOT NULL,
              overwrite INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending', started_at TEXT,
              completed_at TEXT, duration_ms INTEGER, result_json TEXT, error_message TEXT,
              progress_percent REAL NOT NULL DEFAULT 0, status_message TEXT, PRIMARY KEY (job_id, ordinal));
            """
        )
        rows = [
            ("done", "completed", "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00", "2026-01-01T00:10:00+00:00", 100.0, None),
            ("bad", "failed", "2026-01-02T00:00:00+00:00", "2026-01-02T00:00:01+00:00", "2026-01-02T00:10:00+00:00", 30.0, "Boom"),
            ("pend", "pending", "2026-01-03T00:00:00+00:00", None, None, 0.0, None),
            ("run", "running", "2026-01-04T00:00:00+00:00", "2026-01-04T00:00:01+00:00", None, 55.0, None),
            ("canc", "cancelling", "2026-01-05T00:00:00+00:00", "2026-01-05T00:00:01+00:00", None, 5.0, None),
        ]
        for jid, status, created, started, completed, pct, err in rows:
            conn.execute(
                "INSERT INTO pipeline_jobs(job_id, status, created_at, started_at, completed_at, "
                "progress_percent, error_message) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (jid, status, created, started, completed, pct, err),
            )
        conn.execute("INSERT INTO pipeline_job_steps(job_id, ordinal, step_name, status) VALUES ('done', 0, 'a', 'completed')")
        conn.execute("INSERT INTO pipeline_job_steps(job_id, ordinal, step_name, status) VALUES ('run', 0, 'a', 'running')")
        conn.execute("INSERT INTO pipeline_job_steps(job_id, ordinal, step_name, status) VALUES ('run', 1, 'b', 'pending')")


def test_legacy_import_copies_history_once(store, tmp_path):
    app_db = tmp_path / "app.db"
    _legacy_app_db(app_db)

    assert import_legacy_pipeline_jobs(store, app_db) == 5
    assert import_legacy_pipeline_jobs(store, app_db) == 0  # guarded

    status = {jid: store.get(jid)["status"] for jid in ("done", "bad", "pend", "run", "canc")}
    assert status == {"done": "completed", "bad": "failed", "pend": "interrupted",
                      "run": "interrupted", "canc": "cancelled"}
    row = store.get("bad")
    assert (row["kind"], row["lane"], row["title"], row["payload_json"]) == ("pipeline", "batch", "Pipeline", "{}")
    assert row["error_message"] == "Boom" and row["progress_percent"] == 30
    assert row["created_at"] == "2026-01-02T00:00:00+00:00"
    assert [s["status"] for s in store.steps("run")] == ["interrupted", "pending"]
    assert [s["status"] for s in store.steps("done")] == ["completed"]
    # the old tables are left alone
    with sqlite3.connect(app_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM pipeline_jobs").fetchone()[0] == 5


def test_legacy_import_tolerates_missing_tables_and_files(store, tmp_path):
    empty = tmp_path / "empty.db"
    sqlite3.connect(empty).close()

    assert import_legacy_pipeline_jobs(store, empty) == 0
    assert import_legacy_pipeline_jobs(JobsStore(tmp_path / "other.db"), tmp_path / "absent.db") == 0


def test_second_registration_ends_the_first_session(store):
    first = add_session(store)
    second = add_session(store)

    assert store.get_session(first)["ended_at"]
    assert store.get_session(second)["ended_at"] is None
    assert list(store.latest_sessions()) == ["w1"]
    assert store.latest_sessions()["w1"]["session_id"] == second


def test_session_ids_cannot_be_taken_over(store):
    session = add_session(store, worker="w1")
    with pytest.raises(UnknownSession):
        add_session(store, worker="w2", session=session)


def test_online_sessions_use_last_seen(store):
    session = add_session(store)
    cutoff_future = plus_seconds(utc_now(), 100)
    assert store.online_sessions(plus_seconds(utc_now(), -100))[0]["session_id"] == session
    assert store.online_sessions(cutoff_future) == []
    store.end_session("w1", session)
    assert store.online_sessions(plus_seconds(utc_now(), -100)) == []


def test_utc_helpers_sort_as_text():
    moment = datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(timespec="microseconds")
    assert plus_seconds(moment, 1) > moment > plus_seconds(moment, -1)
    assert datetime.fromisoformat(plus_seconds(moment, 90)) == datetime(2026, 1, 1, 0, 1, 30, tzinfo=timezone.utc)


def test_server_bootstrap_creates_jobs_db_but_never_touches_market_or_filings(tmp_path, monkeypatch):
    from src.orchestrator.common import db_config
    from src.orchestrator.common.database_bootstrap import (
        ensure_data_databases,
        ensure_server_databases,
    )

    monkeypatch.setenv("EDINET_DATA_DIR", str(tmp_path))
    db_config.reload()
    try:
        paths = ensure_server_databases()
        assert set(paths) == {"app", "chat", "jobs"}
        assert sorted(path.name for path in tmp_path.glob("*.db")) == ["app.db", "chat.db", "jobs.db"]
        assert JobsStore(paths["jobs"]).counts_by_status() == {}
        with sqlite3.connect(paths["app"]) as conn:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master")}
        assert {"workers", "worker_enrollments", "pipeline_schedules"} <= tables

        data = ensure_data_databases()
        assert set(data) == {"market", "filings"} and data["market"].exists() and data["filings"].exists()
    finally:
        monkeypatch.undo()
        db_config.reload()
