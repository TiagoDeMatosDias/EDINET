"""Supervisor with real spawned children against the fake protocol server."""

from __future__ import annotations

import multiprocessing
import threading

import pytest

from src.worker import supervisor as supervisor_module
from src.worker.client import WorkerClient
from src.worker.runtime import uninstall_worker_runtime
from src.worker.supervisor import Supervisor, check_data_access
from tests.worker_fakes import TOKEN, FakeServer, wait_until

# Test-only handlers (tests/worker_handlers.py) stand in for kinds that have no handler yet.
OVERRIDES = {
    "backtest_single": "tests.worker_handlers:probe",
    "backtest_csv": "tests.worker_handlers:probe",
    "backtest_rolling": "tests.worker_handlers:probe",
}


class Harness:
    def __init__(self, root, **kwargs) -> None:
        self.server = FakeServer(root).start()
        self.state = self.server.state
        self.client = WorkerClient(self.server.url, TOKEN, self.server.fingerprint)
        self.supervisor = Supervisor(
            self.client,
            interactive_slots=1,
            batch_slots=1,
            preload_modules=(),
            run_startup=False,
            handler_overrides=OVERRIDES,
            **kwargs,
        )
        self.result: list[int] = []
        self.thread = threading.Thread(
            target=lambda: self.result.append(self.supervisor.run(handle_signals=False)), daemon=True
        )

    def start(self) -> Harness:
        self.thread.start()
        wait_until(lambda: self.state.data_access and self.state.heartbeats >= 1, timeout=30)
        return self

    def stop(self) -> None:
        self.supervisor.request_stop("test over")
        self.thread.join(30)
        assert not self.thread.is_alive(), "supervisor did not stop"
        self.server.stop()
        uninstall_worker_runtime()
        for child in multiprocessing.active_children():
            child.kill()
            child.join(5)

    def run_job(self, kind, payload=None, status=("completed",), timeout=40):
        job = self.state.enqueue(kind, payload)
        wait_until(lambda: job.status in status, timeout=timeout)
        return job

    def wait_running(self, job, timeout=30) -> int:
        """Wait until the probe handler reports in; returns the pid of the child running it."""
        wait_until(lambda: job.status == "in_progress" and job.progress, timeout=timeout)
        return int(job.progress[0]["status_message"].split()[1])

    def wait_children(self, count=2) -> None:
        wait_until(lambda: len(multiprocessing.active_children()) >= count, timeout=30)


@pytest.fixture(scope="module")
def harness(tmp_path_factory):
    instance = Harness(tmp_path_factory.mktemp("supervisor")).start()
    yield instance
    instance.stop()


def test_echo_job_completes_with_result_artifact_and_progress(harness):
    job = harness.run_job(
        "echo", {"result": {"n": 7}, "artifact_text": "hello", "progress_steps": 3, "sleep_seconds": 0.3}
    )

    assert job.result == {"echo": {"n": 7}}
    assert job.artifacts == {"echo.txt": b"hello"}
    percents = [p["progress_percent"] for p in job.progress if "progress_percent" in p]
    assert percents and percents[-1] == 100.0 and percents == sorted(percents)
    assert {"artifact": "echo.txt", "content_type": "text/plain"} in job.progress
    assert harness.state.data_access is True
    assert not list((harness.state.root / "jobs" / job.job_id).glob("out-*"))  # temp output removed


def test_job_error_message_reaches_the_server(harness):
    job = harness.run_job("echo", {"fail": "Expected failure"}, status=("failed",))

    assert job.error == "Expected failure" and job.retryable is False


def test_unexpected_exception_gives_generic_public_message(harness):
    job = harness.run_job("backtest_single", {"mode": "raise"}, status=("failed",))

    assert job.error == "Backtest failed (ValueError)" and job.retryable is False


def test_soft_cancel_stops_the_job_as_cancelled(harness):
    job = harness.state.enqueue("echo", {"sleep_seconds": 60, "progress_steps": 1000})
    harness.wait_running(job)

    harness.state.request_cancel(job.job_id, "soft")
    wait_until(lambda: job.status == "cancelled", timeout=20)

    assert job.error == "The job was cancelled"


def _alive(pid: int) -> bool:
    return supervisor_module.process_is_alive(pid) and pid in {c.pid for c in multiprocessing.active_children()}


def test_hard_cancel_kills_the_child_and_a_replacement_is_started(harness):
    job = harness.state.enqueue("backtest_single", {"mode": "hang"})
    victim = harness.wait_running(job)

    harness.state.request_cancel(job.job_id, "hard")
    wait_until(lambda: job.status == "cancelled", timeout=20)

    wait_until(lambda: not _alive(victim), timeout=10)
    harness.wait_children(2)
    follow_up = harness.run_job("backtest_single")
    assert follow_up.result["pid"] != victim


def test_lease_loss_kills_the_child_without_reporting(harness):
    job = harness.state.enqueue("backtest_single", {"mode": "hang"})
    victim = harness.wait_running(job)

    harness.state.revoke_lease(job.job_id)

    wait_until(lambda: not _alive(victim), timeout=10)
    harness.wait_children(2)
    assert harness.run_job("backtest_single").result["pid"] != victim
    assert job.status == "in_progress"  # the worker dropped it; the server's sweep owns it now


def test_a_crashing_child_fails_the_job_retryably_and_is_replaced(harness):
    before = {child.pid for child in multiprocessing.active_children()}
    job = harness.state.enqueue("backtest_single", {"mode": "crash"})
    wait_until(lambda: job.status == "failed", timeout=20)

    assert job.error == "The worker process stopped unexpectedly" and job.retryable is True
    wait_until(lambda: len(before - {child.pid for child in multiprocessing.active_children()}) == 1, timeout=10)
    harness.wait_children(2)
    assert harness.run_job("backtest_single").status == "completed"


def test_batch_children_are_recycled_after_every_batch_job(harness):
    first = harness.run_job("backtest_rolling")
    second = harness.run_job("backtest_rolling")
    third = harness.run_job("backtest_rolling")

    assert first.lane == "batch"
    assert len({first.result["pid"], second.result["pid"], third.result["pid"]}) == 3


def test_data_access_check(tmp_path):
    from src.jobs.protocol import WorkerPaths
    from tests.worker_fakes import make_sqlite

    good = make_sqlite(tmp_path / "a.db")
    ok, detail = check_data_access(WorkerPaths(market_db=str(good), filings_db=str(good), data_dir="", jobs_dir=""))
    assert ok and detail is None
    ok, detail = check_data_access(
        WorkerPaths(market_db=str(good), filings_db=str(tmp_path / "missing.db"), data_dir="", jobs_dir="")
    )
    assert not ok and "filings.db not found" in (detail or "")


def test_process_is_alive():
    import os

    assert supervisor_module.process_is_alive(os.getpid())
    assert not supervisor_module.process_is_alive(0)
    assert not supervisor_module.process_is_alive(2**22 + 12345)


def test_graceful_stop_releases_running_jobs_and_unregisters(tmp_path, monkeypatch):
    monkeypatch.setattr(supervisor_module, "SHUTDOWN_WAIT_SECONDS", 2.0)
    harness = Harness(tmp_path).start()
    try:
        cooperative = harness.state.enqueue("echo", {"sleep_seconds": 60})
        stubborn = harness.state.enqueue("backtest_single", {"mode": "hang"})
        wait_until(lambda: cooperative.progress is not None and cooperative.status == "in_progress", timeout=30)
        wait_until(lambda: stubborn.status == "in_progress", timeout=30)
        harness.wait_running(stubborn)
        own = {slot.process.pid for slot in harness.supervisor._slots if slot.process is not None}
    finally:
        harness.supervisor.request_stop("test")
        harness.thread.join(30)
    try:
        assert harness.result == [0]
        assert cooperative.released and cooperative.status == "open"
        assert stubborn.released and stubborn.status == "open"
        assert harness.state.unregistered
        assert own and not own & {child.pid for child in multiprocessing.active_children()}
    finally:
        harness.stop()


def test_configuration_change_resizes_slots_and_shutdown_flag_stops_the_worker(tmp_path):
    harness = Harness(tmp_path, ).start()
    try:
        harness.supervisor._slot_override = (None, None)  # follow the server's counts
        harness.state.slots = (2, 0)
        harness.state.config_version = 2
        wait_until(lambda: harness.state.registrations >= 2, timeout=20)
        wait_until(
            lambda: sorted(s.kind for s in harness.supervisor._slots if not s.retired) == ["interactive", "interactive"],
            timeout=20,
        )
        harness.state.shutdown = True
        harness.thread.join(30)
        assert not harness.thread.is_alive() and harness.result == [0]
    finally:
        harness.state.shutdown = False
        harness.stop()


def test_unknown_session_triggers_reregistration(tmp_path):
    harness = Harness(tmp_path).start()
    try:
        before = harness.state.registrations
        harness.state.sessions.clear()
        wait_until(lambda: harness.state.registrations > before, timeout=20)
        job = harness.run_job("echo", {"result": 1})
        assert job.result == {"echo": 1}
    finally:
        harness.stop()


def test_rejection_stops_with_exit_code_3(tmp_path):
    harness = Harness(tmp_path).start()
    try:
        harness.client._token = "srw_revoked"
        harness.thread.join(30)
        assert harness.result == [3]
    finally:
        harness.stop()


def test_shade_worker_script_runs_jobs_and_stops_on_sigterm(tmp_path):
    """The real entry point: spawn works from the script, logs go to the data folder, SIGTERM exits 0."""
    import json
    import os
    import signal
    import subprocess
    import sys
    from pathlib import Path

    server = FakeServer(tmp_path / "srv").start()
    data_dir = tmp_path / "data"
    environment = {**os.environ, "EDINET_DATA_DIR": str(data_dir)}
    credentials = {
        "server_url": server.url,
        "fingerprint": server.fingerprint,
        "worker_id": "w-test",
        "token": TOKEN,
        "parent_pid": os.getpid(),
    }
    root = Path(__file__).resolve().parents[2]
    process = subprocess.Popen(
        [sys.executable, str(root / "shade_worker.py"), "run", "--stdin-credentials",
         "--interactive-slots", "1", "--batch-slots", "0"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=environment, cwd=tmp_path, text=True,
    )
    try:
        assert process.stdin is not None
        process.stdin.write(json.dumps(credentials) + "\n")
        process.stdin.close()
        job = server.state.enqueue("echo", {"result": "ok", "artifact_text": "x"})
        wait_until(lambda: job.status == "completed", timeout=90)
        assert job.result == {"echo": "ok"} and job.artifacts == {"echo.txt": b"x"}
        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=30) == 0
        assert server.state.unregistered
        assert "Worker stopped" in (data_dir / "logs" / "worker-w-test.log").read_text(encoding="utf-8")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(10)
        server.stop()


def test_worker_stops_when_the_parent_process_exits(tmp_path, monkeypatch):
    import subprocess
    import sys

    monkeypatch.setattr(supervisor_module, "PARENT_CHECK_SECONDS", 0.2)
    parent = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    harness = Harness(tmp_path, parent_pid=parent.pid).start()
    try:
        assert harness.thread.is_alive()
        parent.kill()
        parent.wait(10)
        harness.thread.join(30)
        assert not harness.thread.is_alive() and harness.result == [0]
        assert harness.state.unregistered
    finally:
        if parent.poll() is None:
            parent.kill()
        harness.stop()
