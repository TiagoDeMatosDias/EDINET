"""Enrollment strings, worker.db, CLI handling, maintenance and the echo handler."""

from __future__ import annotations

import io
import json
import logging
import sqlite3
import threading
from contextlib import closing
from pathlib import Path

import pytest

from src.worker import cli, local_config, maintenance
from src.worker.context import JobCancelledError, JobContext, JobError
from src.worker.handlers.diagnostic import echo
from tests.worker_fakes import FakeServer, make_sqlite


def test_enrollment_string_round_trip():
    text = cli.encode_enrollment("https://127.0.0.1:8000", "ab" * 32, "code-1234567890")
    assert text.startswith("SRW1.")
    assert "=" not in text

    assert cli.decode_enrollment(text) == ("https://127.0.0.1:8000", "ab" * 32, "code-1234567890")
    assert cli.decode_enrollment(f"  {text}\n")[2] == "code-1234567890"


@pytest.mark.parametrize("text", ["", "hello", "SRW1.", "SRW1.!!!", "SRW2.e30", "SRW1.e30"])
def test_bad_enrollment_strings_are_rejected(text):
    with pytest.raises(cli.EnrollmentStringError):
        cli.decode_enrollment(text)


def test_worker_db_save_and_load(tmp_path):
    path = tmp_path / "sub" / "worker.db"
    assert local_config.load(path) is None
    assert not path.exists()

    local_config.save(path, local_config.WorkerCredentials("https://h:1", "ff" * 32, "w1", "srw_t", name="Laptop"))
    loaded = local_config.load(path)

    assert loaded is not None
    assert (loaded.server_url, loaded.fingerprint, loaded.worker_id, loaded.token, loaded.name) == (
        "https://h:1", "ff" * 32, "w1", "srw_t", "Laptop",
    )
    assert loaded.enrolled_at
    with closing(sqlite3.connect(path)) as conn:
        assert conn.execute("SELECT count(*) FROM worker_config").fetchone()[0] == 6


def test_worker_db_with_missing_keys_counts_as_not_enrolled(tmp_path):
    path = tmp_path / "worker.db"
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("CREATE TABLE worker_config (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("INSERT INTO worker_config VALUES ('server_url', 'https://h')")
        conn.commit()

    assert local_config.load(path) is None


def test_status_when_not_enrolled(tmp_path, capsys):
    code = cli.main(["status", "--config", str(tmp_path / "worker.db")])

    assert code == 2
    assert "Not enrolled" in capsys.readouterr().out


def test_run_when_not_enrolled(tmp_path, capsys):
    code = cli.main(["run", "--config", str(tmp_path / "worker.db")])

    assert code == 2
    assert "not enrolled" in capsys.readouterr().err


@pytest.mark.parametrize("line", ["", "not json", '{"server_url": "https://h"}'])
def test_stdin_credentials_must_be_complete(monkeypatch, capsys, line):
    monkeypatch.setattr("sys.stdin", io.StringIO(line + "\n"))

    assert cli.main(["run", "--stdin-credentials"]) == 2
    assert "--stdin-credentials" in capsys.readouterr().err


def test_stdin_credentials_are_parsed(monkeypatch):
    payload = {"server_url": "https://h:1", "fingerprint": "ab", "worker_id": "local", "token": "srw_x", "parent_pid": 42}
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload) + "\n"))

    credentials, parent_pid = cli._read_stdin_credentials()

    assert (credentials.worker_id, credentials.token, parent_pid) == ("local", "srw_x", 42)


def test_run_wires_stdin_credentials_and_slot_flags(monkeypatch):
    payload = {"server_url": "https://h:1", "fingerprint": "ab" * 32, "worker_id": "local", "token": "srw_x", "parent_pid": 7}
    monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload) + "\n"))
    seen = {}

    class FakeSupervisor:
        def __init__(self, client, **kwargs):
            seen.update(kwargs, url=client.server_url)

        def run(self):
            return 0

    monkeypatch.setattr("src.worker.supervisor.Supervisor", FakeSupervisor)
    monkeypatch.setattr("src.worker.runtime.configure_logging", lambda *_a, **_k: None)

    code = cli.main(["run", "--stdin-credentials", "--interactive-slots", "2", "--batch-slots", "0"])

    assert code == 0
    assert seen == {"interactive_slots": 2, "batch_slots": 0, "parent_pid": 7, "url": "https://h:1"}


def test_enroll_saves_worker_db_and_status_reaches_server(tmp_path, capsys):
    server = FakeServer(tmp_path / "srv").start() if (tmp_path / "srv").mkdir() is None else None
    assert server is not None
    try:
        config = tmp_path / "worker.db"
        text = cli.encode_enrollment(server.url, server.fingerprint, "good-code-123")

        assert cli.main(["enroll", text, "--config", str(config), "--name", "Test box"]) == 0
        saved = local_config.load(config)
        assert saved is not None and saved.worker_id == "w-enrolled" and saved.name == "Test box"
        assert server.state.enrolled[0].name == "Test box"

        capsys.readouterr()
        assert cli.main(["status", "--config", str(config)]) == 0
        output = capsys.readouterr().out
        assert server.url in output and "w-enrolled" in output and saved.token not in output
        assert "Reachable: yes" in output

        bad = cli.encode_enrollment(server.url, server.fingerprint, "bad-code-123")
        assert cli.main(["enroll", bad, "--config", str(tmp_path / "other.db")]) == 2
        assert not (tmp_path / "other.db").exists()
    finally:
        server.stop()


def test_enroll_with_wrong_fingerprint_fails(tmp_path, capsys):
    (tmp_path / "srv").mkdir()
    server = FakeServer(tmp_path / "srv").start()
    try:
        text = cli.encode_enrollment(server.url, "00" * 32, "good-code-123")

        assert cli.main(["enroll", text, "--config", str(tmp_path / "w.db")]) == 2
        assert "certificate" in capsys.readouterr().err
    finally:
        server.stop()


# --- maintenance ---------------------------------------------------------------


@pytest.fixture
def worker_paths(tmp_path):
    from src.orchestrator.common import db_config

    market = make_sqlite(tmp_path / "market.db")
    filings = make_sqlite(tmp_path / "filings.db")
    db_config.install_worker_paths(str(market), str(filings))
    yield market, filings
    db_config.clear_worker_paths()


def test_startup_runs_every_step_and_survives_failures(worker_paths, monkeypatch):
    market, filings = worker_paths
    calls = []

    def fake_schemas(**kwargs):
        calls.append(("schemas", kwargs))
        raise RuntimeError("disk I/O error")

    monkeypatch.setattr("src.orchestrator.common.database_bootstrap.ensure_data_databases", fake_schemas, raising=False)
    monkeypatch.setattr(
        "src.security_analysis.security_analysis.ensure_security_analysis_indexes", lambda db: calls.append(("indexes", db))
    )
    monkeypatch.setattr("src.screening.screening._prepare_screening_database", lambda db: calls.append(("screening", db)))

    results = maintenance.startup()

    assert results["schemas"].startswith("RuntimeError")
    assert results["screening_prep"] == "ok" and results["security_analysis_indexes"] == "ok"
    assert calls[0] == ("schemas", {"market_db_path": str(market), "filings_db_path": str(filings), "busy_timeout_ms": 30000})
    assert ("screening", str(market)) in calls and ("indexes", str(market)) in calls


def test_startup_on_a_fresh_install_skips_steps_without_their_tables(worker_paths, monkeypatch, caplog):
    """An empty market.db is normal before the first pipeline run: no warning, no traceback."""

    def no_company_table(db):
        raise RuntimeError("Required table not found. Tried: CompanyInfo, companyInfo")

    def no_statements(db):
        raise sqlite3.OperationalError("no such table: main.FinancialStatements")

    monkeypatch.setattr("src.security_analysis.security_analysis.ensure_security_analysis_indexes", no_company_table)
    monkeypatch.setattr("src.screening.screening._prepare_screening_database", no_statements)

    with caplog.at_level(logging.INFO, logger="src.worker.maintenance"):
        results = maintenance.startup()

    assert results["screening_prep"].startswith("skipped:")
    assert results["security_analysis_indexes"].startswith("skipped:")
    assert not [record for record in caplog.records if record.levelno >= logging.WARNING]


def test_after_write_refreshes_market_and_checkpoints_only_written_databases(worker_paths, monkeypatch):
    market, filings = worker_paths
    calls = []
    monkeypatch.setattr(maintenance, "_refresh_market", lambda db, results: calls.append(("refresh", db)))
    monkeypatch.setattr(maintenance, "checkpoint", lambda path: calls.append(("checkpoint", path)))
    # The filing index and its yearly files are checkpointed together.
    monkeypatch.setattr(maintenance, "checkpoint_filings", lambda path: calls.append(("checkpoint", path)))

    maintenance.after_write(frozenset({"filings"}))
    assert calls == [("checkpoint", str(filings))]

    calls.clear()
    results = maintenance.after_write(frozenset({"market", "filings"}))
    assert calls == [("refresh", str(market)), ("checkpoint", str(filings)), ("checkpoint", str(market))]
    assert results["checkpoint_market"] == "ok"


def test_checkpoint_is_passive_on_a_real_wal_database(tmp_path):
    path = tmp_path / "wal.db"
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE t (x)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
        maintenance.checkpoint(str(path))  # a connection is still open: must not block


# --- diagnostic handler --------------------------------------------------------


def _context(tmp_path: Path, payload: dict, sink=None) -> JobContext:
    out = tmp_path / "out"
    out.mkdir(exist_ok=True)
    return JobContext("j1", "echo", 1, payload, tmp_path, out, threading.Event(), sink=sink)


def test_echo_returns_result_progress_and_artifact(tmp_path):
    events = []
    context = _context(
        tmp_path,
        {"result": {"a": 1}, "artifact_text": "hi", "progress_steps": 4, "sleep_seconds": 0.2},
        sink=lambda event, data: events.append((event, data)),
    )

    assert echo(context) == {"echo": {"a": 1}}
    assert (tmp_path / "out" / "echo.txt").read_text() == "hi"
    percents = [data["percent"] for event, data in events if event == "progress"]
    assert percents == [25.0, 50.0, 75.0, 100.0]


def test_echo_failure_and_cancel(tmp_path):
    with pytest.raises(JobError, match="boom"):
        echo(_context(tmp_path, {"fail": "boom"}))

    context = _context(tmp_path, {"sleep_seconds": 30})
    threading.Timer(0.2, context.cancel_event.set).start()
    with pytest.raises(JobCancelledError):
        echo(context)


def test_a_job_whose_supervisor_is_gone_ends_as_cancelled(tmp_path, caplog):
    """The inbox cancels the job when the pipe closes; its next report then fails quietly."""
    from src.worker.child import run_assignment

    cancel = threading.Event()

    def send(message):
        cancel.set()  # what the inbox does when the supervisor's end closes
        raise BrokenPipeError(32, "Broken pipe")

    assignment = {
        "job_id": "j1", "kind": "echo", "lane": "batch", "payload": {"progress_steps": 2}, "attempt": 1,
        "lease_token": "t", "lease_expires_at": "2099-01-01T00:00:00+00:00", "workspace": str(tmp_path),
    }
    with caplog.at_level(logging.ERROR):
        outcome = run_assignment(assignment, cancel, {}, send)

    assert outcome == ("error", "j1", "The job was cancelled", False, True)
    assert not [record for record in caplog.records if record.levelno >= logging.ERROR]
