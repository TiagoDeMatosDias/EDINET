"""LocalWorkerLauncher with stand-in worker commands."""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path

import pytest

from src.jobs import launcher as launcher_module
from src.jobs.launcher import LocalWorkerLauncher, default_command, default_server_url
from src.jobs.registry import WorkerRegistry

# Stand-in worker. argv: output file, mode. It records what it was given and then
# behaves according to the mode.
SCRIPT = r"""
import json, os, signal, sys, time
out, mode = sys.argv[1], sys.argv[2]
line = sys.stdin.readline()
with open(out, "a", encoding="utf-8") as handle:
    handle.write(json.dumps({"stdin": line, "frozen_env": os.environ.get("PYINSTALLER_RESET_ENVIRONMENT"),
                             "pid": os.getpid(), "stdin_eof": sys.stdin.read() == ""}) + "\n")
print("hello from stdout", flush=True)
print("hello from stderr", file=sys.stderr, flush=True)
if mode == "exit3":
    sys.exit(3)
if mode == "ignore-term":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
while True:
    time.sleep(0.1)
"""


@pytest.fixture
def registry(tmp_path):
    return WorkerRegistry(tmp_path / "app.db")


@pytest.fixture
def script(tmp_path):
    path = tmp_path / "fake_worker.py"
    path.write_text(SCRIPT, encoding="utf-8")
    return path


def make(registry, tmp_path, script, mode="sleep", **kw):
    out = tmp_path / "runs.jsonl"
    options = dict(
        server_url="https://127.0.0.1:8000",
        fingerprint="ab" * 32,
        command=lambda: [sys.executable, str(script), str(out), mode],
        launch_local="on",
        log_path=tmp_path / "worker-local.log",
        backoff_seconds=(0.05, 0.05, 0.1),
    )
    options.update(kw)
    return LocalWorkerLauncher(registry, **options), out


def runs(out: Path) -> list[dict]:
    if not out.exists():
        return []
    return [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines() if line.strip()]


def wait_for(condition, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError("condition not met in time")


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def test_credentials_arrive_on_stdin_and_stdin_is_closed(registry, tmp_path, script):
    launcher, out = make(registry, tmp_path, script)
    launcher.start()
    try:
        first = wait_for(lambda: runs(out) or None)[0]
        status = wait_for(lambda: launcher.status() if launcher.status()["state"] == "running" else None)
    finally:
        launcher.stop()

    credentials = json.loads(first["stdin"])
    assert first["stdin"].endswith("\n") and first["stdin_eof"] is True
    assert set(credentials) == {"server_url", "fingerprint", "worker_id", "token", "parent_pid"}
    assert credentials["server_url"] == "https://127.0.0.1:8000"
    assert credentials["fingerprint"] == "ab" * 32
    assert credentials["worker_id"] == "local" and credentials["parent_pid"] == os.getpid()
    assert registry.authenticate(credentials["token"]).worker_id == "local"
    assert status["pid"] == first["pid"] and status["command"][-1] == "sleep"
    assert first["frozen_env"] is None


def test_output_goes_to_the_log_file(registry, tmp_path, script):
    launcher, out = make(registry, tmp_path, script)
    launcher.start()
    try:
        wait_for(lambda: runs(out))
        log = tmp_path / "worker-local.log"
        wait_for(lambda: log.exists() and "hello from stderr" in log.read_text())
    finally:
        launcher.stop()

    assert "hello from stdout" in log.read_text()


def test_frozen_servers_reset_the_pyinstaller_environment(registry, tmp_path, script, monkeypatch):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    launcher, out = make(registry, tmp_path, script)
    launcher.start()
    try:
        first = wait_for(lambda: runs(out) or None)[0]
    finally:
        launcher.stop()

    assert first["frozen_env"] == "1"


def test_stop_terminates_the_process(registry, tmp_path, script):
    launcher, out = make(registry, tmp_path, script)
    launcher.start()
    pid = wait_for(lambda: launcher.status()["pid"])

    launcher.stop()

    assert not alive_after(pid)
    assert launcher.status()["state"] == "stopped" and launcher.status()["pid"] is None


def alive_after(pid: int) -> bool:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not _is_running(pid):
            return False
        time.sleep(0.02)
    return True


def _is_running(pid: int) -> bool:
    # a terminated child that was reaped is gone; a zombie still answers kill(0)
    try:
        with open(f"/proc/{pid}/stat") as handle:
            return handle.read().split(") ")[-1].split()[0] != "Z"
    except OSError:
        return alive(pid)


@pytest.mark.skipif(sys.platform == "win32", reason="SIGTERM handling differs on Windows")
def test_stop_kills_a_process_that_ignores_terminate(registry, tmp_path, script):
    launcher, out = make(registry, tmp_path, script, mode="ignore-term", stop_timeout_seconds=0.5)
    launcher.start()
    pid = wait_for(lambda: launcher.status()["pid"])
    wait_for(lambda: runs(out))
    time.sleep(0.3)  # let the script install its signal handler

    started = time.monotonic()
    launcher.stop()

    assert time.monotonic() - started < 5
    assert not alive_after(pid)


def test_a_process_that_exits_is_restarted_with_a_fresh_token(registry, tmp_path, script):
    launcher, out = make(registry, tmp_path, script, mode="exit3")
    launcher.start()
    try:
        wait_for(lambda: len(runs(out)) >= 3)
        status = launcher.status()
    finally:
        launcher.stop()

    tokens = [json.loads(run["stdin"])["token"] for run in runs(out)]
    assert len(set(tokens)) == len(tokens)
    assert status["last_exit_code"] == 3 and status["restarts"] >= 2
    assert registry.authenticate(tokens[-1]) is not None
    assert all(registry.authenticate(token) is None for token in tokens[:-1])


def test_an_exit_after_stopping_soon_is_expected_and_not_restarted(registry, tmp_path, script, caplog):
    """A Ctrl+C reaches the worker too; once the server is stopping its exit is no surprise."""
    launcher, out = make(registry, tmp_path, script)
    launcher.start()
    try:
        first = wait_for(lambda: runs(out))[0]["pid"]
        launcher.stopping_soon()
        os.kill(first, 15)  # the worker stops on its own, as with a signal to the process group
        wait_for(lambda: not alive(first))
        time.sleep(0.5)  # longer than the backoff: a restart would have happened by now
        assert len(runs(out)) == 1
        assert not [record for record in caplog.records if "exited with code" in record.getMessage()]
    finally:
        launcher.stop()
    assert launcher.status()["state"] == "stopped"


class _FakeProcess:
    pid = 4242

    def __init__(self, signal_error: OSError | None = None):
        self.calls: list[tuple] = []
        self._signal_error = signal_error

    def send_signal(self, number):
        self.calls.append(("signal", number))
        if self._signal_error is not None:
            raise self._signal_error

    def terminate(self):
        self.calls.append(("terminate",))

    def kill(self):
        self.calls.append(("kill",))


@pytest.fixture
def on_windows(monkeypatch):
    """The launcher's Windows branch, with taskkill recorded instead of run."""
    import signal
    import types

    monkeypatch.setattr(launcher_module, "sys", types.SimpleNamespace(platform="win32"))
    monkeypatch.setattr(signal, "CTRL_BREAK_EVENT", 1, raising=False)
    commands: list[list[str]] = []
    monkeypatch.setattr(launcher_module.subprocess, "run", lambda command, **kw: commands.append(command))
    return commands


def test_on_windows_the_worker_is_asked_to_stop_with_a_break_event(on_windows):
    process = _FakeProcess()
    launcher_module._ask_to_stop(process)
    # Not TerminateProcess: that would end only the onefile launcher, not the worker inside it.
    assert process.calls == [("signal", 1)]
    assert on_windows == []


def test_on_windows_without_a_console_the_whole_tree_is_ended(on_windows):
    process = _FakeProcess(signal_error=OSError("no console"))
    launcher_module._ask_to_stop(process)
    assert on_windows == [["taskkill", "/F", "/T", "/PID", "4242"]]
    assert process.calls == [("signal", 1), ("kill",)]


def test_on_windows_a_worker_that_does_not_stop_is_killed_with_its_tree(on_windows):
    process = _FakeProcess()
    launcher_module._kill_tree(process)
    assert on_windows == [["taskkill", "/F", "/T", "/PID", "4242"]]
    assert process.calls == [("kill",)]


def test_backoff_grows_to_its_cap(registry, tmp_path, script, monkeypatch):
    delays: list[float] = []

    class Recording(threading.Event):
        def wait(self, timeout=None):
            delays.append(timeout)
            return super().wait(0.01)

    launcher, out = make(registry, tmp_path, script, mode="exit3", backoff_seconds=(1, 2, 4))
    launcher._stopping = Recording()
    launcher.start()
    try:
        wait_for(lambda: len(delays) >= 5)
    finally:
        launcher.stop()

    assert delays[:5] == [1, 2, 4, 4, 4]


def test_backoff_resets_after_a_long_run(registry, tmp_path, script):
    delays: list[float] = []

    class Recording(threading.Event):
        def wait(self, timeout=None):
            delays.append(timeout)
            return super().wait(0.01)

    launcher, _ = make(registry, tmp_path, script, mode="exit3", backoff_seconds=(1, 2, 4),
                       stable_after_seconds=0)
    launcher._stopping = Recording()
    launcher.start()
    try:
        wait_for(lambda: len(delays) >= 3)
    finally:
        launcher.stop()

    assert delays[:3] == [1, 1, 1]


def test_default_backoff_schedule():
    assert launcher_module.BACKOFF_SECONDS == (1, 2, 4, 8, 16, 32, 60)
    assert launcher_module.STABLE_AFTER_SECONDS == 300


def test_a_missing_executable_is_reported_once(registry, tmp_path, caplog):
    def missing():
        raise FileNotFoundError("/nowhere/ShadeWorker")

    launcher, _ = make(registry, tmp_path, tmp_path / "unused.py", command=missing)
    with caplog.at_level("WARNING"):
        launcher.start()
        wait_for(lambda: launcher.status()["state"] == "missing")
        launcher.stop()
        launcher.start()
        wait_for(lambda: launcher.status()["state"] == "missing")
        launcher.stop()

    assert launcher.status()["pid"] is None
    assert caplog.text.count("cannot be started") == 1


def test_a_command_that_cannot_be_executed_is_missing(registry, tmp_path):
    launcher, _ = make(registry, tmp_path, tmp_path / "unused.py", command=lambda: [str(tmp_path / "nothing-here")])
    launcher.start()
    try:
        wait_for(lambda: launcher.status()["state"] == "missing")
    finally:
        launcher.stop()


def test_disabled_by_the_setting(registry, tmp_path, script):
    launcher, out = make(registry, tmp_path, script, launch_local="off")

    launcher.start()

    assert launcher.status()["state"] == "disabled"
    time.sleep(0.2)
    assert runs(out) == []
    with pytest.raises(KeyError):
        registry.get("local")  # nothing was provisioned
    launcher.stop()
    assert launcher.status()["state"] == "disabled"


def test_the_setting_is_read_from_the_settings_store_by_default(registry, tmp_path, script, monkeypatch):
    seen = []
    monkeypatch.setattr("src.settings.get_setting", lambda key: seen.append(key) or "off")
    launcher, _ = make(registry, tmp_path, script, launch_local=None)

    launcher.start()

    assert seen == ["workers.launch_local"] and launcher.status()["state"] == "disabled"


def test_restart_starts_a_new_process(registry, tmp_path, script):
    launcher, out = make(registry, tmp_path, script)
    launcher.start()
    try:
        first = wait_for(lambda: launcher.status()["pid"])
        status = launcher.restart()
        second = wait_for(lambda: launcher.status()["pid"])
    finally:
        launcher.stop()

    assert second != first
    assert status["state"] in {"stopped", "running"}
    assert len(runs(out)) == 2


def test_start_twice_runs_one_process(registry, tmp_path, script):
    launcher, out = make(registry, tmp_path, script)
    launcher.start()
    launcher.start()
    try:
        wait_for(lambda: runs(out))
        time.sleep(0.3)
    finally:
        launcher.stop()

    assert len(runs(out)) == 1


def test_default_command_from_source(tmp_path, monkeypatch):
    monkeypatch.setattr(launcher_module, "app_dir", lambda: tmp_path)
    with pytest.raises(FileNotFoundError):
        default_command()
    (tmp_path / "shade_worker.py").write_text("")

    assert default_command() == [sys.executable, str(tmp_path / "shade_worker.py"), "run", "--stdin-credentials"]


def test_default_command_when_frozen(tmp_path, monkeypatch):
    monkeypatch.setattr(launcher_module, "app_dir", lambda: tmp_path)
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    with pytest.raises(FileNotFoundError):
        default_command()
    name = "ShadeWorker.exe" if os.name == "nt" else "ShadeWorker"
    (tmp_path / name).write_text("")

    assert default_command() == [str(tmp_path / name), "run", "--stdin-credentials"]


def test_server_urls():
    from types import SimpleNamespace

    def url(host, port=8000):
        return default_server_url(SimpleNamespace(host=host, port=port))

    assert url("127.0.0.1") == "https://127.0.0.1:8000"
    assert url("0.0.0.0", 9) == "https://127.0.0.1:9"
    assert url("::") == "https://[::1]:8000"
    assert url("::1") == "https://[::1]:8000"
    assert url("192.168.1.5") == "https://192.168.1.5:8000"
