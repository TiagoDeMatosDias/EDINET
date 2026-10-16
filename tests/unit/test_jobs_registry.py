"""Worker registry: tokens, revocation, enrollment strings."""

from __future__ import annotations

import base64
import hashlib
import json
import sqlite3

import pytest

from src.jobs.registry import WorkerRegistry, hash_secret, parse_enrollment


@pytest.fixture
def registry(tmp_path):
    return WorkerRegistry(tmp_path / "app.db")


def test_local_worker_gets_a_fresh_token_each_time(registry):
    first = registry.ensure_local_worker()
    second = registry.ensure_local_worker()

    assert first.startswith("srw_") and second.startswith("srw_") and first != second
    assert registry.authenticate(first) is None
    worker = registry.authenticate(second)
    assert worker is not None and worker.worker_id == "local" and worker.kind == "local"
    assert worker.enabled and worker.name == "Local worker"


def test_only_the_hash_of_a_token_is_stored(registry, tmp_path):
    token = registry.ensure_local_worker()

    with sqlite3.connect(tmp_path / "app.db") as conn:
        stored = conn.execute("SELECT token_hash FROM workers").fetchone()[0]
    assert stored == hashlib.sha256(token.encode()).hexdigest() == hash_secret(token)
    assert token not in stored


def test_authenticate_rejects_unknown_and_malformed_tokens(registry):
    registry.ensure_local_worker()
    assert registry.authenticate("") is None
    assert registry.authenticate("srw_nope") is None
    assert registry.authenticate("not-a-worker-token") is None


def test_refreshing_the_local_worker_keeps_its_settings(registry):
    registry.ensure_local_worker()
    registry.update("local", batch_slots=4, enabled=False)
    registry.ensure_local_worker()

    worker = registry.get("local")
    assert worker.batch_slots == 4 and worker.enabled is False


def test_update_changes_fields_and_bumps_config_version(registry):
    registry.ensure_local_worker()
    before = registry.get("local")

    after = registry.update("local", name="Main", interactive_slots=3, batch_slots=0, enabled=False)

    assert (after.name, after.interactive_slots, after.batch_slots, after.enabled) == ("Main", 3, 0, False)
    assert after.config_version == before.config_version + 1


def test_update_validates(registry):
    registry.ensure_local_worker()
    with pytest.raises(ValueError):
        registry.update("local", batch_slots=9)
    with pytest.raises(ValueError):
        registry.update("local", interactive_slots=-1)
    with pytest.raises(ValueError):
        registry.update("local", name=" ")
    with pytest.raises(ValueError):
        registry.update("local", token_hash="x")
    with pytest.raises(KeyError):
        registry.update("missing", name="x")


def _enroll(registry, name="Laptop"):
    text, expires = registry.create_enrollment(name, "admin-1", "https://127.0.0.1:8000", "ab" * 32)
    _, _, code = parse_enrollment(text)
    return text, code, expires


def test_enrollment_round_trip(registry):
    text, code, expires = _enroll(registry)

    assert text.startswith("SRW1.") and "=" not in text
    body = text[5:]
    assert json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))) == {
        "u": "https://127.0.0.1:8000", "f": "ab" * 32, "c": code,
    }
    assert parse_enrollment(text) == ("https://127.0.0.1:8000", "ab" * 32, code)

    worker, token = registry.redeem_enrollment(code, "ignored")

    assert worker.kind == "enrolled" and worker.name == "Laptop"
    assert (worker.interactive_slots, worker.batch_slots) == (1, 2)
    assert registry.authenticate(token).worker_id == worker.worker_id
    assert registry.get(worker.worker_id).worker_id == worker.worker_id


def test_enrollment_without_a_name_uses_the_request_name(registry):
    text, _ = registry.create_enrollment("", None, "https://x", "f")
    worker, _ = registry.redeem_enrollment(parse_enrollment(text)[2], "From request")
    assert worker.name == "From request"


def test_enrollment_is_single_use(registry):
    _, code, _ = _enroll(registry)
    registry.redeem_enrollment(code, "x")

    with pytest.raises(ValueError):
        registry.redeem_enrollment(code, "x")


def test_enrollment_expires(registry, tmp_path):
    _, code, _ = _enroll(registry)
    with sqlite3.connect(tmp_path / "app.db") as conn:
        conn.execute("UPDATE worker_enrollments SET expires_at = '2000-01-01T00:00:00+00:00'")

    with pytest.raises(ValueError):
        registry.redeem_enrollment(code, "x")


def test_enrollment_lasts_thirty_minutes(registry):
    from datetime import datetime, timedelta, timezone

    _, _, expires = _enroll(registry)
    remaining = datetime.fromisoformat(expires) - datetime.now(timezone.utc)
    assert timedelta(minutes=29) < remaining <= timedelta(minutes=30)


def test_unknown_codes_are_refused_and_codes_are_stored_hashed(registry, tmp_path):
    with pytest.raises(ValueError):
        registry.redeem_enrollment("totally-unknown-code", "x")
    _, code, _ = _enroll(registry)
    with sqlite3.connect(tmp_path / "app.db") as conn:
        stored = conn.execute("SELECT code_hash FROM worker_enrollments").fetchone()[0]
    assert stored == hash_secret(code) and code not in stored


def test_revoke_stops_the_token_and_hides_the_worker(registry):
    _, code, _ = _enroll(registry)
    worker, token = registry.redeem_enrollment(code, "x")

    registry.revoke(worker.worker_id)

    assert registry.authenticate(token) is None
    assert worker.worker_id not in [w.worker_id for w in registry.list()]
    assert worker.worker_id in [w.worker_id for w in registry.list(include_revoked=True)]
    assert registry.get(worker.worker_id).revoked_at is not None
    with pytest.raises(KeyError):
        registry.revoke(worker.worker_id)
    with pytest.raises(KeyError):
        registry.update(worker.worker_id, name="again")


def test_disabled_workers_still_authenticate(registry):
    token = registry.ensure_local_worker()
    registry.update("local", enabled=False)

    worker = registry.authenticate(token)
    assert worker is not None and worker.enabled is False


def test_list_puts_the_local_worker_first(registry):
    _, code, _ = _enroll(registry)
    registry.redeem_enrollment(code, "x")
    registry.ensure_local_worker()

    assert [w.kind for w in registry.list()] == ["local", "enrolled"]


def test_parse_enrollment_rejects_garbage():
    for bad in ("", "SRW1.", "SRW1.@@@", "XYZ.abc", "SRW1." + base64.urlsafe_b64encode(b"[1]").decode()):
        with pytest.raises(ValueError):
            parse_enrollment(bad)
