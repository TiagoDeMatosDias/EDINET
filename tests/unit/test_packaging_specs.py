"""The PyInstaller specs keep the translation engine out of the server and every handler in the worker."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from src.jobs.kinds import KINDS

ROOT = Path(__file__).resolve().parents[2]
SERVER_SPEC = (ROOT / "EDINET.spec").read_text(encoding="utf-8")
WORKER_SPEC = (ROOT / "ShadeWorker.spec").read_text(encoding="utf-8")
ENGINE = ("torch", "argostranslate", "ctranslate2", "sentencepiece", "stanza", "spacy")


def _excludes(spec: str) -> set[str]:
    match = re.search(r"excludes=(\[.*?\])", spec, re.S)
    assert match, "spec has no excludes"
    return set(ast.literal_eval(match.group(1)))


def test_server_spec_excludes_translation_engine() -> None:
    assert set(ENGINE) | {"triton"} <= _excludes(SERVER_SPEC)


def test_worker_spec_is_a_console_exe_without_upx_or_frontend() -> None:
    assert "['shade_worker.py']" in WORKER_SPEC
    assert "name='ShadeWorker'" in WORKER_SPEC
    assert "upx=False" in WORKER_SPEC
    assert "console=True" in WORKER_SPEC
    assert "'triton'" in WORKER_SPEC
    assert "frontend-v2" not in WORKER_SPEC.replace("NOT the frontend", "")
    assert "cloudflared" not in WORKER_SPEC.replace("neither the web frontend nor cloudflared", "")
    assert not set(ENGINE) & _excludes(WORKER_SPEC)


def test_worker_spec_collects_handlers_from_the_kind_table() -> None:
    # The spec derives handler modules from KINDS and walks the worker package.
    assert "from src.jobs.kinds import KINDS" in WORKER_SPEC
    assert "_modules_under('src', 'worker')" in WORKER_SPEC
    for name in ("src.backtesting", "src.screening", "src.filings", "src.ownership", "argostranslate"):
        assert f"'{name}'" in WORKER_SPEC
    for spec in KINDS.values():
        module = spec.handler.partition(":")[0]
        assert (ROOT / Path(*module.split("."))).with_suffix(".py").is_file() or (ROOT / Path(*module.split(".")) / "__init__.py").is_file(), module


def test_entry_point_runs_freeze_support_first() -> None:
    tree = ast.parse((ROOT / "shade_worker.py").read_text(encoding="utf-8"))
    guard = next(node for node in tree.body if isinstance(node, ast.If))
    assert ast.unparse(guard.body[0]) == "multiprocessing.freeze_support()"
