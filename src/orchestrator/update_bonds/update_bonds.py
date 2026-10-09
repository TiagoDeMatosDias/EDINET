"""Orchestrator step that keeps ``Bonds.db`` current.

Downloads new shelf-registration supplements (EDINET document type 100),
reads the bond schedule in each company's latest annual report from the
filing catalog, refreshes the Ministry of Finance JGB curve, and rebuilds
the merged bond list the Analysis and Research pages read. Run it after
``get_documents`` and ``download_xbrl`` so new filings are listed and stored.

Auto-discovered by ``build_step_registry()`` in
``src/orchestrator/common/__init__.py``.
"""

from __future__ import annotations

import logging
import os

from src.orchestrator.common import StepDefinition, StepFieldDefinition
from src.orchestrator.common.db_config import get_bonds_db, get_db1, get_db2, get_filings_db

logger = logging.getLogger(__name__)


def _flag(value, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, str):
        return value.strip().lower() not in ("0", "false", "no", "off")
    return bool(value)


def run_update_bonds(config, overwrite=False, context=None):
    """Handler for the *update_bonds* orchestrator step."""
    from src.bonds.update import update_bonds
    from src.filings.acquisition import EdinetDownloadClient

    step_cfg = config.get("update_bonds_config", {}) or {}
    issuances = _flag(step_cfg.get("issuance_documents"), True)
    token = str(config.get("API_KEY", "") or os.getenv("EDINET_API_TOKEN", "")).strip()
    client = EdinetDownloadClient(token) if issuances and token else None
    if issuances and client is None:
        logger.warning("update_bonds: no EDINET API key, so only bond supplements already stored can be read.")

    def progress(done: int, total: int, message: str) -> None:
        if context is not None:
            context.report_progress(done, total, message)

    try:
        result = update_bonds(
            bonds_db=get_bonds_db(),
            db1_path=get_db1(),
            db2_path=get_db2(),
            filings_db_path=get_filings_db(),
            client=client,
            issuances=issuances,
            annual_reports=_flag(step_cfg.get("annual_reports"), True),
            curve=_flag(step_cfg.get("jgb_curve"), True),
            market_prices=_flag(step_cfg.get("market_prices"), True),
            market_days=int(step_cfg.get("market_days") or 5),
            reparse=bool(overwrite),
            max_documents=int(step_cfg.get("max_documents") or 0),
            progress=progress,
        )
    finally:
        if client is not None:
            client.close()
    logger.info("Bond update finished: %s", result)
    return result


STEP_DEFINITION = StepDefinition(
    name="update_bonds",
    handler=run_update_bonds,
    required_keys=(),
    display_name="Update bonds",
    supports_overwrite=True,
    input_fields=(
        StepFieldDefinition(
            key="issuance_documents",
            field_type="bool",
            default=True,
            label="Bond supplements",
            description=(
                "Download new shelf-registration supplements (発行登録追補書類) listed by get_documents "
                "and read each bond's terms and ratings. Needs the EDINET API key."
            ),
        ),
        StepFieldDefinition(
            key="annual_reports",
            field_type="bool",
            default=True,
            label="Annual-report bond schedules",
            description="Read the bond schedule (社債明細表) in each company's latest annual report in the filing catalog.",
        ),
        StepFieldDefinition(
            key="jgb_curve",
            field_type="bool",
            default=True,
            label="JGB curve",
            description="Fetch the Ministry of Finance government bond yields used to measure spreads.",
        ),
        StepFieldDefinition(
            key="market_prices",
            field_type="bool",
            default=True,
            label="JSDA reference prices",
            description=(
                "Read the Japan Securities Dealers Association's daily OTC reference prices "
                "(公社債店頭売買参考統計値) and match them to the bonds, for market yields and spreads."
            ),
        ),
        StepFieldDefinition(
            key="market_days",
            field_type="num",
            default=5,
            label="Reference-price days",
            description="How many of the newest daily JSDA files to read when they are not stored yet. The site limits request rates, so files are read ten seconds apart; a refusal stops the run and the rest are read next time.",
        ),
        StepFieldDefinition(
            key="max_documents",
            field_type="num",
            default=0,
            label="Max documents",
            description="Read at most this many new filings of each kind per run; 0 reads them all.",
        ),
    ),
)
