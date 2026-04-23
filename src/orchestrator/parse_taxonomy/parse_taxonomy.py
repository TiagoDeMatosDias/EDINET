import json
import logging

from src.orchestrator.common import StepDefinition

from . import taxonomy_processing

logger = logging.getLogger(__name__)


def _as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def run_parse_taxonomy(config, overwrite=False):
    logger.info("Parsing EDINET taxonomy...")
    step_cfg = config.get("parse_taxonomy_config", {})
    target_database = step_cfg.get("Target_Database")

    xsd_file = step_cfg.get("xsd_file")
    if xsd_file:
        release_year = step_cfg.get("release_year")
        return taxonomy_processing.import_local_taxonomy_xsd(
            target_database=target_database,
            xsd_file=xsd_file,
            namespace_prefix=step_cfg.get("namespace_prefix"),
            release_label=step_cfg.get("release_label"),
            release_year=int(release_year) if str(release_year or "").strip() else None,
            taxonomy_date=step_cfg.get("taxonomy_date"),
        )

    raw_years = step_cfg.get("release_years") or []
    if isinstance(raw_years, str):
        try:
            raw_years = json.loads(raw_years)
        except Exception:
            raw_years = [raw_years]
    release_years = [int(value) for value in raw_years if str(value).strip()]

    raw_namespaces = step_cfg.get("namespaces") or ["jppfs_cor", "jpcrp_cor"]
    if isinstance(raw_namespaces, str):
        try:
            raw_namespaces = json.loads(raw_namespaces)
        except Exception:
            raw_namespaces = [raw_namespaces]

    return taxonomy_processing.sync_taxonomy_releases(
        target_database=target_database,
        release_selection=step_cfg.get("release_selection", "all"),
        release_years=release_years,
        namespaces=raw_namespaces,
        download_dir=step_cfg.get("download_dir", "assets/taxonomy"),
        force_download=_as_bool(step_cfg.get("force_download", False)),
        force_reparse=_as_bool(step_cfg.get("force_reparse", overwrite)),
    )


STEP_DEFINITION = StepDefinition(
    name="parse_taxonomy",
    handler=run_parse_taxonomy,
    required_config_fields=(("parse_taxonomy_config", "Target_Database"),),
)