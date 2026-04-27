"""Tests for the orchestrator module."""

import importlib
from pathlib import Path
import sys
import textwrap
import threading
import pytest
from unittest.mock import patch, MagicMock

from config import Config


def _purge_package_modules(package_name: str) -> None:
    for module_name in list(sys.modules):
        if module_name == package_name or module_name.startswith(f"{package_name}."):
            sys.modules.pop(module_name, None)


def _write_step_package(
    root_path: Path,
    package_name: str,
    step_name: str,
    alias: str,
) -> None:
    package_dir = root_path / package_name
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "__init__.py").write_text("", encoding="utf-8")

    step_dir = package_dir / step_name
    step_dir.mkdir(parents=True, exist_ok=True)
    (step_dir / "__init__.py").write_text(
        f"from .{step_name} import STEP_DEFINITION\n\n"
        f"__all__ = [\"STEP_DEFINITION\"]\n",
        encoding="utf-8",
    )
    (step_dir / f"{step_name}.py").write_text(
        textwrap.dedent(
            f"""\
            from src.orchestrator.common import StepDefinition, StepFieldDefinition


            def run_{step_name}(config, overwrite=False):
                return {{"step": "{step_name}", "overwrite": overwrite}}


            STEP_DEFINITION = StepDefinition(
                name="{step_name}",
                handler=run_{step_name},
                required_keys=("API_KEY",),
                input_fields=(
                    StepFieldDefinition("Target_Database", "database", required=True),
                ),
            )
            """
        ),
        encoding="utf-8",
    )


class TestRunPipeline:
    """Test orchestrator.run with mocked step execution."""

    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    @staticmethod
    def _make_pipeline_config() -> Config:
        return Config.from_dict({
            "baseURL": "http://example.com",
            "API_KEY": "key123",
            "get_documents_config": {"Target_Database": "test.db"},
            "download_documents_config": {"Target_Database": "test.db"},
        })

    @patch("src.orchestrator.orchestrator.execute_step")
    def test_basic_run(self, mock_execute):
        from src.orchestrator import run

        config = self._make_pipeline_config()
        steps = [
            {"name": "get_documents", "overwrite": False},
            {"name": "download_documents", "overwrite": False},
        ]

        started = []
        done = []

        run(
            steps=steps,
            config=config,
            on_step_start=started.append,
            on_step_done=done.append,
        )

        assert mock_execute.call_count == 2
        assert started == ["get_documents", "download_documents"]
        assert done == ["get_documents", "download_documents"]

    @patch("src.orchestrator.orchestrator.execute_step")
    def test_cancellation(self, mock_execute):
        from src.orchestrator import run

        config = self._make_pipeline_config()
        cancel = threading.Event()
        cancel.set()  # pre-cancel

        steps = [{"name": "get_documents"}]
        started = []

        run(
            steps=steps,
            config=config,
            on_step_start=started.append,
            cancel_event=cancel,
        )

        assert mock_execute.call_count == 0
        assert started == []

    @patch("src.orchestrator.orchestrator.execute_step", side_effect=RuntimeError("fail"))
    def test_error_callback(self, mock_execute):
        from src.orchestrator import run

        config = self._make_pipeline_config()
        steps = [{"name": "get_documents"}]

        errors = []

        with pytest.raises(RuntimeError):
            run(
                steps=steps,
                config=config,
                on_step_error=lambda name, exc: errors.append((name, exc)),
            )

        assert len(errors) == 1
        assert errors[0][0] == "get_documents"


class TestExecuteStep:
    """Test that execute_step dispatches to the correct handler."""

    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_dispatches_known_step(self):
        from src.orchestrator.orchestrator import execute_step

        mock_handler = MagicMock()
        config = Config.from_dict({})
        with patch.dict("src.orchestrator.orchestrator.STEP_HANDLERS", {"get_documents": mock_handler}):
            execute_step("get_documents", config, overwrite=True)

        mock_handler.assert_called_once_with(config, overwrite=True)

    def test_unknown_step_does_not_raise(self):
        from src.orchestrator.orchestrator import execute_step

        config = Config.from_dict({})
        execute_step("nonexistent_step", config)  # should not raise

    def test_discovery_registers_canonical_steps(self):
        from src.orchestrator import list_available_steps
        from src.orchestrator.orchestrator import STEP_HANDLERS

        available_steps = list_available_steps()
        available_step_names = {step["name"] for step in available_steps}

        assert "generate_financial_statements" in STEP_HANDLERS
        assert "generate_financial_statements" in available_step_names
        assert any(
            step["name"] == "generate_financial_statements"
            and step["config_key"] == "generate_financial_statements_config"
            for step in available_steps
        )


def test_build_step_registry_discovers_custom_step_package(tmp_path, monkeypatch):
    from src.orchestrator.common import build_step_registry

    package_name = "temp_orchestrator_pkg"
    monkeypatch.syspath_prepend(str(tmp_path))
    _write_step_package(tmp_path, package_name, "alpha_step", "Alpha Step")

    importlib.invalidate_caches()
    _purge_package_modules(package_name)
    handlers, step_definitions, discovered_modules = build_step_registry(
        package_name=package_name,
    )

    assert "alpha_step" in handlers
    assert step_definitions["alpha_step"].name == "alpha_step"
    assert step_definitions["alpha_step"].required_keys == ("API_KEY",)
    assert step_definitions["alpha_step"].resolved_config_key == "alpha_step_config"
    assert step_definitions["alpha_step"].required_input_fields[0].key == "Target_Database"
    assert f"{package_name}.alpha_step" in discovered_modules


def test_build_step_registry_picks_up_new_step_folder_on_refresh(tmp_path, monkeypatch):
    from src.orchestrator.common import build_step_registry

    package_name = "temp_orchestrator_pkg_refresh"
    monkeypatch.syspath_prepend(str(tmp_path))
    _write_step_package(tmp_path, package_name, "alpha_step", "Alpha Step")

    importlib.invalidate_caches()
    _purge_package_modules(package_name)
    handlers, _, discovered_modules = build_step_registry(package_name=package_name)
    assert "alpha_step" in handlers
    assert "beta_step" not in handlers
    assert f"{package_name}.alpha_step" in discovered_modules

    _write_step_package(tmp_path, package_name, "beta_step", "Beta Step")

    importlib.invalidate_caches()
    _purge_package_modules(package_name)
    handlers, _, discovered_modules = build_step_registry(package_name=package_name)

    assert "beta_step" in handlers
    assert f"{package_name}.beta_step" in discovered_modules


class TestValidateInput:
    """Test pre-flight config validation."""

    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_missing_keys_raises(self):
        from src.orchestrator import validate_input

        config = Config.from_dict({})
        with pytest.raises(RuntimeError, match="missing"):
            validate_input(config, steps=[{"name": "get_documents"}])

    def test_all_keys_present_passes(self):
        from src.orchestrator import validate_input

        config = Config.from_dict({
            "baseURL": "http://example.com",
            "API_KEY": "key123",
            "get_documents_config": {"Target_Database": "test.db"},
        })
        validate_input(config, steps=[{"name": "get_documents"}])  # should not raise

    def test_validate_input_applies_step_defined_defaults(self):
        from src.orchestrator import validate_input

        config = Config.from_dict({
            "DB_STOCK_PRICES_TABLE": "stock_prices",
            "import_stock_prices_csv_config": {
                "Target_Database": "prices.db",
                "csv_file": "prices.csv",
            },
        })

        validate_input(config, steps=[{"name": "import_stock_prices_csv"}])

        step_cfg = config.get("import_stock_prices_csv_config")
        assert step_cfg["default_currency"] == "JPY"
        assert step_cfg["date_column"] == "Date"
        assert step_cfg["price_column"] == "Price"

    def test_parse_taxonomy_no_longer_requires_legacy_taxonomy_table_key(self):
        from src.orchestrator import validate_input

        config = Config.from_dict({
            "parse_taxonomy_config": {"Target_Database": "taxonomy.db"},
        })
        validate_input(config, steps=[{"name": "parse_taxonomy"}])

    def test_validate_input_rejects_unknown_step(self):
        from src.orchestrator import validate_input

        config = Config.from_dict({})
        with pytest.raises(RuntimeError, match="Unknown orchestrator step"):
            validate_input(config, steps=[{"name": "missing_step"}])

    def test_validate_input_rejects_invalid_numeric_field(self):
        from src.orchestrator import validate_input

        config = Config.from_dict({
            "generate_ratios_config": {
                "Database": "ratios.db",
                "batch_size": "not-a-number",
            }
        })

        with pytest.raises(RuntimeError, match="must be numeric"):
            validate_input(config, steps=[{"name": "generate_ratios"}])


class TestGenerateRatiosStep:
    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_uses_local_generate_ratios_function(self):
        from src.orchestrator.generate_ratios.generate_ratios import run_generate_ratios

        config = Config.from_dict({
            "generate_ratios_config": {
                "Database": "ratios.db",
                "batch_size": 123,
            }
        })

        with patch(
            "src.orchestrator.generate_ratios.generate_ratios.generate_ratios"
        ) as mock_generate:
            run_generate_ratios(config, overwrite=True)

        mock_generate.assert_called_once_with(
            database="ratios.db",
            overwrite=True,
            batch_size=123,
        )


class TestParseTaxonomyStep:
    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_syncs_remote_taxonomy_when_no_local_xsd_is_supplied(self):
        from src.orchestrator.parse_taxonomy.parse_taxonomy import run_parse_taxonomy

        config = Config.from_dict({
            "parse_taxonomy_config": {
                "Target_Database": "taxonomy.db",
                "release_years": "[2025]",
                "namespaces": "[\"jppfs_cor\", \"jpcrp_cor\"]",
                "download_dir": "assets/taxonomy",
                "force_download": "True",
                "force_reparse": "False",
            }
        })

        with patch(
            "src.orchestrator.parse_taxonomy.parse_taxonomy.taxonomy_processing.sync_taxonomy_releases"
        ) as mock_sync:
            run_parse_taxonomy(config, overwrite=False)

        mock_sync.assert_called_once_with(
            target_database="taxonomy.db",
            release_selection="all",
            release_years=[2025],
            namespaces=["jppfs_cor", "jpcrp_cor"],
            download_dir="assets/taxonomy",
            force_download=True,
            force_reparse=False,
        )


class TestImportStockPricesCsvStep:
    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_uses_price_column_default_matching_backup_csv_schema(self):
        from src.orchestrator.import_stock_prices_csv.import_stock_prices_csv import run_import_stock_prices_csv

        config = Config.from_dict({
            "DB_STOCK_PRICES_TABLE": "stock_prices",
            "import_stock_prices_csv_config": {
                "Target_Database": "prices.db",
                "csv_file": "prices.csv",
            },
        })

        with patch(
            "src.orchestrator.import_stock_prices_csv.import_stock_prices_csv.import_stock_prices_csv"
        ) as mock_import:
            run_import_stock_prices_csv(config, overwrite=False)

        mock_import.assert_called_once_with(
            db_name="prices.db",
            prices_table="stock_prices",
            csv_path="prices.csv",
            default_ticker="",
            default_currency="JPY",
            date_column="Date",
            price_column="Price",
            ticker_column="",
            currency_column="",
        )


class TestUpdateStockPricesStep:
    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_uses_local_update_all_stock_prices_function(self):
        from src.orchestrator.update_stock_prices.update_stock_prices import run_update_stock_prices

        config = Config.from_dict({
            "DB_COMPANY_INFO_TABLE": "company_info",
            "DB_STOCK_PRICES_TABLE": "stock_prices",
            "DB_FINANCIAL_DATA_TABLE": "financial_data",
            "update_stock_prices_config": {
                "Target_Database": "prices.db",
            },
        })

        with patch(
            "src.orchestrator.update_stock_prices.update_stock_prices.update_all_stock_prices"
        ) as mock_update:
            run_update_stock_prices(config, overwrite=False)

        mock_update.assert_called_once_with(
            db_name="prices.db",
            Company_Table="company_info",
            prices_table="stock_prices",
            standardized_table="financial_data",
        )


class TestGenerateFinancialStatementsStep:
    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_passes_granularity_level(self):
        from src.orchestrator.generate_financial_statements.generate_financial_statements import run_generate_financial_statements

        config = Config.from_dict({
            "generate_financial_statements_config": {
                "Source_Database": "base.db",
                "Target_Database": "standardized.db",
                "Granularity_level": 5,
            },
        })

        with patch(
            "src.orchestrator.generate_financial_statements.generate_financial_statements.financial_statement_services.generate_financial_statements"
        ) as mock_generate:
            run_generate_financial_statements(config, overwrite=False)

        mock_generate.assert_called_once_with(
            source_database="base.db",
            target_database="standardized.db",
            granularity_level=5,
            overwrite=False,
        )


class TestGenerateRollingMetricsStep:
    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_passes_source_and_target_databases(self):
        from src.orchestrator.generate_rolling_metrics.generate_rolling_metrics import run_generate_rolling_metrics

        config = Config.from_dict({
            "generate_rolling_metrics_config": {
                "Source_Database": "base.db",
                "Target_Database": "standardized.db",
            },
        })

        with patch(
            "src.orchestrator.generate_rolling_metrics.generate_rolling_metrics.rolling_metrics_services.generate_rolling_metrics"
        ) as mock_generate:
            run_generate_rolling_metrics(config, overwrite=True)

        mock_generate.assert_called_once_with(
            source_database="base.db",
            target_database="standardized.db",
            overwrite=True,
        )


class TestConfigFromDict:
    """Test Config.from_dict."""

    def test_from_dict_creates_instance(self):
        cfg = Config.from_dict({"key": "value"})
        assert cfg.get("key") == "value"

    def test_from_dict_independent_instances(self):
        cfg1 = Config.from_dict({"key": "a"})
        cfg2 = Config.from_dict({"key": "b"})
        assert cfg1.get("key") == "a"
        assert cfg2.get("key") == "b"
