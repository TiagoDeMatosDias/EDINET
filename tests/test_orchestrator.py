"""Tests for the orchestrator module."""

import threading
import pytest
from unittest.mock import patch, MagicMock

from config import Config


class TestRunPipeline:
    """Test orchestrator.run_pipeline with mocked step execution."""

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

    @patch("src.orchestrator.execute_step")
    def test_basic_run(self, mock_execute):
        from src.orchestrator import run_pipeline

        config = self._make_pipeline_config()
        steps = [
            {"name": "get_documents", "overwrite": False},
            {"name": "download_documents", "overwrite": False},
        ]

        started = []
        done = []

        run_pipeline(
            steps=steps,
            config=config,
            on_step_start=started.append,
            on_step_done=done.append,
        )

        assert mock_execute.call_count == 2
        assert started == ["get_documents", "download_documents"]
        assert done == ["get_documents", "download_documents"]

    @patch("src.orchestrator.execute_step")
    def test_cancellation(self, mock_execute):
        from src.orchestrator import run_pipeline

        config = self._make_pipeline_config()
        cancel = threading.Event()
        cancel.set()  # pre-cancel

        steps = [{"name": "get_documents"}]
        started = []

        run_pipeline(
            steps=steps,
            config=config,
            on_step_start=started.append,
            cancel_event=cancel,
        )

        assert mock_execute.call_count == 0
        assert started == []

    @patch("src.orchestrator.execute_step", side_effect=RuntimeError("fail"))
    def test_error_callback(self, mock_execute):
        from src.orchestrator import run_pipeline

        config = self._make_pipeline_config()
        steps = [{"name": "get_documents"}]

        errors = []

        with pytest.raises(RuntimeError):
            run_pipeline(
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
        from src.orchestrator import execute_step

        mock_handler = MagicMock()
        config = Config.from_dict({})
        with patch.dict("src.orchestrator.STEP_HANDLERS", {"get_documents": mock_handler}):
            execute_step("get_documents", config, overwrite=True)

        mock_handler.assert_called_once_with(config, overwrite=True)

    def test_unknown_step_does_not_raise(self):
        from src.orchestrator import execute_step

        config = Config.from_dict({})
        execute_step("nonexistent_step", config)  # should not raise


class TestValidateConfig:
    """Test pre-flight config validation."""

    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_missing_keys_raises(self):
        from src.orchestrator import validate_config

        config = Config.from_dict({})
        with pytest.raises(RuntimeError, match="missing"):
            validate_config(config, ["get_documents"])

    def test_all_keys_present_passes(self):
        from src.orchestrator import validate_config

        config = Config.from_dict({
            "baseURL": "http://example.com",
            "API_KEY": "key123",
            "get_documents_config": {"Target_Database": "test.db"},
        })
        validate_config(config, ["get_documents"])  # should not raise

    def test_populate_business_descriptions_en_requires_config_fields(self):
        from src.orchestrator import validate_config

        config = Config.from_dict({})
        with pytest.raises(RuntimeError, match="populate_business_descriptions_en_config"):
            validate_config(config, ["populate_business_descriptions_en"])

    def test_parse_taxonomy_no_longer_requires_legacy_taxonomy_table_key(self):
        from src.orchestrator import validate_config

        config = Config.from_dict({
            "parse_taxonomy_config": {"Target_Database": "taxonomy.db"},
        })
        validate_config(config, ["parse_taxonomy"])


class TestParseTaxonomyStep:
    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_syncs_remote_taxonomy_when_no_local_xsd_is_supplied(self):
        from src.orchestrator import _step_parse_taxonomy

        processor = MagicMock()
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

        with patch("src.orchestrator.d.data", return_value=processor):
            _step_parse_taxonomy(config, overwrite=False)

        processor.sync_taxonomy_releases.assert_called_once_with(
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
        from src.orchestrator import _step_import_stock_prices_csv

        config = Config.from_dict({
            "DB_STOCK_PRICES_TABLE": "stock_prices",
            "import_stock_prices_csv_config": {
                "Target_Database": "prices.db",
                "csv_file": "prices.csv",
            },
        })

        with patch("src.orchestrator.stockprice_api.import_stock_prices_csv") as mock_import:
            _step_import_stock_prices_csv(config, overwrite=False)

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


class TestGenerateFinancialStatementsStep:
    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_passes_statement_hierarchy_depth(self):
        from src.orchestrator import _step_generate_financial_statements

        processor = MagicMock()
        config = Config.from_dict({
            "DB_FINANCIAL_DATA_TABLE": "financialData_full",
            "DB_COMPANY_INFO_TABLE": "companyInfo",
            "DB_STOCK_PRICES_TABLE": "Stock_Prices",
            "generate_financial_statements_config": {
                "Source_Database": "base.db",
                "Target_Database": "standardized.db",
                "Mappings_Config": "config/reference/canonical_metrics_config.json",
                "max_line_depth": 5,
            },
        })

        with patch("src.orchestrator.d.data", return_value=processor):
            _step_generate_financial_statements(config, overwrite=False)

        processor.generate_financial_statements.assert_called_once_with(
            source_database="base.db",
            source_table="financialData_full",
            target_database="standardized.db",
            mappings_config="config/reference/canonical_metrics_config.json",
            company_table="companyInfo",
            prices_table="Stock_Prices",
            overwrite=False,
            batch_size=2500,
            max_line_depth=5,
        )


class TestConfigFromDict:
    """Test Config.from_dict bypass of the singleton."""

    def setup_method(self):
        Config.reset()

    def teardown_method(self):
        Config.reset()

    def test_from_dict_creates_independent_instance(self):
        cfg = Config.from_dict({"key": "value"})
        assert cfg.get("key") == "value"
        assert cfg.run_config_path is None

    def test_from_dict_does_not_set_singleton(self):
        Config.from_dict({"key": "value"})
        assert Config._instance is None
