"""Tests for the orchestrator.run_pipeline function."""

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

    @patch("src.orchestrator.execute_step")
    @patch("src.orchestrator.d")
    @patch("src.orchestrator.edinet_api")
    def test_basic_run(self, mock_edinet_api, mock_d, mock_execute):
        from src.orchestrator import run_pipeline

        mock_edinet_api.Edinet.return_value = MagicMock()
        mock_d.data.return_value = MagicMock()

        config = Config.from_dict({"run_steps": {}})
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
    @patch("src.orchestrator.d")
    @patch("src.orchestrator.edinet_api")
    def test_cancellation(self, mock_edinet_api, mock_d, mock_execute):
        from src.orchestrator import run_pipeline

        mock_edinet_api.Edinet.return_value = MagicMock()
        mock_d.data.return_value = MagicMock()

        config = Config.from_dict({"run_steps": {}})
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
    @patch("src.orchestrator.d")
    @patch("src.orchestrator.edinet_api")
    def test_error_callback(self, mock_edinet_api, mock_d, mock_execute):
        from src.orchestrator import run_pipeline

        mock_edinet_api.Edinet.return_value = MagicMock()
        mock_d.data.return_value = MagicMock()

        config = Config.from_dict({"run_steps": {}})
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
