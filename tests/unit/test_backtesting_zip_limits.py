"""Bounded on-disk rolling backtest archive tests."""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from src.backtesting.zip_export import (
    ExportSizeLimitExceeded,
    save_rolling_backtest_zip,
)


def _rolling_result() -> dict:
    return {
        "config": {"cadence": "yearly", "durations": ["1yr"]},
        "aggregate": {"total_runs": 1, "successful": 1, "failed": 0},
        "results": [
            {
                "period": "2020-01",
                "backtests": {
                    "equal": {
                        "1yr": {
                            "metrics": {
                                "total_return": 0.1,
                                "sharpe_ratio": 0.5,
                                "max_drawdown": -0.1,
                            },
                            "daily": [
                                {"date": "2020-01-02", "Ticker": "10010"}
                            ],
                        }
                    }
                },
            }
        ],
    }


def test_rolling_archive_streams_to_final_zip(tmp_path):
    saved_directory = Path(
        save_rolling_backtest_zip(
            _rolling_result(),
            str(tmp_path),
            1024 * 1024,
        )
    )

    archive_path = saved_directory / "backtest.zip"
    assert archive_path.is_file()
    assert not (saved_directory / "backtest.zip.partial").exists()
    with zipfile.ZipFile(archive_path) as archive:
        assert any(
            name.endswith("/per_company_per_day.csv")
            for name in archive.namelist()
        )


def test_rolling_archive_limit_removes_partial_directory(tmp_path):
    with pytest.raises(ExportSizeLimitExceeded):
        save_rolling_backtest_zip(
            _rolling_result(),
            str(tmp_path),
            32,
        )

    assert list(tmp_path.iterdir()) == []


def test_rolling_archive_of_a_screen_with_no_runs(tmp_path):
    """A screen that matched nothing reports ``stats: None``; the archive still builds."""
    empty = {
        "config": {"cadence": "yearly"},
        "aggregate": {"total_runs": 0, "successful": 0, "failed": 0, "benchmark_comparison": None, "stats": None},
        "results": [],
    }

    saved = Path(save_rolling_backtest_zip(empty, str(tmp_path), 10_000_000))

    with zipfile.ZipFile(saved / "backtest.zip") as archive:
        assert "Total backtests:   0" in archive.read("summary.txt").decode()
