"""Tax-lot and scenario correctness tests."""

from datetime import date

import pytest

from src.portfolio.scenarios import ScenarioShock, apply_scenario
from src.portfolio.tax_lots import LotLedger, TaxLot


def test_fifo_realizes_oldest_lot_first():
    ledger = LotLedger("fifo")
    ledger.buy(TaxLot("old", "ABC", date(2024, 1, 1), 10, 10))
    ledger.buy(TaxLot("new", "ABC", date(2025, 1, 1), 10, 20))
    realized = ledger.sell("ABC", 12, 15)
    assert [item.lot_id for item in realized] == ["old", "new"]
    assert realized[0].gain == 50


def test_specific_lot_requires_explicit_selection():
    ledger = LotLedger("specific")
    ledger.buy(TaxLot("lot-a", "ABC", date(2024, 1, 1), 2, 10))
    with pytest.raises(ValueError, match="lot_ids"):
        ledger.sell("ABC", 1, 12)


def test_scenario_applies_price_and_currency_shocks():
    values = apply_scenario(
        {"ABC": 10},
        {"ABC": 100},
        {"ABC": "USD"},
        ScenarioShock({"ABC": -0.1}, {"USD": 0.05}),
    )
    assert values["ABC"] == 945.0
