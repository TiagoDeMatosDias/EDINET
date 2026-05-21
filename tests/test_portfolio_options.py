"""Tests for src/portfolio/option_pricing.py."""

import math
import pytest
from src.portfolio.option_pricing import (
    black_scholes,
    binomial_tree,
    option_greeks,
    implied_volatility,
    get_option_price,
)


class TestBlackScholes:
    def test_atm_call(self):
        price = black_scholes("call", S=100, K=100, T=1.0, r=0.05, sigma=0.20)
        # ATM 1yr call with 5% rf, 20% vol ≈ 10.45
        assert 9.5 < price < 11.5, f"Unexpected price: {price}"

    def test_atm_put(self):
        price = black_scholes("put", S=100, K=100, T=1.0, r=0.05, sigma=0.20)
        assert 5.5 < price < 7.5, f"Unexpected price: {price}"

    def test_deep_itm_call(self):
        price = black_scholes("call", S=200, K=100, T=1.0, r=0.05, sigma=0.20)
        # deep ITM → price ≈ S - PV(K) = 200 - 95.12 ≈ 104.88
        assert 100 < price < 110

    def test_deep_otm_put(self):
        price = black_scholes("put", S=200, K=100, T=1.0, r=0.05, sigma=0.20)
        assert price < 1.0

    def test_expired_option(self):
        price_call = black_scholes("call", S=100, K=90, T=0, r=0.05, sigma=0.20)
        assert price_call == 10.0  # intrinsic value
        price_put = black_scholes("put", S=100, K=110, T=0, r=0.05, sigma=0.20)
        assert price_put == 10.0

    def test_put_call_parity(self):
        S, K, T, r, sigma = 100, 100, 1.0, 0.05, 0.20
        c = black_scholes("call", S, K, T, r, sigma)
        p = black_scholes("put", S, K, T, r, sigma)
        # c + PV(K) = p + S
        lhs = c + K * math.exp(-r * T)
        rhs = p + S
        assert abs(lhs - rhs) < 0.01, f"Parity violated: {lhs} != {rhs}"

    def test_case_insensitive(self):
        c1 = black_scholes("CALL", 100, 100, 1, 0.05, 0.20)
        c2 = black_scholes("call", 100, 100, 1, 0.05, 0.20)
        assert c1 == c2


class TestBinomialTree:
    def test_vs_black_scholes_atm_call(self):
        S, K, T, r, sigma = 100, 100, 1.0, 0.05, 0.20
        bs_price = black_scholes("call", S, K, T, r, sigma)
        bt_price = binomial_tree("call", S, K, T, r, sigma, steps=200)
        # Binomial should converge to BS for European options (no early exercise)
        assert abs(bs_price - bt_price) < 0.10, f"Binom={bt_price}, BS={bs_price}"

    def test_american_put_early_exercise(self):
        """Deep ITM American put with dividend should be > European."""
        # No dividend → no early exercise advantage for put, but verify convergence
        S, K, T, r, sigma = 50, 100, 1.0, 0.05, 0.30
        bt = binomial_tree("put", S, K, T, r, sigma, steps=200)
        bs = black_scholes("put", S, K, T, r, sigma)
        # American put ≥ European put
        assert bt >= bs, f"American put ({bt}) < European ({bs})"

    def test_expired_at_binomial(self):
        assert binomial_tree("call", 100, 100, 0, 0.05, 0.20) == 0
        assert binomial_tree("put", 100, 100, 0, 0.05, 0.20) == 0
        call_itm = binomial_tree("call", 100, 80, 0, 0.05, 0.20)
        assert call_itm == 20.0

    def test_monotonic_in_steps(self):
        """Option price should stabilise as steps increase."""
        prices = [binomial_tree("call", 100, 100, 1, 0.05, 0.20, steps=s)
                  for s in [10, 50, 100, 200]]
        # Check that the range narrows as steps go up
        diffs = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
        assert diffs[-1] < diffs[0]  # convergence


class TestGreeks:
    def test_delta_range(self):
        g = option_greeks("call", 100, 100, 1, 0.05, 0.20)
        assert 0.5 < g["delta"] < 0.7
        g_put = option_greeks("put", 100, 100, 1, 0.05, 0.20)
        assert -0.5 < g_put["delta"] < -0.3

    def test_gamma_positive(self):
        g = option_greeks("call", 100, 100, 1, 0.05, 0.20)
        assert g["gamma"] > 0

    def test_theta_negative_for_long(self):
        g = option_greeks("call", 100, 100, 1, 0.05, 0.20)
        assert g["theta"] < 0  # long option loses value as time passes

    def test_vega_positive(self):
        g = option_greeks("call", 100, 100, 1, 0.05, 0.20)
        assert g["vega"] > 0

    def test_exact_put_call_delta_relationship(self):
        """Put-call delta parity: delta_call - delta_put = 1."""
        g_call = option_greeks("call", 100, 100, 1, 0.05, 0.20)
        g_put = option_greeks("put", 100, 100, 1, 0.05, 0.20)
        assert abs((g_call["delta"] - g_put["delta"]) - 1.0) < 0.001


class TestImpliedVolatility:
    def test_recovery(self):
        sigma_true = 0.20
        price = black_scholes("call", 100, 100, 1, 0.05, sigma_true)
        sigma_est = implied_volatility("call", price, 100, 100, 1, 0.05)
        assert sigma_est is not None
        assert abs(sigma_est - sigma_true) < 0.02

    def test_expired_returns_none(self):
        assert implied_volatility("call", 5.0, 100, 100, 0, 0.05) is None


class TestGetOptionPrice:
    def test_american_uses_binomial(self):
        """get_option_price with american=True returns binomial price."""
        price = get_option_price("put", 100, 100, 1, 0.05, 0.20, american=True)
        bt_price = binomial_tree("put", 100, 100, 1, 0.05, 0.20)
        assert price == bt_price

    def test_european_uses_black_scholes(self):
        price = get_option_price("call", 100, 100, 1, 0.05, 0.20, american=False)
        bs_price = black_scholes("call", 100, 100, 1, 0.05, 0.20)
        assert price == bs_price
