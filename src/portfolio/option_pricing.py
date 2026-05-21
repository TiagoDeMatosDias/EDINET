"""Option pricing models: Black-Scholes, CRR binomial tree, and Greeks.

Uses only numpy + scipy (already project dependencies).  All prices are
theoretical — the portfolio module values options based on these models
with configurable volatility (default 20%).
"""

from __future__ import annotations

import numpy as np
from scipy.stats import norm

# ---------------------------------------------------------------------------
# Black-Scholes (European)
# ---------------------------------------------------------------------------

def black_scholes(
    opt_type: str,
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
) -> float:
    """Price a European option using Black-Scholes.

    Args:
        opt_type: ``'call'`` or ``'put'`` (case-insensitive).
        S: Current underlying price.
        K: Strike price.
        T: Time to expiration in years.
        r: Risk-free rate (annualised, decimal).
        sigma: Implied volatility (annualised, decimal).

    Returns:
        Option premium.
    """
    if T <= 0:
        return max(0.0, S - K) if opt_type.lower() == "call" else max(0.0, K - S)

    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    if opt_type.lower() == "call":
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    else:
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


# ---------------------------------------------------------------------------
# Binomial tree (Cox-Ross-Rubinstein) for American options
# ---------------------------------------------------------------------------

def binomial_tree(
    opt_type: str,
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    steps: int = 100,
) -> float:
    """Price an American option using the CRR binomial tree.

    The tree allows early exercise at every node, correctly valuing
    American-style options (which most equity options are).

    Args:
        opt_type: ``'call'`` or ``'put'`` (case-insensitive).
        S, K, T, r, sigma: As for ``black_scholes``.
        steps: Number of tree steps (default 100; higher = more accurate).

    Returns:
        Option premium.
    """
    if T <= 0:
        return max(0.0, S - K) if opt_type.lower() == "call" else max(0.0, K - S)

    dt = T / steps
    u = np.exp(sigma * np.sqrt(dt))
    d = 1.0 / u
    p = (np.exp(r * dt) - d) / (u - d)
    q = 1.0 - p
    discount = np.exp(-r * dt)

    is_call = opt_type.lower() == "call"

    # Terminal payoffs
    prices = np.zeros(steps + 1)
    for i in range(steps + 1):
        spot = S * (u ** (steps - i)) * (d ** i)
        prices[i] = max(spot - K, 0) if is_call else max(K - spot, 0)

    # Backward induction
    for j in range(steps - 1, -1, -1):
        for i in range(j + 1):
            hold = discount * (p * prices[i] + q * prices[i + 1])
            spot = S * (u ** (j - i)) * (d ** i)
            exercise = max(spot - K, 0) if is_call else max(K - spot, 0)
            prices[i] = max(hold, exercise)

    return float(prices[0])


# ---------------------------------------------------------------------------
# Greeks (Black-Scholes)
# ---------------------------------------------------------------------------

def option_greeks(
    opt_type: str,
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
) -> dict[str, float]:
    """Compute Black-Scholes Greeks.

    Returns:
        dict with keys ``delta``, ``gamma``, ``theta``, ``vega``, ``rho``.
    """
    if T <= 0:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "rho": 0.0}

    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    is_call = opt_type.lower() == "call"
    sign = 1 if is_call else -1

    delta = norm.cdf(d1) if is_call else norm.cdf(d1) - 1
    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    vega = S * norm.pdf(d1) * np.sqrt(T) * 0.01  # per 1% vol change

    # Theta (per day)
    theta_term1 = -(S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T))
    theta_term2 = r * K * np.exp(-r * T)
    if is_call:
        theta = (theta_term1 - theta_term2 * norm.cdf(d2)) / 365
    else:
        theta = (theta_term1 + theta_term2 * norm.cdf(-d2)) / 365

    rho = sign * K * T * np.exp(-r * T) * norm.cdf(sign * d2) * 0.01  # per 1% rate change

    return {
        "delta": round(delta, 6),
        "gamma": round(gamma, 6),
        "theta": round(theta, 6),
        "vega": round(vega, 6),
        "rho": round(rho, 6),
    }


# ---------------------------------------------------------------------------
# Implied volatility
# ---------------------------------------------------------------------------

def implied_volatility(
    opt_type: str,
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    tol: float = 1e-6,
    max_iter: int = 100,
) -> float | None:
    """Compute implied volatility via Newton-Raphson.

    Returns None if the iteration fails to converge.
    """
    if T <= 0:
        return None

    sigma = 0.3  # initial guess
    for _ in range(max_iter):
        price = black_scholes(opt_type, S, K, T, r, sigma)
        diff = price - market_price

        if abs(diff) < tol:
            return sigma

        # Vega = dPrice/dSigma
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        vega = S * norm.pdf(d1) * np.sqrt(T)
        if vega < 1e-12:
            return None

        sigma = sigma - diff / vega
        if sigma <= 0:
            return None

    return None


# ---------------------------------------------------------------------------
# Unified pricing
# ---------------------------------------------------------------------------

def get_option_price(
    opt_type: str,
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float = 0.20,
    american: bool = True,
) -> float:
    """Price an option using the appropriate model.

    Args:
        american: If True, uses binomial tree (default); else Black-Scholes.

    Returns:
        Option premium.
    """
    if american:
        return binomial_tree(opt_type, S, K, T, r, sigma)
    return black_scholes(opt_type, S, K, T, r, sigma)
