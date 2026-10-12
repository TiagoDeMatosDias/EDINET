"""Cloudflare API calls that create and manage the server's own tunnel.

The server creates a named tunnel once, stores its token in ``app.db`` (the
``tunnel.credentials`` setting), and reuses it on every start so the address
stays the same. A reset deletes the tunnel and its stored credentials so a
fresh one is created.

Every call needs the ``tunnel.api_token`` setting: an API token with the
Cloudflare Tunnel (Edit) and DNS (Edit) account permissions.
"""

from __future__ import annotations

import logging
from typing import Any

import requests  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

_API = "https://api.cloudflare.com/client/v4"
_TIMEOUT = (10, 30)


class TunnelApiError(RuntimeError):
    """The Cloudflare API refused a request; the message is shown to the administrator."""


def _headers(api_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}


def _request(method: str, api_token: str, path: str, **kwargs: Any) -> dict[str, Any]:
    """One Cloudflare API call; raise ``TunnelApiError`` on any failure."""
    try:
        response = requests.request(method, _API + path, headers=_headers(api_token), timeout=_TIMEOUT, **kwargs)
    except requests.RequestException as exc:
        raise TunnelApiError(f"Cloudflare could not be reached ({type(exc).__name__}).") from exc
    try:
        payload = response.json()
    except ValueError:
        raise TunnelApiError(f"Cloudflare returned an unreadable response (HTTP {response.status_code}).") from None
    if not payload.get("success"):
        errors = "; ".join(str(item.get("message", item)) for item in payload.get("errors") or [])
        raise TunnelApiError(f"Cloudflare refused the request: {errors or f'HTTP {response.status_code}'}")
    return payload


def account_id(api_token: str) -> str:
    """The account the API token belongs to."""
    payload = _request("GET", api_token, "/accounts")
    accounts = payload.get("result") or []
    if not accounts:
        raise TunnelApiError("The API token does not belong to any Cloudflare account.")
    return str(accounts[0]["id"])


def _zone(api_token: str, hostname: str) -> str:
    """The zone (registered domain) on Cloudflare that owns ``hostname``."""
    payload = _request("GET", api_token, "/zones")
    zones = [str(zone["name"]) for zone in payload.get("result") or [] if zone.get("name")]
    for zone in sorted(zones, key=len, reverse=True):
        if hostname == zone or hostname.endswith("." + zone):
            return zone
    raise TunnelApiError(f"The domain of {hostname} is not on Cloudflare; add it first.")


def create_tunnel(api_token: str, name: str, hostname: str, origin: str) -> tuple[str, str]:
    """Create a tunnel that publishes ``origin`` at ``hostname``; return (token, tunnel_id)."""
    account = account_id(api_token)
    payload = _request(
        "POST", api_token, f"/accounts/{account}/cfd_tunnel",
        json={"name": name, "config_src": "cloudflare"},
    )
    result = payload.get("result") or {}
    tunnel_id = str(result.get("id") or "")
    token = str(result.get("token") or "")
    if not tunnel_id or not token:
        raise TunnelApiError("Cloudflare created the tunnel but did not return its token.")
    # Point the hostname at this server, with a catch-all so unmatched requests 404.
    _request(
        "PUT", api_token, f"/accounts/{account}/cfd_tunnel/{tunnel_id}/configurations",
        json={"config": {"ingress": [
            {"hostname": hostname, "service": origin},
            {"service": "http_status:404"},
        ]}},
    )
    # Point the hostname's DNS at the tunnel.
    zone = _zone(api_token, hostname)
    _request(
        "POST", api_token, f"/zones/{zone}/dns_records",
        json={"type": "CNAME", "proxied": True, "name": hostname, "content": f"{tunnel_id}.cfargotunnel.com"},
    )
    return token, tunnel_id


def delete_tunnel(api_token: str, tunnel_id: str) -> None:
    """Delete a tunnel the server created; a failure is logged, not raised."""
    if not tunnel_id:
        return
    try:
        account = account_id(api_token)
        _request("DELETE", api_token, f"/accounts/{account}/cfd_tunnel/{tunnel_id}")
    except TunnelApiError as exc:
        logger.warning("Could not delete the old tunnel: %s", exc)
