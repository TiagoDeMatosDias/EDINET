"""Administrator control of the running server: its Cloudflare tunnel, and shutting it down."""

from __future__ import annotations

import logging
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict

from src.auth.dependencies import require_admin
from src.auth.models import AuthenticatedUser
from src.auth.service import AuthService
from src.web_app.shutdown import stop_server
from src.web_app.tunnel import TunnelManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin/server", tags=["admin-server"])

Admin = Annotated[AuthenticatedUser, Depends(require_admin)]


class ShutdownRequest(BaseModel):
    """A JSON body is required, so another site cannot stop a server that runs without sign-in."""

    model_config = ConfigDict(extra="forbid")

    confirm: Literal[True]


def _audit(request: Request, user: AuthenticatedUser, event: str) -> None:
    service = getattr(request.app.state, "auth_service", None)
    if isinstance(service, AuthService):
        service.store.audit(
            event,
            user.user_id,
            remote_addr=request.client.host if request.client else None,
        )


def _tunnel(request: Request) -> TunnelManager:
    tunnel = getattr(request.app.state, "tunnel", None)
    if not isinstance(tunnel, TunnelManager):
        raise HTTPException(status_code=503, detail="The tunnel is unavailable")
    return tunnel


@router.get("/tunnel")
def tunnel_status(request: Request, _user: Admin) -> dict[str, Any]:
    """Whether the Cloudflare tunnel is open, and at which address.

    It is turned on and off with the ``tunnel.enabled`` setting.
    """
    return _tunnel(request).status()


@router.post("/tunnel/restart")
def restart_tunnel(request: Request, user: Admin) -> dict[str, Any]:
    """Start the tunnel again: after a failure, or for a new temporary address."""
    tunnel = _tunnel(request)
    tunnel.apply()
    _audit(request, user, "tunnel_restarted")
    return tunnel.status()


@router.post("/shutdown", status_code=status.HTTP_202_ACCEPTED)
def shut_down_server(_payload: ShutdownRequest, request: Request, user: Admin) -> dict[str, str]:
    """Stop the server. It can only be started again on the machine it runs on."""
    logger.warning("Server shutdown requested by %s", user.username)
    _audit(request, user, "server_shutdown")
    stop_server()
    return {"status": "shutting_down"}
