"""FastAPI dependencies for current-principal and permission resolution."""

from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status

from .models import AuthenticatedUser
from .permissions import Permission, has_permission


def current_user(request: Request) -> AuthenticatedUser:
    """Resolve the authenticated principal from request state.

    Returns 401 when no valid session or API token was presented.
    """
    user = getattr(request.state, "user", None)
    if not isinstance(user, AuthenticatedUser):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def require_permission(permission: Permission):
    """FastAPI dependency factory: require a specific permission."""

    def _dependency(user: AuthenticatedUser = Depends(current_user)) -> AuthenticatedUser:
        if not has_permission(user, permission):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions for this operation",
            )
        return user

    return _dependency


def require_admin(user: AuthenticatedUser = Depends(current_user)) -> AuthenticatedUser:
    """Dependency: the caller must be an active administrator."""
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Administrator permission required",
        )
    return user


def require_operator(user: AuthenticatedUser = Depends(current_user)) -> AuthenticatedUser:
    """Dependency: the caller must be an active administrator or operator."""
    if user.role not in {"admin", "operator"}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operator permission required",
        )
    return user
