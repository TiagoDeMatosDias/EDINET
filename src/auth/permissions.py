"""Centralized role-to-permission and ownership enforcement."""

from __future__ import annotations

from enum import Enum, auto
from typing import Callable

from .models import AuthenticatedUser


class Permission(Enum):
    """Atomic permissions granted by role assignment."""

    READ_SHARED = auto()       # company, filing, taxonomy, standardized market data
    MUTATE_PIPELINE = auto()   # pipeline definitions, jobs, provider refresh, config
    READ_RESEARCH = auto()     # own research state
    MUTATE_RESEARCH = auto()    # own research state
    READ_PORTFOLIO = auto()    # own portfolio
    MUTATE_PORTFOLIO = auto()  # own portfolio
    ADMIN_USERS = auto()       # user/role/registration management


ROLE_PERMISSIONS: dict[str, frozenset[Permission]] = {
    "admin": frozenset(Permission),
    "operator": frozenset({
        Permission.READ_SHARED,
        Permission.MUTATE_PIPELINE,
        Permission.READ_RESEARCH,
        Permission.MUTATE_RESEARCH,
        Permission.READ_PORTFOLIO,
        Permission.MUTATE_PORTFOLIO,
    }),
    "member": frozenset({
        Permission.READ_SHARED,
        Permission.READ_RESEARCH,
        Permission.MUTATE_RESEARCH,
        Permission.READ_PORTFOLIO,
        Permission.MUTATE_PORTFOLIO,
    }),
}


def has_permission(user: AuthenticatedUser, permission: Permission) -> bool:
    """Check whether an authenticated user holds a specific permission."""
    if user.status != "active":
        return False
    allowed = ROLE_PERMISSIONS.get(user.role, frozenset())
    return permission in allowed


def require_permission(permission: Permission) -> Callable[[AuthenticatedUser], bool]:
    """Return a predicate that can be used as a route dependency check."""
    def _check(user: AuthenticatedUser) -> bool:
        return has_permission(user, permission)
    return _check


def is_owner(resource_owner_id: str, user: AuthenticatedUser) -> bool:
    """Return whether the authenticated user owns a given resource."""
    return resource_owner_id == user.user_id


def require_owner(resource_owner_id: str, user: AuthenticatedUser) -> None:
    """Raise PermissionError when the user does not own the resource."""
    if not is_owner(resource_owner_id, user):
        raise PermissionError("Resource is owned by another account")


def require_role(user: AuthenticatedUser, roles: frozenset[str]) -> None:
    """Raise PermissionError when the user's role is not in the allowed set."""
    if user.role not in roles:
        raise PermissionError("Insufficient role for this operation")
