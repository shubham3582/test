"""FastAPI dependencies: shared Phronexus instance + auth guards."""

from __future__ import annotations

from fastapi import Depends, Request

from phronexus.api.auth import Authenticator, AuthError, Principal
from phronexus.core import Phronexus
from phronexus.errors import PhronexusError


def get_px(request: Request) -> Phronexus:
    return request.app.state.px


def get_authenticator(request: Request) -> Authenticator:
    return request.app.state.authenticator


def get_state_machine(request: Request):
    return request.app.state.state_machine


def require_principal(
    request: Request,
    authn: Authenticator = Depends(get_authenticator),
) -> Principal:
    try:
        principal = authn.authenticate(request)
    except AuthError as exc:
        raise _unauthorized(exc.detail) from exc
    request.state.principal = principal
    return principal


def require_admin(
    principal: Principal = Depends(require_principal),
    authn: Authenticator = Depends(get_authenticator),
) -> Principal:
    if not authn.is_admin(principal):
        raise _forbidden("principal is not permitted to administer contracts")
    return principal


def require_permission(perm: str):
    """Dependency factory: require ``perm`` (config-driven RBAC). 403 otherwise.

    Usage: ``dependencies=[Depends(require_permission("contract:approve"))]`` or
    ``principal = Depends(require_permission("cob:set"))`` to also bind the caller.
    """

    def _dep(
        principal: Principal = Depends(require_principal),
        authn: Authenticator = Depends(get_authenticator),
    ) -> Principal:
        if not authn.has_permission(principal, perm):
            raise _forbidden(f"missing required permission: {perm}")
        return principal

    return _dep


class _AuthPhronexusError(PhronexusError):
    def __init__(self, code: str, status: int, detail: str):
        super().__init__(detail)
        self.code = code
        self.http_status = status


def _unauthorized(detail: str) -> _AuthPhronexusError:
    return _AuthPhronexusError("unauthorized", 401, detail)


def _forbidden(detail: str) -> _AuthPhronexusError:
    return _AuthPhronexusError("forbidden", 403, detail)
