"""Login / session endpoints for the UI."""

from __future__ import annotations

import time

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from phronexus.api import jwt
from phronexus.api.auth import Principal
from phronexus.api.deps import _unauthorized, require_principal

router = APIRouter(tags=["auth"])


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    token: str
    token_type: str = "bearer"
    expires_in: int
    principal: str
    roles: list[str]


@router.get("/auth/config")
def auth_config(request: Request) -> dict:
    """Tell the UI how to authenticate (so it can show password vs SSO)."""
    cfg = request.app.state.px.settings.api.auth
    return {"provider": cfg.provider, "oidc_enabled": cfg.oidc.enabled}


@router.post("/auth/login", response_model=LoginResponse)
def login(body: LoginRequest, request: Request) -> LoginResponse:
    cfg = request.app.state.px.settings.api.auth
    provider = request.app.state.auth_provider
    roles = provider.authenticate(body.username, body.password)
    if roles is None:
        raise _unauthorized("invalid username or password")
    exp = int(time.time()) + cfg.jwt_ttl_seconds
    token = jwt.encode({"sub": body.username, "roles": roles, "exp": exp}, cfg.jwt_secret)
    return LoginResponse(
        token=token, expires_in=cfg.jwt_ttl_seconds, principal=body.username, roles=roles
    )


@router.get("/auth/me")
def me(principal: Principal = Depends(require_principal)) -> dict:
    return {"principal": principal.name, "scheme": principal.scheme, "roles": list(principal.roles)}
