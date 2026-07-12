"""Login / session endpoints for the UI."""

from __future__ import annotations

import base64
import hashlib
import secrets
import time

from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from phronexus.api import jwt
from phronexus.api.auth import Principal
from phronexus.api.deps import _forbidden, _unauthorized, require_principal

router = APIRouter(tags=["auth"])


def _b64url(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    token: str
    token_type: str = "bearer"
    expires_in: int
    principal: str
    roles: list[str]
    permissions: list[str]


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
    perms = sorted(request.app.state.authenticator.permissions_for_roles(roles))
    return LoginResponse(
        token=token, expires_in=cfg.jwt_ttl_seconds, principal=body.username,
        roles=roles, permissions=perms,
    )


@router.get("/auth/me")
def me(request: Request, principal: Principal = Depends(require_principal)) -> dict:
    authn = request.app.state.authenticator
    return {
        "principal": principal.name, "scheme": principal.scheme,
        "roles": list(principal.roles),
        "permissions": sorted(authn.permissions(principal)),
    }


# --- OIDC (Microsoft Entra / Azure AD) authorization-code + PKCE ---------

@router.get("/auth/oidc/login")
def oidc_login(request: Request):
    cfg = request.app.state.px.settings.api.auth
    if cfg.provider != "oidc":
        raise _forbidden("OIDC is not enabled")
    provider = request.app.state.auth_provider
    verifier = secrets.token_urlsafe(48)
    nonce = secrets.token_urlsafe(16)
    challenge = _b64url(hashlib.sha256(verifier.encode()).digest())
    # Stateless PKCE: carry the verifier + nonce in a short-lived signed state,
    # so no server-side session store is needed (replica-safe).
    state = jwt.encode(
        {"v": verifier, "n": nonce, "exp": int(time.time()) + 600}, cfg.jwt_secret
    )
    url = provider.authorization_url(state=state, code_challenge=challenge, nonce=nonce)
    return RedirectResponse(url=url, status_code=307)


@router.get("/auth/oidc/callback")
def oidc_callback(request: Request, code: str, state: str):
    cfg = request.app.state.px.settings.api.auth
    if cfg.provider != "oidc":
        raise _forbidden("OIDC is not enabled")
    provider = request.app.state.auth_provider
    try:
        st = jwt.decode(state, cfg.jwt_secret)
    except jwt.JWTError as exc:
        raise _unauthorized("invalid or expired login state") from exc
    claims = provider.exchange_and_verify(code, st["v"], nonce=st["n"])
    roles = provider.roles_from_claims(claims)
    sub = claims.get("preferred_username") or claims.get("email") or claims.get("sub")
    exp = int(time.time()) + cfg.jwt_ttl_seconds
    token = jwt.encode({"sub": sub, "roles": roles, "exp": exp}, cfg.jwt_secret)
    # Hand the session token to the SPA via the URL fragment (not sent to servers).
    return RedirectResponse(url=f"/ui/#token={token}", status_code=307)
