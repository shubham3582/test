"""Authentication providers for the login flow.

``LocalPasswordProvider`` verifies fixed username/password credentials from
config. ``OidcProvider`` is the seam for Microsoft Entra / Azure AD (or any OIDC
IdP) — wire it up by implementing the authorization-code exchange and JWKS
verification against your tenant. Both return the authenticated user's roles.
"""

from __future__ import annotations

import abc
import hashlib
from typing import Optional

from phronexus.config import AuthSettings
from phronexus.errors import ConfigError


class AuthProvider(abc.ABC):
    name: str = "provider"

    @abc.abstractmethod
    def authenticate(self, username: str, password: str) -> Optional[list[str]]:
        """Return the user's roles on success, or None on failure."""


class LocalPasswordProvider(AuthProvider):
    name = "local"

    def __init__(self, users: dict[str, dict]):
        self._users = users

    def authenticate(self, username: str, password: str) -> Optional[list[str]]:
        u = self._users.get(username)
        if not u:
            return None
        if "password" in u and _constant_eq(password, u["password"]):
            return list(u.get("roles", []))
        if "password_sha256" in u:
            digest = hashlib.sha256(password.encode()).hexdigest()
            if _constant_eq(digest, u["password_sha256"]):
                return list(u.get("roles", []))
        return None


class OidcProvider(AuthProvider):  # pragma: no cover - requires an IdP
    """Microsoft Entra / Azure AD (OIDC) seam.

    Direct username/password is intentionally not supported (Entra uses the
    authorization-code flow). Implement ``start_authorization``/``handle_callback``
    to redirect to the tenant, exchange the code, verify the id_token via JWKS,
    and map ``role_claim`` -> Phronexus roles.
    """

    name = "oidc"

    def __init__(self, oidc):
        if not oidc.issuer or not oidc.client_id:
            raise ConfigError("oidc provider requires issuer and client_id")
        self._cfg = oidc

    def authenticate(self, username: str, password: str) -> Optional[list[str]]:
        raise ConfigError(
            "OIDC/Entra uses the browser authorization-code flow, not password login. "
            "Implement start_authorization/handle_callback against your tenant."
        )


def build_provider(cfg: AuthSettings) -> AuthProvider:
    if cfg.provider == "oidc":
        return OidcProvider(cfg.oidc)
    return LocalPasswordProvider(cfg.users)


def _constant_eq(a: str, b: str) -> bool:
    import hmac

    return hmac.compare_digest(a, b)
