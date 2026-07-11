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


class OidcProvider(AuthProvider):
    """Microsoft Entra / Azure AD (OIDC) authorization-code + PKCE flow.

    Direct username/password is intentionally unsupported (browser flow only).
    Discovery, token exchange and JWKS verification use ``pyjwt[crypto]`` +
    ``httpx`` (the ``[oidc]`` extra), imported lazily so they stay optional.
    """

    name = "oidc"

    def __init__(self, oidc):
        if not oidc.issuer or not oidc.client_id:
            raise ConfigError("oidc provider requires issuer and client_id")
        self._cfg = oidc
        self._disc: Optional[dict] = None
        self._http_client = None

    # --- HTTP (lazy; overridable in tests) ------------------------------

    @property
    def _http(self):
        if self._http_client is None:
            import httpx  # optional dep

            self._http_client = httpx.Client(timeout=10.0)
        return self._http_client

    def _discovery(self) -> dict:
        if self._disc is None:
            url = self._cfg.issuer.rstrip("/") + "/.well-known/openid-configuration"
            self._disc = self._http.get(url).json()
        return self._disc

    # --- flow -----------------------------------------------------------

    def authorization_url(self, *, state: str, code_challenge: str, nonce: str) -> str:
        from urllib.parse import urlencode

        d = self._discovery()
        q = {
            "client_id": self._cfg.client_id,
            "response_type": "code",
            "redirect_uri": self._cfg.redirect_uri,
            "scope": " ".join(self._cfg.scopes),
            "state": state,
            "nonce": nonce,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            "response_mode": "query",
        }
        return d["authorization_endpoint"] + "?" + urlencode(q)

    def exchange_and_verify(self, code: str, code_verifier: str, *, nonce: Optional[str] = None) -> dict:
        import json as _json

        import jwt as pyjwt  # PyJWT (optional dep)

        d = self._discovery()
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self._cfg.redirect_uri,
            "client_id": self._cfg.client_id,
            "code_verifier": code_verifier,
        }
        if self._cfg.client_secret:
            data["client_secret"] = self._cfg.client_secret
        tok = self._http.post(d["token_endpoint"], data=data).json()
        id_token = tok["id_token"]

        header = pyjwt.get_unverified_header(id_token)
        jwks = self._http.get(d["jwks_uri"]).json()
        jwk = next(k for k in jwks["keys"] if k.get("kid") == header.get("kid"))
        key = pyjwt.algorithms.RSAAlgorithm.from_jwk(_json.dumps(jwk))
        claims = pyjwt.decode(
            id_token, key, algorithms=["RS256"],
            audience=self._cfg.client_id, issuer=d.get("issuer", self._cfg.issuer),
        )
        if nonce is not None and claims.get("nonce") != nonce:
            raise ConfigError("OIDC nonce mismatch")
        return claims

    def roles_from_claims(self, claims: dict) -> list[str]:
        r = claims.get(self._cfg.role_claim, [])
        return [r] if isinstance(r, str) else list(r)

    def authenticate(self, username: str, password: str) -> Optional[list[str]]:
        raise ConfigError(
            "OIDC/Entra uses the browser authorization-code flow, not password login."
        )


def build_provider(cfg: AuthSettings) -> AuthProvider:
    if cfg.provider == "oidc":
        return OidcProvider(cfg.oidc)
    return LocalPasswordProvider(cfg.users)


def _constant_eq(a: str, b: str) -> bool:
    import hmac

    return hmac.compare_digest(a, b)
