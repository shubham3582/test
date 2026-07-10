"""Pluggable authentication.

Configured schemes are tried in order; the first that yields a principal wins.
Supported: ``none`` (open, dev only), ``api_key`` (header), ``bearer`` (opaque
token; swap for JWT verification in production), and ``mtls`` (client-cert CN
forwarded by a trusted TLS-terminating proxy, or read from the TLS transport).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from starlette.requests import HTTPConnection

from phronexus.config import AuthSettings


@dataclass(frozen=True)
class Principal:
    name: str
    scheme: str

    @property
    def is_anonymous(self) -> bool:
        return self.scheme == "none"


class AuthError(Exception):
    def __init__(self, detail: str):
        self.detail = detail


class Authenticator:
    def __init__(self, cfg: AuthSettings):
        self.cfg = cfg

    def authenticate(self, conn: HTTPConnection) -> Principal:
        for scheme in self.cfg.schemes:
            principal = self._try(scheme, conn)
            if principal is not None:
                return principal
        raise AuthError("authentication required")

    def _try(self, scheme: str, conn: HTTPConnection) -> Optional[Principal]:
        if scheme == "none":
            return Principal(name="anonymous", scheme="none")
        if scheme == "api_key":
            key = conn.headers.get(self.cfg.api_key_header)
            if key and key in self.cfg.api_keys:
                return Principal(name=self.cfg.api_keys[key], scheme="api_key")
            return None
        if scheme == "bearer":
            auth = conn.headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                token = auth[7:]
                if token in self.cfg.bearer_tokens:
                    return Principal(name=self.cfg.bearer_tokens[token], scheme="bearer")
            return None
        if scheme == "mtls":
            cn = self._client_cn(conn)
            if cn is None:
                return None
            allowed = self.cfg.mtls_allowed_cns
            if not allowed:
                return Principal(name=cn, scheme="mtls")
            if cn in allowed:
                return Principal(name=allowed[cn], scheme="mtls")
            return None
        return None

    def _client_cn(self, conn: HTTPConnection) -> Optional[str]:
        # Preferred: forwarded by a trusted proxy that terminated mTLS.
        cn = conn.headers.get(self.cfg.mtls_cn_header)
        if cn:
            return cn
        # Fallback: direct TLS termination — pull CN from the peer certificate.
        ssl_object = conn.scope.get("extensions", {}).get("tls", {})
        peercert = ssl_object.get("peercert") if isinstance(ssl_object, dict) else None
        if peercert:
            for rdn in peercert.get("subject", ()):  # pragma: no cover
                for attr, value in rdn:
                    if attr == "commonName":
                        return value
        return None

    def is_admin(self, principal: Principal) -> bool:
        admins = self.cfg.admin_principals
        return not admins or principal.name in admins
