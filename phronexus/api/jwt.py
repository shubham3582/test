"""Minimal HS256 JWT (stdlib only — no external dependency).

Enough to issue and verify signed session tokens for the UI login flow. For a
production IdP integration (Microsoft Entra / Azure AD), tokens are verified via
the provider's JWKS instead — see phronexus.api.providers.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any


class JWTError(Exception):
    pass


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def encode(payload: dict[str, Any], secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    segments = [
        _b64url(json.dumps(header, separators=(",", ":")).encode()),
        _b64url(json.dumps(payload, separators=(",", ":")).encode()),
    ]
    signing_input = ".".join(segments).encode()
    sig = hmac.new(secret.encode(), signing_input, hashlib.sha256).digest()
    segments.append(_b64url(sig))
    return ".".join(segments)


def decode(token: str, secret: str) -> dict[str, Any]:
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
    except ValueError as exc:
        raise JWTError("malformed token") from exc
    signing_input = f"{header_b64}.{payload_b64}".encode()
    expected = hmac.new(secret.encode(), signing_input, hashlib.sha256).digest()
    if not hmac.compare_digest(expected, _b64url_decode(sig_b64)):
        raise JWTError("bad signature")
    payload = json.loads(_b64url_decode(payload_b64))
    if "exp" in payload and time.time() > payload["exp"]:
        raise JWTError("token expired")
    return payload
