"""Cryptographic helpers for promotion bundles and evidence hashes.

A promotion bundle is a contract version plus provenance, HMAC-signed so a target
environment can verify it was produced by a trusted control plane and has not
been altered in transit (air-gapped or across clusters)."""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any


def canonical(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def content_hash(obj: Any) -> str:
    return hashlib.sha256(canonical(obj).encode()).hexdigest()


def sign(secret: str, payload: Any) -> str:
    return hmac.new(secret.encode(), canonical(payload).encode(), hashlib.sha256).hexdigest()


def verify(secret: str, payload: Any, signature: str) -> bool:
    return hmac.compare_digest(sign(secret, payload), signature or "")
