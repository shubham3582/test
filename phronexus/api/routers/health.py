"""Liveness/readiness. Unauthenticated so orchestrators can probe freely."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from phronexus.api.deps import get_px
from phronexus.core import Phronexus

router = APIRouter(tags=["ops"])


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@router.get("/readyz")
def readyz(px: Phronexus = Depends(get_px)) -> dict:
    # Ready once the contract cache has loaded at least once.
    return {
        "status": "ready",
        "backend": px.settings.backend,
        "contract_cache_age_s": round(px.registry.cache_age_seconds, 1),
    }
