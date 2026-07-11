"""Liveness/readiness. Unauthenticated so orchestrators can probe freely."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from phronexus.api.deps import get_px
from phronexus.core import Phronexus

router = APIRouter(tags=["ops"])


@router.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@router.get("/readyz")
def readyz(px: Phronexus = Depends(get_px)):
    # Verify actual backend connectivity, not just process liveness.
    body = {
        "status": "ready",
        "backend": px.settings.backend,
        "contract_cache_age_s": round(px.registry.cache_age_seconds, 1),
        "store": "ok",
    }
    try:
        px.store.get(px.settings.aerospike.contracts_set, "__ping__")
    except Exception as exc:  # noqa: BLE001
        body.update(status="not_ready", store=f"error: {exc}")
        return JSONResponse(status_code=503, content=body)
    return body
