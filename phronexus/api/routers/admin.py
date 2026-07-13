"""Admin operations: date-scoped store resync between the hot and cold tiers.

Permission-gated behind ``resync:control`` (config-driven RBAC). The run itself
is synchronous here (fine for bounded windows); the checkpointed job state is
fleet-visible via :meth:`GovernanceService.resync_status` and can be
paused/cancelled through ``/admin/resync/control``.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from phronexus.admin.resync import DIRECTIONS, ResyncJob
from phronexus.api.auth import Principal
from phronexus.api.deps import get_px, require_permission
from phronexus.core import Phronexus
from phronexus.errors import PhronexusError

router = APIRouter(tags=["admin"])


class ResyncRequest(BaseModel):
    entity: str
    direction: str                     # cold-to-hot | hot-to-cold
    date_from: Optional[int] = None    # YYYYMMDD inclusive
    date_to: Optional[int] = None      # YYYYMMDD inclusive
    overwrite: bool = False            # cold-to-hot: clobber a live hot doc
    dry_run: bool = False


class ResyncControlRequest(BaseModel):
    run_key: str                       # "entity:direction:window"
    action: str                        # pause | resume | cancel | reset


@router.post("/admin/resync")
def resync(body: ResyncRequest,
           principal: Principal = Depends(require_permission("resync:control")),
           px: Phronexus = Depends(get_px)) -> dict:
    if body.direction not in DIRECTIONS:
        raise PhronexusError(f"unknown direction {body.direction!r}; expected one of {DIRECTIONS}")
    px.governance.log.append("resync.run", principal.name,
                             target=f"{body.entity}:{body.direction}")
    count = ResyncJob(px).run(
        body.entity, direction=body.direction, date_from=body.date_from,
        date_to=body.date_to, overwrite=body.overwrite, dry_run=body.dry_run)
    return {"entity": body.entity, "direction": body.direction, "from": body.date_from,
            "to": body.date_to, "resynced": count, "dry_run": body.dry_run}


@router.get("/admin/resync/status")
def resync_status(run_key: Optional[str] = None,
                  principal: Principal = Depends(require_permission("resync:control")),
                  px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.resync_status(run_key)


@router.post("/admin/resync/control")
def resync_control(body: ResyncControlRequest,
                   principal: Principal = Depends(require_permission("resync:control")),
                   px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.control_resync(principal.name, body.run_key, body.action)
