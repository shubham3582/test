"""Governance control-plane endpoints: change requests, approval, history,
rollback, and compatibility explanations. Every action is permission-gated
(config-driven RBAC) and recorded in the immutable governance log."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from phronexus.api.auth import Principal
from phronexus.api.deps import get_px, require_permission
from phronexus.contracts.loader import parse_contract
from phronexus.contracts.models import StorageContract
from phronexus.core import Phronexus

router = APIRouter(tags=["governance"])


class DraftRequest(BaseModel):
    contract: dict[str, Any]
    environment: Optional[str] = None


class ReasonRequest(BaseModel):
    reason: str = ""


class RollbackRequest(BaseModel):
    identity: str
    reason: str = ""


# --- change-request workflow ---------------------------------------------

@router.post("/governance/changes")
def draft(body: DraftRequest, principal: Principal = Depends(require_permission("contract:draft")),
          px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.draft(principal.name, body.contract, environment=body.environment)


@router.post("/governance/changes/{change_id}/submit")
def submit(change_id: str, principal: Principal = Depends(require_permission("contract:submit")),
           px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.submit(principal.name, change_id)


@router.post("/governance/changes/{change_id}/approve")
def approve(change_id: str, principal: Principal = Depends(require_permission("contract:approve")),
            px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.approve(principal.name, change_id)


@router.post("/governance/changes/{change_id}/reject")
def reject(change_id: str, body: ReasonRequest,
           principal: Principal = Depends(require_permission("contract:approve")),
           px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.reject(principal.name, change_id, body.reason)


@router.post("/governance/changes/{change_id}/withdraw")
def withdraw(change_id: str, principal: Principal = Depends(require_permission("contract:draft")),
             px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.withdraw(principal.name, change_id)


@router.post("/governance/changes/{change_id}/publish")
def publish(change_id: str, force: bool = False,
            principal: Principal = Depends(require_permission("contract:publish")),
            px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.publish(principal.name, change_id, force=force)


@router.get("/governance/changes")
def list_changes(principal: Principal = Depends(require_permission("governance:read")),
                 px: Phronexus = Depends(get_px)) -> dict:
    return {"changes": px.governance.list_changes()}


@router.get("/governance/changes/{change_id}")
def get_change(change_id: str, principal: Principal = Depends(require_permission("governance:read")),
               px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.get_change(change_id)


# --- rollback (governed) --------------------------------------------------

@router.post("/governance/rollback")
def rollback(body: RollbackRequest,
             principal: Principal = Depends(require_permission("contract:rollback")),
             px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.rollback(principal.name, body.identity, reason=body.reason)


# --- compatibility + history ---------------------------------------------

@router.post("/governance/compat")
def compat(body: DraftRequest,
           principal: Principal = Depends(require_permission("governance:read")),
           px: Phronexus = Depends(get_px)) -> dict:
    c = parse_contract(body.contract)
    if isinstance(c, StorageContract):
        return px.registry.compat_report(c)
    return {"compatible": True, "first_version": False, "changes": [], "kind": c.kind.value}


@router.get("/governance/diff")
def diff(a: str, b: str,
         principal: Principal = Depends(require_permission("governance:read")),
         px: Phronexus = Depends(get_px)) -> dict:
    return px.registry.diff(a, b)


@router.get("/governance/log")
def log(action: Optional[str] = None, target: Optional[str] = None,
        principal: Principal = Depends(require_permission("governance:read")),
        px: Phronexus = Depends(get_px)) -> dict:
    entries = px.governance.log.entries(action=action, target=target)
    return {"entries": entries, "verify": px.governance.log.verify()}


# --- fleet / COB / backfill (Wave 3) -------------------------------------

@router.get("/governance/fleet")
def fleet(principal: Principal = Depends(require_permission("governance:read")),
          px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.fleet()


class CobRequest(BaseModel):
    cob: Optional[int] = None       # int YYYYMMDD; omit with advance=true to +1 day
    advance: bool = False
    environment: Optional[str] = None


@router.get("/governance/cob")
def get_cob(environment: Optional[str] = None,
            principal: Principal = Depends(require_permission("governance:read")),
            px: Phronexus = Depends(get_px)) -> dict:
    return {"environment": environment or px.settings.governance.environment,
            "cob": px.governance.get_cob(environment)}


@router.post("/governance/cob")
def set_cob(body: CobRequest,
            principal: Principal = Depends(require_permission("cob:set")),
            px: Phronexus = Depends(get_px)) -> dict:
    if body.advance:
        return px.governance.advance_cob(principal.name, body.environment)
    if body.cob is None:
        from phronexus.governance import GovernanceError
        raise GovernanceError("provide 'cob' (YYYYMMDD) or set advance=true")
    return px.governance.set_cob(principal.name, body.cob, body.environment)


@router.get("/governance/backfill")
def backfill_status(entity: Optional[str] = None,
                    principal: Principal = Depends(require_permission("governance:read")),
                    px: Phronexus = Depends(get_px)) -> dict:
    return {"backfills": px.governance.backfill_status(entity)}


@router.post("/governance/backfill/{entity}/{action}")
def control_backfill(entity: str, action: str,
                     principal: Principal = Depends(require_permission("backfill:control")),
                     px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.control_backfill(principal.name, entity, action)


# --- promotion + evidence (Wave 5) ---------------------------------------

@router.post("/governance/promote/{identity}/bundle")
def export_bundle(identity: str,
                  principal: Principal = Depends(require_permission("contract:promote")),
                  px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.export_bundle(principal.name, identity)


class ImportRequest(BaseModel):
    bundle: dict[str, Any]
    signature: str


@router.post("/governance/import")
def import_bundle(body: ImportRequest,
                  principal: Principal = Depends(require_permission("contract:promote")),
                  px: Phronexus = Depends(get_px)) -> dict:
    return px.governance.import_bundle(principal.name, body.bundle, body.signature)


class EvidenceRequest(BaseModel):
    entity: Optional[str] = None
    doc_id: Optional[str] = None
    start_ts: Optional[float] = None
    end_ts: Optional[float] = None
    sources: Optional[list[str]] = None


@router.post("/governance/evidence")
def evidence(body: EvidenceRequest,
             principal: Principal = Depends(require_permission("evidence:export")),
             px: Phronexus = Depends(get_px)) -> dict:
    from phronexus.governance.evidence import build_evidence

    return build_evidence(px, principal.name, entity=body.entity, doc_id=body.doc_id,
                          start_ts=body.start_ts, end_ts=body.end_ts, sources=body.sources)
