"""Contract-admin endpoints (publish / refresh). Requires admin principal."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from phronexus.api.deps import get_px, require_admin
from phronexus.api.schemas import ContractResponse
from phronexus.contracts.loader import parse_contract
from phronexus.core import Phronexus

router = APIRouter(tags=["contracts"], dependencies=[Depends(require_admin)])


@router.post("/contracts", response_model=ContractResponse)
def publish(body: dict[str, Any], activate: bool = True, px: Phronexus = Depends(get_px)):
    contract = parse_contract(body)
    px.publish_contract(contract, activate=activate)
    return ContractResponse(published=contract.identity(), activated=activate)


@router.post("/contracts/refresh")
def refresh(px: Phronexus = Depends(get_px)) -> dict:
    px.refresh_contracts()
    return {"refreshed": True}
