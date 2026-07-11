"""Schedule management endpoints (admin)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from phronexus.api.deps import require_admin
from phronexus.scheduler.models import ScheduleSpec

router = APIRouter(tags=["scheduler"], dependencies=[Depends(require_admin)])


def _scheduler(request: Request):
    return request.app.state.scheduler


@router.get("/schedules")
def list_schedules(request: Request) -> dict:
    sch = _scheduler(request)
    specs = sch.list_schedules()
    return {"schedules": [
        {**s.model_dump(mode="json"), "state": sch.get_state(s.name)} for s in specs
    ]}


@router.put("/schedules")
def upsert_schedule(body: dict, request: Request) -> dict:
    spec = ScheduleSpec.model_validate(body)
    _scheduler(request).upsert_schedule(spec)
    return {"upserted": spec.name}


@router.delete("/schedules/{name}")
def delete_schedule(name: str, request: Request) -> dict:
    _scheduler(request).remove_schedule(name)
    return {"deleted": name}


@router.post("/schedules/tick")
def tick(request: Request) -> dict:
    """Fire due schedules now (manual trigger for testing)."""
    return {"fired": _scheduler(request).tick()}
