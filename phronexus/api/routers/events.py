"""Synchronous state-machine endpoint — the REST twin of the Kafka input.

``POST /entities/{entity}/events`` submits a domain event, runs one
transactional transition, and returns **accept/reject in the HTTP response**:

    applied / duplicate -> 200   (state committed; output events fan out via the outbox)
    no valid transition -> 409   (conflict with the current state)
    guard failed        -> 422   (unprocessable)

Downstream events are still emitted asynchronously (outbox -> Kafka); only the
accept/reject decision is synchronous. The endpoint is OpenAPI-documented.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from phronexus.api.deps import get_state_machine, require_principal
from phronexus.api.schemas import EventRequest, EventResponse
from phronexus.statemachine.machine import StateMachine
from phronexus.statemachine.models import InputEvent

router = APIRouter(tags=["events"], dependencies=[Depends(require_principal)])


@router.post(
    "/entities/{entity}/events",
    response_model=EventResponse,
    responses={
        409: {"model": EventResponse, "description": "No valid transition for the current state"},
        422: {"model": EventResponse, "description": "A transition guard rejected the event"},
    },
)
def submit_event(
    entity: str,
    body: EventRequest,
    sm: StateMachine = Depends(get_state_machine),
) -> JSONResponse:
    event = InputEvent(
        entity=entity,
        event_type=body.event_type,
        key=body.key,
        payload=body.payload,
        event_id=body.event_id or f"{body.key}:{uuid.uuid4().hex}",
        ts=body.ts,
    )
    result = sm.process(event)
    payload = EventResponse(
        status=result.status,
        doc_id=result.doc_id,
        from_state=result.from_state,
        to_state=result.to_state,
        emitted=result.emitted,
        reason=result.reason,
    ).model_dump()

    if result.status in ("applied", "duplicate"):
        status_code = 200
    elif result.reason and result.reason.startswith("guard"):
        status_code = 422
    else:
        status_code = 409
    return JSONResponse(status_code=status_code, content=payload)
