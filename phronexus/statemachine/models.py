"""Input/output event models for the state machine."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Optional


@dataclass
class InputEvent:
    """A domain event consumed from Kafka (or any source).

    ``key`` identifies the entity instance (the document id / partition key), so
    all events for one entity are ordered on a single consumer. ``event_id`` must
    be unique and stable per event — it drives exactly-once dedup on redelivery.
    """

    entity: str
    event_type: str
    key: str
    payload: dict[str, Any]
    event_id: str
    ts: float = 0.0


@dataclass
class OutputEvent:
    topic: str
    type: str
    key: str
    payload: dict[str, Any]
    ts: float
    cause_event_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "type": self.type,
            "key": self.key,
            "payload": self.payload,
            "ts": self.ts,
            "cause_event_id": self.cause_event_id,
        }


@dataclass
class ProcessResult:
    status: Literal["applied", "duplicate", "rejected", "dropped"]
    doc_id: Optional[str] = None
    from_state: Optional[str] = None
    to_state: Optional[str] = None
    emitted: list[str] = field(default_factory=list)  # output topics
    reason: Optional[str] = None
    # Full outbound events (topic/type/key/payload) sent by this transition — the
    # "what we sent" half of the journaled request/response. Not surfaced in the
    # REST/node response; captured in the interaction journal.
    emitted_events: list[dict[str, Any]] = field(default_factory=list)
