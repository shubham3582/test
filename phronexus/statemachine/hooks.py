"""Processing hooks — custom code between consuming an event and committing.

Hooks run for **every** face of the state machine (Kafka runner, sync REST, DAG
node) because they live inside ``StateMachine.process()``. Extension points:

    on_event(event)      after consume, before the transition resolves.
                         Enrich / validate / transform. Return a modified event,
                         or None to DROP it (ack + skip, not retried).

    on_transition(ctx)   transition matched, BEFORE the atomic commit. Mutate
                         ctx.new_doc (enrichment) or raise TransitionRejected to
                         reject (not committed; retryable).

    on_committed(event, result)   after the commit, around the outbox relay.
                         Side effects, audit, metrics.

Rule of thumb: any I/O (an HTTP lookup, a reference-data read) belongs in
``on_event`` / ``on_transition`` — *before* the transaction — so the store
transaction stays tight. ``on_committed`` is for post-commit effects only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from phronexus.contracts.models import Transition, TransitionContract
from phronexus.statemachine.models import InputEvent, ProcessResult


@dataclass
class TransitionContext:
    event: InputEvent
    contract: TransitionContract
    transition: Transition
    current: Optional[dict[str, Any]]   # current state document (None on creation)
    new_doc: dict[str, Any]             # candidate document — mutate this to enrich


class ProcessingHook:
    """Base hook. Override the points you need; defaults are no-ops."""

    def on_event(self, event: InputEvent) -> Optional[InputEvent]:
        return event

    def on_transition(self, ctx: TransitionContext) -> None:
        return None

    def on_committed(self, event: InputEvent, result: ProcessResult) -> None:
        return None
