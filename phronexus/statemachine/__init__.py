"""Phronexus as a transactional, event-driven state machine.

The autonomous face: consume a domain event, load the entity's current state,
evaluate a metadata-driven transition, and atomically persist the new state,
enqueue output events, and record a dedup marker — all in one store
transaction. A relay then publishes the outbox to Kafka at-least-once; combined
with idempotent writes (doc_id + generation CAS) and input dedup, the pipeline
is effectively-once.

    consume ─▶ [ store new state + outbox + dedup marker ]atomic ─▶ relay ─▶ emit

The same processor also backs a DishtaYantra node adapter (see ``node``), so the
one engine has two faces: standalone service and embedded DAG node.
"""

from phronexus.statemachine.hooks import ProcessingHook, TransitionContext
from phronexus.statemachine.io import (
    HttpOutputPublisher,
    MemoryOutputPublisher,
    NullOutputPublisher,
    RoutingOutputPublisher,
    build_output_publisher,
)
from phronexus.statemachine.machine import StateMachine, build_state_machine
from phronexus.statemachine.models import InputEvent, OutputEvent, ProcessResult

__all__ = [
    "StateMachine",
    "build_state_machine",
    "InputEvent",
    "OutputEvent",
    "ProcessResult",
    "ProcessingHook",
    "TransitionContext",
    "HttpOutputPublisher",
    "MemoryOutputPublisher",
    "NullOutputPublisher",
    "RoutingOutputPublisher",
    "build_output_publisher",
]
