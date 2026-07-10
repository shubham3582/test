"""The embedded face: a DishtaYantra-style calculator node.

The same :class:`StateMachine` engine that runs as an autonomous service also
plugs into a DishtaYantra DAG as a ``CalculationNode``. DishtaYantra calculators
implement ``calculate(data)`` / ``details()``; this adapter maps a record on the
DAG into an :class:`InputEvent`, runs one transactional transition, and returns
the result for the next node.

The exact base class / registration hook depends on DishtaYantra's SPI — swap
the ``Calculator`` base and decorators once the repo API is confirmed.
"""

from __future__ import annotations

import time
from typing import Any

from phronexus.statemachine.machine import StateMachine
from phronexus.statemachine.models import InputEvent


class PhronexusStateMachineNode:
    """Calculator adapter. Expects records shaped like::

        {"entity": "trade", "event_type": "TradeConfirmed",
         "key": "T-1", "event_id": "...", "payload": {...}}
    """

    def __init__(self, machine: StateMachine, entity: str | None = None):
        self._machine = machine
        self._entity = entity
        self._count = 0

    def calculate(self, data: dict[str, Any]) -> dict[str, Any]:
        event = InputEvent(
            entity=data.get("entity") or self._entity,
            event_type=data["event_type"],
            key=data["key"],
            payload=data.get("payload", {}),
            event_id=data.get("event_id") or f"{data['key']}:{time.time_ns()}",
            ts=data.get("ts", 0.0),
        )
        result = self._machine.process(event)
        self._count += 1
        return {
            "status": result.status,
            "doc_id": result.doc_id,
            "from": result.from_state,
            "to": result.to_state,
            "emitted": result.emitted,
            "reason": result.reason,
        }

    def details(self) -> dict[str, Any]:
        return {"calculator": "phronexus_state_machine", "entity": self._entity, "processed": self._count}
