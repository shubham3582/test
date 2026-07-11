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


class PhronexusQueryCalculator:
    """Read/enrichment calculator: join a DAG record with looked-up Phronexus data.

    Configure exactly one lookup:
      * ``key_from`` — a record field holding a doc id → direct ``get`` (or ``view``)
      * ``pattern``  — a named query pattern; params are bound from the record
      * ``where``    — inline predicates; ``${field}`` values bound from the record

    ``mode``: ``"enrich"`` merges the hits into the record under ``result_key``
    (default ``<entity>_hits``); ``"replace"`` returns the hits for the next node.
    Read-only — never touches the write path.
    """

    def __init__(
        self, px, entity: str, *, key_from: str | None = None, pattern: str | None = None,
        where: list[dict[str, Any]] | None = None, view: str | None = None,
        mode: str = "enrich", result_key: str | None = None,
    ):
        if sum(x is not None for x in (key_from, pattern, where)) != 1:
            raise ValueError("configure exactly one of key_from / pattern / where")
        self._px = px
        self._entity = entity
        self._key_from = key_from
        self._pattern = pattern
        self._where = where
        self._view = view
        self._mode = mode
        self._result_key = result_key or f"{entity}_hits"
        self._count = 0

    @staticmethod
    def _bind(value: Any, data: dict[str, Any]) -> Any:
        """Resolve a ``${field}`` placeholder against the incoming record."""
        if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            return data.get(value[2:-1])
        return value

    def calculate(self, data: dict[str, Any]) -> Any:
        self._count += 1
        if self._key_from is not None:
            doc_id = str(data.get(self._key_from))
            hit = (self._px.view(self._entity, self._view, doc_id) if self._view
                   else self._px.get(self._entity, doc_id))
            hits = [hit] if hit is not None else []
        elif self._pattern is not None:
            hits = self._px.query_pattern(self._entity, self._pattern, **data)
        else:
            where = [{**p, "value": self._bind(p.get("value"), data)} for p in (self._where or [])]
            hits = self._px.query({"entity": self._entity, "where": where})
        if self._mode == "replace":
            return hits
        return {**data, self._result_key: hits}

    def details(self) -> dict[str, Any]:
        return {"calculator": "phronexus_query", "entity": self._entity,
                "mode": self._mode, "processed": self._count}
