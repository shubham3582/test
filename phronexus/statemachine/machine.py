"""The transactional state-machine processor.

For each input event: load current state, evaluate the metadata-driven
transition, then atomically persist the new state document, enqueue the output
events, and record a dedup marker in a single store transaction. After commit, a
relay drains the outbox to the output publisher (at-least-once). Idempotent
writes (doc_id + generation CAS) plus input dedup make the pipeline
effectively-once end to end.
"""

from __future__ import annotations

import time
import uuid
from typing import Optional

import structlog

from phronexus.config import StateMachineSettings
from phronexus.errors import DocumentAlreadyExists
from phronexus.statemachine.guard import safe_eval
from phronexus.statemachine.io import MemoryOutputPublisher, OutputPublisher
from phronexus.statemachine.models import InputEvent, OutputEvent, ProcessResult

log = structlog.get_logger(__name__)


class StateMachine:
    def __init__(self, px, output: Optional[OutputPublisher] = None,
                 settings: Optional[StateMachineSettings] = None):
        self._px = px
        self._store = px.store
        self._registry = px.registry
        self._manifest = px.manifest
        self._out = output or MemoryOutputPublisher()
        self._cfg = settings or px.settings.statemachine

    @property
    def output(self) -> OutputPublisher:
        return self._out

    # --- processing -----------------------------------------------------

    def process(self, event: InputEvent) -> ProcessResult:
        with self._px.telemetry.span("statemachine.process", entity=event.entity):
            tc = self._registry.active_transition(event.entity)

            # 1) Dedup — a redelivered event whose marker is already committed.
            if self._store.get(self._cfg.dedup_set, event.event_id) is not None:
                self._px.telemetry.incr("phronexus.sm.duplicates", entity=event.entity)
                return ProcessResult(status="duplicate", reason="event already processed")

            # 2) Load current state and resolve the transition.
            current = self._manifest.read(event.entity, event.key)
            cur_state = current.get(tc.state_field) if current else None
            tr = tc.match(event.event_type, cur_state)
            if tr is None:
                self._px.telemetry.incr("phronexus.sm.rejected", entity=event.entity)
                return ProcessResult(
                    status="rejected", from_state=cur_state,
                    reason=f"no transition for event={event.event_type!r} state={cur_state!r}",
                )

            # 3) Build the candidate document and evaluate the guard.
            new_doc = {**(current or {}), **event.payload, tc.state_field: tr.to}
            if tr.guard and not safe_eval(tr.guard, new_doc):
                self._px.telemetry.incr("phronexus.sm.rejected", entity=event.entity)
                return ProcessResult(
                    status="rejected", from_state=cur_state, to_state=tr.to,
                    reason=f"guard failed: {tr.guard}",
                )

            # 4) Atomic: new state + outbox events + dedup marker in one txn.
            now = time.time()
            outs = [
                OutputEvent(
                    topic=em.topic, type=em.type or tr.to, key=event.key,
                    payload=new_doc, ts=now, cause_event_id=event.event_id,
                )
                for em in tr.emit
            ]
            try:
                with self._store.transaction() as txn:
                    staged = self._manifest.stage_write(event.entity, new_doc, txn)
                    for i, oe in enumerate(outs):
                        self._store.put(
                            self._cfg.outbox_set, f"{event.event_id}:{i}", oe.to_dict(), txn=txn
                        )
                    self._store.put(
                        self._cfg.dedup_set, event.event_id,
                        {"doc_id": staged.doc_id, "ts": now},
                        ttl=self._cfg.dedup_ttl, txn=txn,
                    )
            except DocumentAlreadyExists:
                return ProcessResult(status="rejected", reason="insert_only violation")

            # 5) Post-commit side effects + relay the outbox.
            self._manifest.post_write(staged, new_doc)
            self._px.telemetry.incr("phronexus.sm.applied", entity=event.entity)
            self.drain_outbox()
            log.info(
                "statemachine.transition", entity=event.entity, doc_id=staged.doc_id,
                **{"from": cur_state}, to=tr.to, emitted=[o.topic for o in outs],
            )
            return ProcessResult(
                status="applied", doc_id=staged.doc_id, from_state=cur_state,
                to_state=tr.to, emitted=[o.topic for o in outs],
            )

    # --- outbox relay ---------------------------------------------------

    def drain_outbox(self, max_events: int = 1000) -> int:
        """Publish and remove queued outbox events. Safe to call repeatedly."""
        published = 0
        for key, rec in list(self._store.scan(self._cfg.outbox_set)):
            b = rec.bins
            try:
                self._out.publish(OutputEvent(
                    topic=b["topic"], type=b["type"], key=b["key"],
                    payload=b["payload"], ts=b["ts"], cause_event_id=b["cause_event_id"],
                ))
            except Exception:  # noqa: BLE001 - leave in outbox for retry
                log.warning("statemachine.publish_failed", key=key)
                continue
            self._store.remove(self._cfg.outbox_set, key)
            published += 1
            if published >= max_events:
                break
        return published


def build_state_machine(px, output: Optional[OutputPublisher] = None) -> StateMachine:
    return StateMachine(px, output=output)
