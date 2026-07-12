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
from typing import Optional

import structlog

from phronexus.config import StateMachineSettings
from phronexus.errors import (
    DocumentAlreadyExists,
    GenerationConflict,
    TransitionRejected,
    ValidationError,
)
from phronexus.statemachine.emit import build_emit_payload
from phronexus.statemachine.guard import safe_eval
from phronexus.statemachine.hooks import TransitionContext
from phronexus.statemachine.io import MemoryOutputPublisher, OutputPublisher
from phronexus.statemachine.models import InputEvent, OutputEvent, ProcessResult

log = structlog.get_logger(__name__)


class StateMachine:
    def __init__(self, px, output: Optional[OutputPublisher] = None,
                 settings: Optional[StateMachineSettings] = None,
                 hooks: Optional[list] = None, journal=None):
        self._px = px
        self._store = px.store
        self._registry = px.registry
        self._manifest = px.manifest
        self._out = output or MemoryOutputPublisher()
        self._cfg = settings or px.settings.statemachine
        self._hooks = list(hooks or [])
        self._journal = journal  # optional RequestJournal — records req/resp as msgpack

    @property
    def output(self) -> OutputPublisher:
        return self._out

    # --- processing -----------------------------------------------------

    def process(self, event: InputEvent) -> ProcessResult:
        result = self._process(event)
        if self._journal is not None:
            self._record(event, result)
        return result

    def _record(self, event: InputEvent, result: ProcessResult) -> None:
        """Persist the request + response as msgpack (best-effort — never blocks)."""
        from dataclasses import asdict

        try:
            self._journal.record(
                event.event_id, request=asdict(event), response=asdict(result),
                entity=event.entity, type=event.event_type, status=result.status,
                ts=event.ts or time.time(),
            )
        except Exception:  # noqa: BLE001 - journaling must not fail the pipeline
            log.warning("statemachine.journal_failed", event_id=event.event_id)

    def _process(self, event: InputEvent) -> ProcessResult:
        # A concurrent writer on the same document makes the manifest CAS raise
        # GenerationConflict at commit. Re-process from a fresh read (the state
        # may have moved — the event might now be a duplicate, a different
        # transition, or a reject) instead of crashing the runner. On exhaustion
        # return a reject, which the runner dead-letters.
        retries = getattr(self._manifest, "_write_max_retries", 3)
        for attempt in range(retries + 1):
            try:
                return self._process_once(event)
            except GenerationConflict:
                self._px.telemetry.incr("phronexus.sm.conflicts", entity=event.entity)
                if attempt >= retries:
                    return ProcessResult(
                        status="rejected",
                        reason="write conflict: concurrent update, retries exhausted",
                    )

    def _process_once(self, event: InputEvent) -> ProcessResult:
        with self._px.telemetry.span("statemachine.process", entity=event.entity):
            tc = self._registry.active_transition(event.entity)

            # 1) Dedup — a redelivered event whose marker is already committed.
            if self._store.get(self._cfg.dedup_set, event.event_id) is not None:
                self._px.telemetry.incr("phronexus.sm.duplicates", entity=event.entity)
                return ProcessResult(status="duplicate", reason="event already processed")

            # 2) HOOK on_event — custom code after consume, before the transition.
            #    A hook returning None drops the event (ack + skip, not retried).
            for h in self._hooks:
                event = h.on_event(event)
                if event is None:
                    self._px.telemetry.incr("phronexus.sm.dropped", entity=event.entity if event else "?")
                    return ProcessResult(status="dropped", reason="dropped by hook")

            # 2.5) INBOUND schema validation — check the arriving message against
            #      its ingress JSON Schema before it can touch state. A failure is
            #      rejected here (the runner dead-letters it), so a malformed
            #      inbound message never drives a transition. Opt-in per event type.
            irep = self._px.validator.validate_inbound(event.entity, event.event_type, event.payload)
            if irep.warnings:
                log.warning("statemachine.inbound_schema.warnings", warnings=irep.warnings)
            if not irep.ok:
                self._px.telemetry.incr("phronexus.sm.rejected", entity=event.entity)
                return ProcessResult(
                    status="rejected",
                    reason="validation (inbound schema): " + "; ".join(irep.errors),
                )

            # 3) Load current state and resolve the transition.
            current = self._manifest.read(event.entity, event.key)
            cur_state = current.get(tc.state_field) if current else None
            tr = tc.match(event.event_type, cur_state)
            if tr is None:
                self._px.telemetry.incr("phronexus.sm.rejected", entity=event.entity)
                return ProcessResult(
                    status="rejected", from_state=cur_state,
                    reason=f"no transition for event={event.event_type!r} state={cur_state!r}",
                )

            # 4) Build the candidate document.
            new_doc = {**(current or {}), **event.payload, tc.state_field: tr.to}

            # 5) HOOK on_transition — enrich/validate before commit; may reject.
            ctx = TransitionContext(
                event=event, contract=tc, transition=tr, current=current, new_doc=new_doc,
            )
            try:
                for h in self._hooks:
                    h.on_transition(ctx)
            except TransitionRejected as exc:
                self._px.telemetry.incr("phronexus.sm.rejected", entity=event.entity)
                return ProcessResult(
                    status="rejected", from_state=cur_state, to_state=tr.to, reason=str(exc),
                )
            new_doc = ctx.new_doc

            # 6) Evaluate the guard on the (possibly enriched) candidate.
            if tr.guard and not safe_eval(tr.guard, new_doc):
                self._px.telemetry.incr("phronexus.sm.rejected", entity=event.entity)
                return ProcessResult(
                    status="rejected", from_state=cur_state, to_state=tr.to,
                    reason=f"guard failed: {tr.guard}",
                )

            # 4) Build output events — each payload is SHAPED from the candidate
            #    document per the emit spec (specific fields / rename / transform;
            #    default is the whole doc) — then validated against their stream
            #    JSON Schema at produce time (so malformed events are never
            #    published). Failures reject the transition — nothing is committed.
            now = time.time()
            outs = [
                OutputEvent(
                    topic=em.topic, type=em.type or tr.to, key=event.key,
                    payload=build_emit_payload(em, new_doc), ts=now,
                    cause_event_id=event.event_id,
                )
                for em in tr.emit
            ]
            ev_errors: list[str] = []
            for oe in outs:
                rep = self._px.validator.validate_event(event.entity, oe.type, oe.payload)
                if rep.warnings:
                    log.warning("statemachine.event_schema.warnings", warnings=rep.warnings)
                ev_errors.extend(rep.errors)
            if ev_errors:
                self._px.telemetry.incr("phronexus.sm.rejected", entity=event.entity)
                return ProcessResult(
                    status="rejected", from_state=cur_state, to_state=tr.to,
                    reason="validation (event schema): " + "; ".join(ev_errors),
                )

            def _stage_outputs(txn):
                # Staged BEFORE the manifest commit point: a committed transition
                # always implies its output events are durable (never lost, even
                # on a non-transactional crash).
                for i, oe in enumerate(outs):
                    self._store.put(
                        self._cfg.outbox_set, f"{event.event_id}:{i}", oe.to_dict(), txn=txn
                    )

            try:
                with self._store.transaction(native=self._manifest.native_txn_for(event.entity)) as txn:
                    staged = self._manifest.stage_write(
                        event.entity, new_doc, txn, stage_extra=_stage_outputs
                    )
                    # Dedup marker AFTER the manifest: if a CE crash orphans it, a
                    # replay must NOT see a false "duplicate" and skip a write that
                    # never committed (that would be a lost update). Under native
                    # txn the whole block is atomic (dedup present iff committed),
                    # which is why the output relay gates on this marker.
                    self._store.put(
                        self._cfg.dedup_set, event.event_id,
                        {"doc_id": staged.doc_id, "ts": now},
                        ttl=self._cfg.dedup_ttl, txn=txn,
                    )
            except DocumentAlreadyExists:
                return ProcessResult(status="rejected", reason="insert_only violation")
            except ValidationError as exc:
                self._px.telemetry.incr("phronexus.sm.rejected", entity=event.entity)
                return ProcessResult(
                    status="rejected", from_state=cur_state, to_state=tr.to,
                    reason=f"validation: {exc}",
                )

            # 7) Post-commit side effects + relay the outbox.
            self._manifest.post_write(staged, new_doc)
            self._px.telemetry.incr("phronexus.sm.applied", entity=event.entity)
            result = ProcessResult(
                status="applied", doc_id=staged.doc_id, from_state=cur_state,
                to_state=tr.to, emitted=[o.topic for o in outs],
                emitted_events=[o.to_dict() for o in outs],
            )
            # HOOK on_committed — post-commit effects, around the outbox relay.
            for h in self._hooks:
                h.on_committed(event, result)
            # Inline relay keeps it simple; disable it to let a standalone relay
            # process own the drain (insulates this path from slow brokers).
            if self._cfg.inline_relay:
                self.drain_outbox()
            log.info(
                "statemachine.transition", entity=event.entity, doc_id=staged.doc_id,
                **{"from": cur_state}, to=tr.to, emitted=[o.topic for o in outs],
            )
            return result

    # --- outbox relay ---------------------------------------------------

    def drain_outbox(self, max_events: int = 1000) -> int:
        """Publish and remove queued outbox events. Safe to call repeatedly.

        Only outputs whose transition actually committed are relayed: the dedup
        marker is written in the same transaction as the state + outputs, so its
        presence proves the transition committed. This stops a phantom output
        from an uncommitted (crashed) transition — the outputs are staged before
        the manifest, so without this gate a non-transactional crash could leave
        orphan outputs in the outbox.
        """
        published = 0
        for key, rec in list(self._store.scan(self._cfg.outbox_set)):
            # DLQ rows (dead-lettered rejects) always relay — a reject is a real
            # fact even though nothing committed, so they carry no dedup marker.
            if not key.startswith("dlq:"):
                event_id = key.rsplit(":", 1)[0]
                if self._store.get(self._cfg.dedup_set, event_id) is None:
                    continue  # transition did not commit -> do not emit a phantom
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

    # --- dead-letter ----------------------------------------------------

    def dead_letter(self, event: InputEvent, result: ProcessResult) -> bool:
        """Durably route a rejected/poison event to the DLQ topic (if configured).

        The DLQ event is staged into the transactional outbox FIRST, then relayed.
        If the publish fails it stays in the outbox for :meth:`drain_outbox` to
        retry — so a rejected event is never silently dropped, even though the
        runner commits the input offset regardless of publish success.
        """
        topic = self._cfg.dlq_topic
        if not topic:
            return False
        oe = OutputEvent(
            topic=topic, type="DeadLetter", key=event.key,
            payload={
                "entity": event.entity, "event_type": event.event_type,
                "event_id": event.event_id, "payload": event.payload,
                "status": result.status, "reason": result.reason,
            },
            ts=time.time(), cause_event_id=event.event_id,
        )
        key = f"dlq:{event.event_id}"
        self._store.put(self._cfg.outbox_set, key, oe.to_dict())  # durable first
        self._px.telemetry.incr("phronexus.sm.dead_lettered", entity=event.entity)
        try:
            self._out.publish(oe)
            self._store.remove(self._cfg.outbox_set, key)
        except Exception:  # noqa: BLE001 - keep it durable for retry
            log.warning("statemachine.dlq_publish_deferred", event_id=event.event_id)
        return True


def build_state_machine(px, output: Optional[OutputPublisher] = None,
                        hooks: Optional[list] = None) -> StateMachine:
    # Fail fast: the saga's exactly-once guarantee requires atomic multi-record
    # transactions. Refuse to run on a backend that can't provide them unless the
    # operator explicitly opts into best-effort (at-least-once) mode.
    scfg = px.settings.statemachine
    if scfg.require_atomic and not px.store.supports_atomic_txn():
        from phronexus.errors import ConfigError

        raise ConfigError(
            "state machine requires atomic multi-record transactions for "
            "exactly-once processing, but the store does not provide them "
            "(Aerospike: set aerospike.use_native_txn=true on an 8.0+ cluster). "
            "Set statemachine.require_atomic=false to run in best-effort "
            "(at-least-once) mode."
        )
    journal = None
    jcfg = px.settings.journal
    if jcfg.enabled and jcfg.journal_requests:
        journal = px.request_journal()
    return StateMachine(px, output=output, hooks=hooks, journal=journal)
