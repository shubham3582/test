"""The scheduler engine — exactly-once firing via an Aerospike CAS lease.

Per tick, for each due schedule the engine tries to claim the current occurrence
by writing the schedule's state record with a generation-CAS (expected
generation). Only one replica wins; losers get a conflict and skip — so an
occurrence is fired once even with many replicas. The claim and the trigger
outbox row are written in one transaction; the winner relays the outbox to the
schedule's topic. Triggers carry ``(schedule, occurrence)`` so any crash-recovery
re-publish is idempotent downstream.
"""

from __future__ import annotations

import signal
import threading
import time
from typing import Optional

import structlog

from phronexus.config import SchedulerSettings
from phronexus.errors import GenerationConflict
from phronexus.scheduler.models import ScheduleSpec, TriggerEvent
from phronexus.statemachine.io import MemoryOutputPublisher, OutputPublisher
from phronexus.statemachine.models import OutputEvent

log = structlog.get_logger(__name__)


class Scheduler:
    def __init__(self, px, output: Optional[OutputPublisher] = None,
                 settings: Optional[SchedulerSettings] = None):
        self._px = px
        self._store = px.store
        self._out = output or MemoryOutputPublisher()
        self._cfg = settings or px.settings.scheduler

    @property
    def output(self) -> OutputPublisher:
        return self._out

    # --- schedule management --------------------------------------------

    def upsert_schedule(self, spec: ScheduleSpec) -> None:
        self._store.put(self._cfg.schedules_set, spec.name, {"spec": spec.model_dump(mode="json")})

    def remove_schedule(self, name: str) -> None:
        self._store.remove(self._cfg.schedules_set, name)

    def list_schedules(self) -> list[ScheduleSpec]:
        out = []
        for _key, rec in self._store.scan(self._cfg.schedules_set):
            out.append(ScheduleSpec.model_validate(rec.bins["spec"]))
        return out

    def get_state(self, name: str) -> dict:
        rec = self._store.get(self._cfg.state_set, name)
        return dict(rec.bins) if rec else {}

    # --- firing ---------------------------------------------------------

    def tick(self, now: Optional[float] = None) -> list[str]:
        """Fire all due schedules (claiming each occurrence once). Returns names fired."""
        now = time.time() if now is None else now
        fired: list[str] = []
        for spec in self.list_schedules():
            occ = spec.current_occurrence(now)
            if occ is None:
                continue
            state = self._store.get(self._cfg.state_set, spec.name)
            last = state.bins.get("last", 0) if state else 0
            if occ <= last:
                continue  # this occurrence already fired
            trigger = TriggerEvent(schedule=spec.name, occurrence=occ, fired_at=now, payload=spec.payload)
            try:
                with self._store.transaction() as txn:
                    self._store.put(
                        self._cfg.state_set, spec.name,
                        {"last": occ, "fired_at": now, "count": (state.bins.get("count", 0) + 1 if state else 1)},
                        expected_generation=(state.generation if state else 0), txn=txn,
                    )
                    self._store.put(
                        self._cfg.outbox_set, f"{spec.name}:{occ}",
                        {"ev": trigger.to_dict(), "topic": spec.topic}, txn=txn,
                    )
            except GenerationConflict:
                continue  # another replica claimed this occurrence
            self._px.telemetry.incr("phronexus.scheduler.fired", schedule=spec.name)
            fired.append(spec.name)
        self.drain()
        return fired

    def drain(self, max_events: int = 1000) -> int:
        """Relay claimed triggers to their topics. Safe to call repeatedly."""
        published = 0
        for key, rec in list(self._store.scan(self._cfg.outbox_set)):
            ev = rec.bins["ev"]
            try:
                self._out.publish(OutputEvent(
                    topic=rec.bins["topic"], type="ScheduledTrigger", key=ev["schedule"],
                    payload=ev, ts=ev["fired_at"], cause_event_id=f"{ev['schedule']}:{ev['occurrence']}",
                ))
            except Exception:  # noqa: BLE001 - leave for retry
                log.warning("scheduler.publish_failed", key=key)
                continue
            self._store.remove(self._cfg.outbox_set, key)
            published += 1
            if published >= max_events:
                break
        return published

    # --- loop -----------------------------------------------------------

    def run(self, *, max_ticks: Optional[int] = None) -> None:  # pragma: no cover - loop
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        ticks = 0
        while not stop.is_set() and (max_ticks is None or ticks < max_ticks):
            try:
                self.tick()
            except Exception:  # noqa: BLE001 - never kill the loop
                log.exception("scheduler.tick_failed")
            ticks += 1
            stop.wait(self._cfg.poll_seconds)
        self.drain()
