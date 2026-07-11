"""The autonomous face: ``python -m phronexus.statemachine.runner``.

Consumes domain events from Kafka, runs one transactional transition per event,
relays the outbox to Kafka, and dead-letters rejected/poison events. Partition
input by entity key so all events for one entity are processed in order by a
single consumer. Shuts down gracefully on SIGTERM/SIGINT (drains, then closes —
which commits Kafka offsets).
"""

from __future__ import annotations

import signal
import threading

import structlog

from phronexus.config import Settings
from phronexus.core import Phronexus
from phronexus.statemachine.io import KafkaInputSource, KafkaOutputPublisher
from phronexus.statemachine.machine import StateMachine

log = structlog.get_logger("phronexus.statemachine")


def run(px: Phronexus, source, machine: StateMachine, *, batch_size: int = 100,
        max_batches: int | None = None) -> dict[str, int]:
    """Drive the consume->transition->relay loop. Returns a status tally.

    Rejected events are dead-lettered (if a DLQ topic is configured).
    """
    tally = {"applied": 0, "duplicate": 0, "rejected": 0, "dropped": 0, "dead_lettered": 0}
    batches = 0
    while max_batches is None or batches < max_batches:
        events = source.poll(batch_size)
        if not events:
            break
        for ev in events:
            result = machine.process(ev)
            tally[result.status] = tally.get(result.status, 0) + 1
            if result.status == "rejected" and machine.dead_letter(ev, result):
                tally["dead_lettered"] += 1
        machine.drain_outbox()  # sweep any outbox rows left by failed publishes
        # Commit offsets only after the batch is processed + relayed (manual
        # commit closes the loss window; replay is safe because the path is
        # idempotent). Sources without a commit() (in-memory) are a no-op.
        commit = getattr(source, "commit", None)
        if commit is not None:
            commit()
        batches += 1
    return tally


def main() -> None:  # pragma: no cover - process entrypoint
    settings = Settings()
    px = Phronexus(settings)
    sm_cfg = settings.statemachine
    msg_journal = (px.message_journal()
                   if settings.journal.enabled and settings.journal.journal_messages else None)
    source = KafkaInputSource(settings.kafka, sm_cfg.input_topics, sm_cfg.consumer_group,
                              journal=msg_journal)
    machine = px.state_machine(output=KafkaOutputPublisher(settings.kafka))

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    log.info("statemachine.start", topics=sm_cfg.input_topics)
    try:
        while not stop.is_set():
            tally = run(px, source, machine, max_batches=1)
            if sum(tally.values()) == 0:
                stop.wait(0.5)  # idle backoff when no events
    finally:
        machine.drain_outbox()   # flush any pending outputs before exit
        source.close()           # commits Kafka offsets
        machine.output.close()
        px.close()
        log.info("statemachine.stopped")


if __name__ == "__main__":  # pragma: no cover
    main()
