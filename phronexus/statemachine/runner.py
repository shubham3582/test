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
        for ev in events:
            result = machine.process(ev)
            tally[result.status] = tally.get(result.status, 0) + 1
            if result.status == "rejected" and machine.dead_letter(ev, result):
                tally["dead_lettered"] += 1
        if events:
            machine.drain_outbox()  # sweep any outbox rows left by failed publishes
        # Commit offsets only after the batch is processed + relayed (manual
        # commit closes the loss window; replay is safe because the path is
        # idempotent). Sources without a commit() (in-memory) are a no-op.
        # Also commit when the poll consumed messages that all filtered out as
        # poison (no events, but the offset advanced), so a poison-only batch
        # can't redeliver forever across a restart/rebalance.
        commit = getattr(source, "commit", None)
        if commit is not None and (events or getattr(source, "uncommitted", False)):
            commit()
        if not events:
            break
        batches += 1
    return tally


def serve(px, source, machine, stop, *, initial_backoff: float = 1.0,
          max_backoff: float = 30.0, idle: float = 0.5) -> None:
    """Resilient consume loop until ``stop`` is set.

    A transient broker/store error backs off exponentially and retries instead of
    crashing the process (offsets are only committed inside :func:`run` after a
    batch is processed + relayed, so a mid-batch failure replays rather than
    loses). Mirrors the retention worker's recovery.
    """
    backoff = initial_backoff
    while not stop.is_set():
        try:
            tally = run(px, source, machine, max_batches=1)
            backoff = initial_backoff  # progress -> reset backoff
            if sum(tally.values()) == 0:
                stop.wait(idle)  # idle backoff when no events
        except Exception as exc:  # noqa: BLE001 - transient; retry with backoff
            log.warning("statemachine.iteration_failed", error=str(exc),
                        retry_in_s=round(backoff, 1))
            stop.wait(backoff)
            backoff = min(backoff * 2, max_backoff)


def _make_poison_handler(px, settings):  # pragma: no cover - needs a broker
    """Route structurally-undecodable inbound messages aside so a poison message
    never wedges a partition. Produces the RAW bytes to the DLQ topic (with the
    decode error and broker coordinates as headers) and counts them; if no DLQ
    topic is configured, falls back to log-and-drop."""
    dlq = settings.statemachine.dlq_topic
    if not dlq:
        return None  # KafkaInputSource uses its log-and-drop default
    from phronexus.kafka_client import make_producer

    producer = make_producer(settings.kafka)
    topic = dlq.split("://", 1)[-1]

    def handle(raw: bytes, meta: dict, exc: Exception) -> None:
        px.telemetry.incr("phronexus.sm.poison")
        headers = [("error", str(exc).encode()),
                   ("source_topic", str(meta.get("topic", "")).encode()),
                   ("source_partition", str(meta.get("partition", "")).encode()),
                   ("source_offset", str(meta.get("offset", "")).encode())]
        producer.produce(topic, value=raw, headers=headers)
        producer.poll(0)
        log.error("statemachine.poison_message", error=str(exc), dlq=topic, **meta)

    return handle


def main() -> None:  # pragma: no cover - process entrypoint
    settings = Settings()
    px = Phronexus(settings)
    sm_cfg = settings.statemachine
    msg_journal = (px.message_journal()
                   if settings.journal.enabled and settings.journal.journal_messages else None)
    source = KafkaInputSource(settings.kafka, sm_cfg.input_topics, sm_cfg.consumer_group,
                              journal=msg_journal, on_poison=_make_poison_handler(px, settings))
    machine = px.state_machine(output=KafkaOutputPublisher(settings.kafka))

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    log.info("statemachine.start", topics=sm_cfg.input_topics)
    try:
        serve(px, source, machine, stop)
    finally:
        machine.drain_outbox()   # flush any pending outputs before exit
        source.close()           # commits Kafka offsets
        machine.output.close()
        px.close()
        log.info("statemachine.stopped")


if __name__ == "__main__":  # pragma: no cover
    main()
