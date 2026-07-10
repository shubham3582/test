"""The autonomous face: ``python -m phronexus.statemachine.runner``.

Consumes domain events from Kafka, runs one transactional transition per event,
and relays the outbox to Kafka. Partition input by entity key so all events for
one entity are processed in order by a single consumer.
"""

from __future__ import annotations

import structlog

from phronexus.config import Settings
from phronexus.core import Phronexus
from phronexus.statemachine.io import KafkaInputSource, KafkaOutputPublisher
from phronexus.statemachine.machine import StateMachine

log = structlog.get_logger("phronexus.statemachine")


def run(px: Phronexus, source, machine: StateMachine, *, batch_size: int = 100,
        max_batches: int | None = None) -> dict[str, int]:
    """Drive the consume→transition→relay loop. Returns a status tally."""
    tally = {"applied": 0, "duplicate": 0, "rejected": 0, "dropped": 0}
    batches = 0
    while max_batches is None or batches < max_batches:
        events = source.poll(batch_size)
        if not events:
            break
        for ev in events:
            result = machine.process(ev)
            tally[result.status] = tally.get(result.status, 0) + 1
        machine.drain_outbox()  # sweep any outbox rows left by failed publishes
        batches += 1
    return tally


def main() -> None:  # pragma: no cover - process entrypoint
    settings = Settings()
    px = Phronexus(settings)
    sm_cfg = settings.statemachine
    source = KafkaInputSource(settings.kafka, sm_cfg.input_topics, sm_cfg.consumer_group)
    machine = StateMachine(px, output=KafkaOutputPublisher(settings.kafka))
    log.info("statemachine.start", topics=sm_cfg.input_topics)
    try:
        while True:
            run(px, source, machine, max_batches=1)
    except KeyboardInterrupt:
        pass
    finally:
        source.close()
        machine.output.close()
        px.close()


if __name__ == "__main__":  # pragma: no cover
    main()
