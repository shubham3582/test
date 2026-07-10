"""Change-feed emission: every committed document becomes an event.

Consumed later (Phase 4) by the Iceberg retention worker. The sink is pluggable
so the core emits the same event whether it lands in Kafka/Redpanda or an
in-memory buffer for tests.
"""

from phronexus.events.base import CommitEvent, EventSink, NullSink
from phronexus.events.memory import MemorySink

__all__ = ["CommitEvent", "EventSink", "NullSink", "MemorySink", "build_sink"]


def build_sink(settings) -> EventSink:
    if settings.kafka.enabled:
        from phronexus.events.kafka import KafkaSink  # lazy: optional dep

        return KafkaSink(settings.kafka)
    return MemorySink()
