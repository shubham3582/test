"""Change-feed sources for the retention worker."""

from __future__ import annotations

import abc
import json

from phronexus.config import KafkaSettings
from phronexus.events.base import CommitEvent
from phronexus.events.memory import MemorySink


class EventSource(abc.ABC):
    @abc.abstractmethod
    def poll(self, max_events: int) -> list[CommitEvent]:
        """Return up to ``max_events`` events; empty list when drained."""

    def close(self) -> None:  # pragma: no cover - trivial default
        pass


class MemoryEventSource(EventSource):
    """Drains a :class:`MemorySink` — pairs with an in-process Phronexus."""

    def __init__(self, sink: MemorySink):
        self._sink = sink

    def poll(self, max_events: int) -> list[CommitEvent]:
        out = self._sink.events[:max_events]
        del self._sink.events[:max_events]
        return out


class KafkaEventSource(EventSource):  # pragma: no cover - needs a broker
    """Consumes commit events from Kafka/Redpanda topics."""

    def __init__(self, cfg: KafkaSettings, entities: list[str], group_id: str = "phronexus-retention"):
        from phronexus.kafka_client import make_consumer

        # Manual commit: offsets advance only after rows are flushed to Iceberg,
        # so a crash replays the batch instead of dropping it (append is
        # idempotent by txn, so replay is safe).
        self._consumer = make_consumer(cfg, {  # TLS/mTLS + SASL + MSK IAM
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self._consumer.subscribe([f"{cfg.topic_prefix}.{e}" for e in entities])

    def poll(self, max_events: int) -> list[CommitEvent]:
        out: list[CommitEvent] = []
        for _ in range(max_events):
            msg = self._consumer.poll(0.5)
            if msg is None:
                break
            if msg.error():
                continue
            data = json.loads(msg.value())
            out.append(CommitEvent(**data))
        return out

    def commit(self) -> None:
        """Commit offsets — call only after the batch is flushed to Iceberg.

        An empty poll cycle stores no offsets, so librdkafka raises
        ``_NO_OFFSET``; that just means "nothing new to commit" and is benign.
        """
        from confluent_kafka import KafkaError, KafkaException

        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError._NO_OFFSET:
                raise

    def close(self) -> None:
        self._consumer.close()
