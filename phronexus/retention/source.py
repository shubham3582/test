"""Change-feed sources for the retention worker."""

from __future__ import annotations

import abc
import json
from typing import Optional

from phronexus.config import KafkaSettings
from phronexus.errors import ConfigError
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
        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise ConfigError(
                "KafkaEventSource requires confluent-kafka: pip install 'phronexus-core[kafka]'"
            ) from exc
        conf = cfg.client_config()  # TLS/mTLS + SASL
        conf.update({
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        self._consumer = Consumer(conf)
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

    def close(self) -> None:
        self._consumer.close()
