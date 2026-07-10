"""Input sources and output publishers for the state machine.

In-memory implementations back tests/demos; Kafka implementations (optional
dependency) back production. Output is the transactional-outbox drain target,
so publishing is at-least-once and the consumer of these topics must dedup.
"""

from __future__ import annotations

import abc
import json
from typing import Optional

from phronexus.config import KafkaSettings
from phronexus.errors import ConfigError
from phronexus.statemachine.models import InputEvent, OutputEvent


# --- input ---------------------------------------------------------------

class InputSource(abc.ABC):
    @abc.abstractmethod
    def poll(self, max_events: int) -> list[InputEvent]: ...

    def close(self) -> None:  # pragma: no cover
        pass


class MemoryInputSource(InputSource):
    def __init__(self, events: Optional[list[InputEvent]] = None):
        self.events: list[InputEvent] = list(events or [])

    def offer(self, event: InputEvent) -> None:
        self.events.append(event)

    def poll(self, max_events: int) -> list[InputEvent]:
        out = self.events[:max_events]
        del self.events[:max_events]
        return out


class KafkaInputSource(InputSource):  # pragma: no cover - needs a broker
    def __init__(self, cfg: KafkaSettings, topics: list[str], group_id: str):
        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise ConfigError("KafkaInputSource requires confluent-kafka") from exc
        self._c = Consumer({
            "bootstrap.servers": cfg.bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        self._c.subscribe(topics)

    def poll(self, max_events: int) -> list[InputEvent]:
        out: list[InputEvent] = []
        for _ in range(max_events):
            msg = self._c.poll(0.5)
            if msg is None:
                break
            if msg.error():
                continue
            d = json.loads(msg.value())
            out.append(InputEvent(
                entity=d["entity"], event_type=d["event_type"], key=d["key"],
                payload=d.get("payload", {}), event_id=d["event_id"], ts=d.get("ts", 0.0),
            ))
        return out

    def close(self) -> None:
        self._c.close()


# --- output --------------------------------------------------------------

class OutputPublisher(abc.ABC):
    @abc.abstractmethod
    def publish(self, event: OutputEvent) -> None: ...

    def close(self) -> None:  # pragma: no cover
        pass


class MemoryOutputPublisher(OutputPublisher):
    def __init__(self) -> None:
        self.events: list[OutputEvent] = []

    def publish(self, event: OutputEvent) -> None:
        self.events.append(event)


class KafkaOutputPublisher(OutputPublisher):  # pragma: no cover - needs a broker
    def __init__(self, cfg: KafkaSettings):
        try:
            from confluent_kafka import Producer
        except ImportError as exc:
            raise ConfigError("KafkaOutputPublisher requires confluent-kafka") from exc
        self._p = Producer({"bootstrap.servers": cfg.bootstrap_servers, "client.id": cfg.client_id})

    def publish(self, event: OutputEvent) -> None:
        # topic may be a bare name or a "kafka://name" URI.
        topic = event.topic.split("://", 1)[-1]
        self._p.produce(topic, key=event.key.encode(), value=json.dumps(event.to_dict()).encode())
        self._p.poll(0)

    def close(self) -> None:
        self._p.flush(5)
