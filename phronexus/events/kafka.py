"""Kafka / Redpanda change-feed sink (optional dependency).

Topic is ``{topic_prefix}.{entity}``; the message key is the doc id so all
versions of a document land on the same partition and stay ordered.
"""

from __future__ import annotations

import json

from phronexus.config import KafkaSettings
from phronexus.errors import ConfigError
from phronexus.events.base import CommitEvent, EventSink

try:  # pragma: no cover - exercised only with the driver installed
    from confluent_kafka import Producer
except ImportError:  # pragma: no cover
    Producer = None


class KafkaSink(EventSink):  # pragma: no cover - needs a broker
    def __init__(self, cfg: KafkaSettings):
        if Producer is None:
            raise ConfigError(
                "kafka.enabled requires confluent-kafka: "
                "pip install 'phronexus-core[kafka]'"
            )
        self.cfg = cfg
        self._producer = Producer(
            {"bootstrap.servers": cfg.bootstrap_servers, "client.id": cfg.client_id}
        )

    def emit(self, event: CommitEvent) -> None:
        topic = f"{self.cfg.topic_prefix}.{event.entity}"
        self._producer.produce(
            topic,
            key=event.doc_id.encode(),
            value=json.dumps(event.to_dict()).encode(),
        )
        self._producer.poll(0)

    def close(self) -> None:
        self._producer.flush(5)
