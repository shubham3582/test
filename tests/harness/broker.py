"""A deterministic in-memory Kafka model for consumer chaos tests.

Models the semantics the framework depends on — partitioned logs, consumer
groups with independent committed offsets, at-least-once redelivery of
uncommitted messages, and rebalance (revoke/assign) — without a real broker.

It is intentionally tiny: values are opaque Python objects (no serialization),
partitioning is by key hash, and a "crash" is just discarding a consumer without
committing (a new consumer in the same group resumes from the last commit).
"""

from __future__ import annotations

from typing import Any, Optional


class Broker:
    def __init__(self, partitions: int = 1) -> None:
        self._partitions = partitions
        self._logs: dict[str, list[list[Any]]] = {}
        self._committed: dict[tuple[str, str, int], int] = {}

    def _log(self, topic: str, part: int) -> list[Any]:
        parts = self._logs.setdefault(topic, [[] for _ in range(self._partitions)])
        return parts[part]

    def produce(self, topic: str, value: Any, *, key: Optional[str] = None) -> None:
        part = (hash(key) % self._partitions) if key is not None else 0
        self._log(topic, part).append(value)

    def committed(self, group: str, topic: str, part: int) -> int:
        return self._committed.get((group, topic, part), 0)

    def set_committed(self, group: str, topic: str, part: int, offset: int) -> None:
        self._committed[(group, topic, part)] = offset


class BrokerConsumer:
    """A consumer-group member over a set of topics/partitions. ``poll`` advances
    a fetch position; ``commit`` persists it as the group's offset; ``revoke``
    (rebalance) resets the fetch position to the committed offset, so any polled-
    but-uncommitted messages are redelivered."""

    def __init__(self, broker: Broker, group: str, topics: list[str],
                 partitions: Optional[list[int]] = None) -> None:
        self._b = broker
        self.group = group
        self.topics = topics
        self._assigned = partitions if partitions is not None else list(range(broker._partitions))
        self._pos: dict[tuple[str, int], int] = {}
        self._reset()

    def _reset(self) -> None:  # fetch position <- committed offset
        for t in self.topics:
            for p in self._assigned:
                self._pos[(t, p)] = self._b.committed(self.group, t, p)

    def poll(self, max_events: int) -> list[Any]:
        out: list[Any] = []
        for t in self.topics:
            for p in self._assigned:
                log = self._b._log(t, p)
                pos = self._pos[(t, p)]
                while pos < len(log) and len(out) < max_events:
                    out.append(log[pos])
                    pos += 1
                self._pos[(t, p)] = pos
        return out

    def commit(self) -> None:
        for t in self.topics:
            for p in self._assigned:
                self._b.set_committed(self.group, t, p, self._pos[(t, p)])

    def revoke(self) -> None:
        self._reset()  # rebalance: uncommitted work will be redelivered

    def close(self) -> None:
        pass


class BrokerInputSource:
    """A state-machine ``InputSource`` backed by a :class:`BrokerConsumer`."""

    def __init__(self, broker: Broker, group: str, topics: list[str]) -> None:
        self._c = BrokerConsumer(broker, group, topics)

    def poll(self, max_events: int) -> list[Any]:
        return self._c.poll(max_events)

    def commit(self) -> None:
        self._c.commit()

    def revoke(self) -> None:
        self._c.revoke()

    def close(self) -> None:
        self._c.close()


class BrokerEventSource:
    """A retention/audit ``EventSource`` backed by a :class:`BrokerConsumer`."""

    def __init__(self, broker: Broker, group: str, topics: list[str]) -> None:
        self._c = BrokerConsumer(broker, group, topics)

    def poll(self, max_events: int) -> list[Any]:
        return self._c.poll(max_events)

    def commit(self) -> None:
        self._c.commit()

    def revoke(self) -> None:
        self._c.revoke()

    def close(self) -> None:
        self._c.close()
