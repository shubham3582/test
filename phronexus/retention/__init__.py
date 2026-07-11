"""Phase 4 — async long-term retention into Iceberg.

The write path emits a :class:`~phronexus.events.base.CommitEvent` per committed
document onto the change feed (Kafka/Redpanda). The retention worker consumes
that feed and lands rows in Iceberg tables declared by each storage contract's
``iceberg`` block. Aerospike stays the source of truth for the hot path; Iceberg
is the analytical, long-term tail.

Everything is behind small abstractions so it runs in-process for tests
(``MemoryEventSource`` + ``InMemoryWarehouse``) and against Kafka + pyiceberg in
production.
"""

from phronexus.retention.source import EventSource, MemoryEventSource
from phronexus.retention.warehouse import (
    InMemoryWarehouse,
    Warehouse,
    build_warehouse,
    decode,
    reconcile,
)
from phronexus.retention.worker import RetentionWorker

__all__ = [
    "EventSource",
    "MemoryEventSource",
    "Warehouse",
    "InMemoryWarehouse",
    "build_warehouse",
    "reconcile",
    "decode",
    "RetentionWorker",
]
