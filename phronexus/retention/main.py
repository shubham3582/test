"""Standalone retention worker: ``python -m phronexus.retention.main``.

In production this runs as its own process next to (not inside) the app: it
builds a registry against the same Aerospike cluster, consumes the Kafka change
feed, and writes to Iceberg. It never touches the hot write path.

Entities are re-resolved from the contract registry each cycle, so the worker
can start before contracts are ingested and pick them up as they appear (and
subscribe to newly Iceberg-enabled entities without a restart).
"""

from __future__ import annotations

import signal
import threading

import structlog

from phronexus.config import Settings
from phronexus.contracts.registry import ContractRegistry
from phronexus.kv import build_store
from phronexus.observability.logging import configure_logging
from phronexus.retention.source import KafkaEventSource
from phronexus.retention.warehouse import build_warehouse
from phronexus.retention.worker import RetentionWorker

log = structlog.get_logger("phronexus.retention")


def main() -> None:  # pragma: no cover - process entrypoint
    settings = Settings()
    configure_logging(settings.observability)

    store = build_store(settings)
    registry = ContractRegistry(
        store,
        contracts_set=settings.aerospike.contracts_set,
        refresh_seconds=settings.contracts.refresh_seconds,
    )
    warehouse = build_warehouse(settings.iceberg)
    worker = RetentionWorker(registry, warehouse)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    poll = settings.scheduler.poll_seconds  # reuse a small poll cadence
    entities: list[str] = []
    source: KafkaEventSource | None = None
    log.info("retention.start", warehouse=settings.iceberg.backend)
    try:
        while not stop.is_set():
            # Re-resolve the set of Iceberg-enabled entities; (re)subscribe on change.
            registry.refresh(force=True)
            current = sorted({c.entity for c in worker._enabled_contracts()})  # noqa: SLF001
            if current != entities:
                if source is not None:
                    source.close()
                entities = current
                source = KafkaEventSource(settings.kafka, entities) if entities else None
                log.info("retention.subscribed", entities=entities)
            if source is not None:
                worker.run(source, batch_size=settings.iceberg.batch_size, max_batches=1)
                if hasattr(warehouse, "flush"):
                    warehouse.flush()
                # Commit offsets only after the batch is durably in Iceberg.
                source.commit()
            stop.wait(poll if not entities else 1.0)
    finally:
        if source is not None:
            source.close()
        warehouse.close()
        log.info("retention.stop", stats=worker.stats)


if __name__ == "__main__":  # pragma: no cover
    main()
