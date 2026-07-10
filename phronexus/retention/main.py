"""Standalone retention worker: ``python -m phronexus.retention.main``.

In production this runs as its own process next to (not inside) the app: it
builds a registry against the same Aerospike cluster, consumes the Kafka change
feed, and writes to Iceberg. It never touches the hot write path.
"""

from __future__ import annotations

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

    # Subscribe to every entity that declares Iceberg retention.
    entities = sorted({c.entity for c in worker._enabled_contracts()})  # noqa: SLF001
    source = KafkaEventSource(settings.kafka, entities)
    log.info("retention.start", entities=entities, warehouse=settings.iceberg.backend)
    try:
        while True:
            worker.run(source, batch_size=settings.iceberg.batch_size, max_batches=1)
    except KeyboardInterrupt:
        pass
    finally:
        source.close()
        warehouse.close()
        log.info("retention.stop", stats=worker.stats)


if __name__ == "__main__":  # pragma: no cover
    main()
