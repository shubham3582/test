"""Standalone audit worker: ``python -m phronexus.audit.main``.

Runs as its own process next to the app: it builds a registry against the same
store, consumes the Kafka change feed, and writes per-document trace records. It
never touches the hot write path, so it can be scaled or restarted freely — a
crash just replays the batch (records are idempotent by txn).

Entities are re-resolved from the contract registry each cycle, so the worker can
start before contracts are ingested and pick up new ones without a restart.
"""

from __future__ import annotations

import signal
import threading

import structlog

from phronexus.audit.log import AuditLog
from phronexus.audit.worker import AuditWorker
from phronexus.config import Settings
from phronexus.contracts.models import StorageContract
from phronexus.contracts.registry import ContractRegistry
from phronexus.kv import build_store
from phronexus.observability.logging import configure_logging
from phronexus.retention.source import KafkaEventSource  # shared change-feed consumer

log = structlog.get_logger("phronexus.audit")


def _entities(registry: ContractRegistry) -> list[str]:
    return sorted({
        c.entity
        for c in list(registry._by_identity.values())  # noqa: SLF001
        if isinstance(c, StorageContract)
    })


def consume_once(worker, source, max_events: int = 500) -> int:
    """Poll a batch, record it, then commit — commit only after durable record
    (replay is safe: audit rows are idempotent by txn). Returns events handled."""
    events = source.poll(max_events)
    if events:
        worker.process(events)
    commit = getattr(source, "commit", None)
    if commit is not None:
        commit()
    return len(events)


def main() -> None:  # pragma: no cover - process entrypoint
    settings = Settings()
    configure_logging(settings.observability)

    store = build_store(settings)
    registry = ContractRegistry(
        store,
        contracts_set=settings.aerospike.contracts_set,
        refresh_seconds=settings.contracts.refresh_seconds,
    )
    audit = AuditLog(
        store, settings.audit.audit_set, settings.audit.ttl_seconds, settings.audit.enabled
    )
    worker = AuditWorker(audit, registry)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    poll = settings.scheduler.poll_seconds
    entities: list[str] = []
    source: KafkaEventSource | None = None
    backoff = 1.0
    log.info("audit.start", set=settings.audit.audit_set)
    try:
        while not stop.is_set():
            try:
                registry.refresh(force=True)
                current = _entities(registry)
                if current != entities:
                    if source is not None:
                        source.close()
                    entities = current
                    source = (
                        KafkaEventSource(settings.kafka, entities, group_id="phronexus-audit")
                        if entities
                        else None
                    )
                    log.info("audit.subscribed", entities=entities)
                if source is not None:
                    consume_once(worker, source)
                backoff = 1.0  # progress -> reset
                stop.wait(poll if not entities else 1.0)
            except Exception as exc:  # noqa: BLE001 - transient broker/store error
                log.warning("audit.iteration_failed", error=str(exc), retry_in_s=round(backoff, 1))
                stop.wait(backoff)
                backoff = min(backoff * 2, 30.0)
    finally:
        if source is not None:
            source.close()
        log.info("audit.stop", stats=worker.stats)


if __name__ == "__main__":  # pragma: no cover
    main()
