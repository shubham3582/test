"""Retention compaction: ``python -m phronexus.retention.compact``.

Coalesces the insert-only Iceberg log so it doesn't grow without bound:
deduplicates replay rows by ``_txn``, drops fully-tombstoned documents and rows
past their retention horizon, and rewrites each table as one snapshot (which also
compacts small files). Idempotent — run it on a schedule (it pairs naturally with
the distributed scheduler, e.g. a nightly ``ccr-compaction`` trigger).

    python -m phronexus.retention.compact            # keep version history
    python -m phronexus.retention.compact --collapse # also drop old versions
"""

from __future__ import annotations

import argparse

import structlog

from phronexus.config import Settings
from phronexus.contracts.registry import ContractRegistry
from phronexus.kv import build_store
from phronexus.observability.logging import configure_logging
from phronexus.retention.warehouse import build_warehouse
from phronexus.retention.worker import RetentionWorker

log = structlog.get_logger("phronexus.retention.compact")


def run(settings: Settings, *, keep_history: bool = True, now: float = 0.0) -> list[dict]:
    store = build_store(settings)
    registry = ContractRegistry(
        store, contracts_set=settings.aerospike.contracts_set,
        refresh_seconds=settings.contracts.refresh_seconds,
    )
    registry.refresh(force=True)
    warehouse = build_warehouse(settings.iceberg)
    worker = RetentionWorker(registry, warehouse)
    try:
        stats = worker.compact_all(now=now, keep_history=keep_history)
        for s in stats:
            log.info("retention.compacted", **s)
        return stats
    finally:
        warehouse.close()


def main() -> None:  # pragma: no cover - process entrypoint
    ap = argparse.ArgumentParser(description="Compact the Iceberg retention log.")
    ap.add_argument("--collapse", action="store_true",
                    help="also drop superseded versions (keep only the latest per doc)")
    ap.add_argument("--now", type=float, default=0.0,
                    help="epoch seconds used to evaluate _expire_at (0 = don't expire)")
    args = ap.parse_args()

    settings = Settings()
    configure_logging(settings.observability)
    stats = run(settings, keep_history=not args.collapse, now=args.now)
    total = sum(s["removed"] for s in stats)
    print(f"compacted {len(stats)} table(s); removed {total} row(s)")


if __name__ == "__main__":  # pragma: no cover
    main()
