"""Validate the Iceberg retention lake: ``python -m phronexus.retention.validate``.

Loads the same catalog the retention worker writes to, then for every
Iceberg-enabled storage contract prints the table's row count and a sample row.
Run it after writing some documents through the API to confirm rows landed on
S3/Iceberg end to end. Exits non-zero if no enabled table has any rows yet.
"""

from __future__ import annotations

import sys

import structlog

from phronexus.config import Settings
from phronexus.contracts.registry import ContractRegistry
from phronexus.kv import build_store
from phronexus.observability.logging import configure_logging
from phronexus.retention.warehouse import IcebergWarehouse
from phronexus.retention.worker import RetentionWorker

log = structlog.get_logger("phronexus.retention.validate")


def main() -> None:  # pragma: no cover - process entrypoint
    settings = Settings()
    configure_logging(settings.observability)
    if settings.iceberg.backend != "iceberg":
        print("iceberg.backend is not 'iceberg' — nothing to validate")
        sys.exit(2)

    store = build_store(settings)
    registry = ContractRegistry(
        store, contracts_set=settings.aerospike.contracts_set,
        refresh_seconds=settings.contracts.refresh_seconds,
    )
    registry.refresh(force=True)
    warehouse = IcebergWarehouse(settings.iceberg)
    worker = RetentionWorker(registry, warehouse)

    tables = sorted({c.iceberg.table for c in worker._enabled_contracts()})  # noqa: SLF001
    if not tables:
        print("no Iceberg-enabled contracts found (ingest contracts first)")
        sys.exit(2)

    total = 0
    for table in tables:
        try:
            rows = warehouse.scan(table)
        except Exception as exc:  # noqa: BLE001 - table may not exist yet
            print(f"{table:24} — not created yet ({type(exc).__name__})")
            continue
        total += len(rows)
        current = warehouse.latest_state(table)  # reconciled: latest per doc, no tombstones
        sample = {k: v for k, v in (current[0].items() if current else [])
                  if not k.startswith("_")}
        print(f"{table:24} log_rows={len(rows):<6} current={len(current):<6} sample={sample}")

    warehouse.close()
    if total == 0:
        print("\nNo rows landed yet — write some documents, then re-run.")
        sys.exit(1)
    print(f"\nOK: {total} row(s) retained across {len(tables)} Iceberg table(s).")


if __name__ == "__main__":  # pragma: no cover
    main()
