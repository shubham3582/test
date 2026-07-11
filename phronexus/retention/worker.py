"""The retention worker: change feed -> Iceberg.

Consumes commit events, resolves each entity's storage contract, and lands rows
in the contract's Iceberg table (only for entities with ``iceberg.enabled``).
Upserts are keyed by doc id so replays are idempotent; deletes remove the row.
Each row carries retention metadata (``_expire_at`` from ``retention_days``) so a
maintenance pass can drop aged-out rows.
"""

from __future__ import annotations

from typing import Any

import structlog

from phronexus import codec
from phronexus.contracts.models import StorageContract
from phronexus.contracts.registry import ContractRegistry
from phronexus.errors import ContractNotFound
from phronexus.events.base import CommitEvent
from phronexus.retention.source import EventSource
from phronexus.retention.warehouse import Warehouse

log = structlog.get_logger(__name__)

_DAY = 86400


class RetentionWorker:
    def __init__(self, registry: ContractRegistry, warehouse: Warehouse):
        self._registry = registry
        self._warehouse = warehouse
        self.stats = {"upserts": 0, "deletes": 0, "skipped": 0}

    def process(self, events: list[CommitEvent]) -> dict[str, int]:
        for ev in events:
            sc = self._storage_for(ev)
            if sc is None or not sc.iceberg.enabled or not sc.iceberg.table:
                self.stats["skipped"] += 1
                continue
            # Insert-only: every commit is one immutable row, idempotent by txn.
            idem_key = f"{ev.doc_id}:{ev.txn_id}"
            self._warehouse.append(sc.iceberg.table, idem_key, self._row(sc, ev))
            self.stats["deletes" if ev.op == "delete" else "upserts"] += 1
        return dict(self.stats)

    def run(self, source: EventSource, *, batch_size: int = 500, max_batches: int | None = None) -> dict[str, int]:
        batches = 0
        while max_batches is None or batches < max_batches:
            events = source.poll(batch_size)
            if not events:
                break
            self.process(events)
            batches += 1
        return dict(self.stats)

    def expire_all(self, now: float) -> int:
        """Drop rows past their retention horizon across all enabled tables."""
        removed = 0
        for sc in self._enabled_contracts():
            removed += self._warehouse.expire(sc.iceberg.table, now)
        return removed

    # --- internals ------------------------------------------------------

    def _storage_for(self, ev: CommitEvent):
        # Pin the exact contract version that produced the document.
        try:
            return self._registry.get_version(f"storage:{ev.entity}:v{ev.contract_version}")
        except ContractNotFound:
            try:
                return self._registry.active_storage(ev.entity)
            except ContractNotFound:
                return None

    def _row(self, sc: StorageContract, ev: CommitEvent) -> dict[str, Any]:
        # Typed metadata columns (queryable) + one msgpack blob (_raw) holding the
        # exact document for byte-faithful retrieval. Deletes carry no document.
        row = dict(ev.document or {})
        row["_doc_id"] = ev.doc_id
        row["_txn"] = ev.txn_id            # idempotency key
        row["_version"] = ev.version        # monotonic order for "latest wins"
        row["_op"] = ev.op                  # upsert | delete (tombstone)
        row["_cver"] = ev.contract_version
        row["_ts"] = ev.ts
        row["_raw"] = codec.pack(ev.document) if ev.document is not None else None
        if sc.iceberg.retention_days > 0:
            row["_expire_at"] = ev.ts + sc.iceberg.retention_days * _DAY
        else:
            row["_expire_at"] = None
        return row

    def _enabled_contracts(self) -> list[StorageContract]:
        seen: dict[str, StorageContract] = {}
        for identity, contract in list(self._registry._by_identity.items()):  # noqa: SLF001
            if isinstance(contract, StorageContract) and contract.iceberg.enabled and contract.iceberg.table:
                seen[contract.iceberg.table] = contract
        return list(seen.values())
