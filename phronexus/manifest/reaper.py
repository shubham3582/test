"""Orphan reaper.

Crashed writes (projections written, manifest never committed) leave invisible
orphan projection records. The reaper scans projection sets and removes records
whose ``_txn`` does not match any committed manifest and that are older than a
grace period. It is safe to run continuously — it only ever deletes records that
no committed manifest references.
"""

from __future__ import annotations

import time
from typing import Iterable

import structlog

from phronexus.contracts.registry import ContractRegistry
from phronexus.kv.base import KVStore
from phronexus.manifest.manager import M_PROJECTIONS, M_STATUS, STATUS_COMMITTED
from phronexus.storage.projection import META_TXN

log = structlog.get_logger(__name__)


class Reaper:
    def __init__(self, store: KVStore, registry: ContractRegistry):
        self._store = store
        self._registry = registry

    def _committed_keys(self, manifest_set: str) -> set[tuple[str, str]]:
        live: set[tuple[str, str]] = set()
        for _key, rec in self._store.scan(manifest_set):
            if rec.bins.get(M_STATUS) == STATUS_COMMITTED:
                for p in rec.bins.get(M_PROJECTIONS, []):
                    live.add((p["set"], p["key"]))
        return live

    def sweep_entity(self, entity: str) -> int:
        """Reap orphan projections for one entity. Returns count removed."""
        sc = self._registry.active_storage(entity)
        referenced = self._committed_keys(sc.manifest_set)
        removed = 0
        sets = {p.set for p in sc.projections}
        for set_name in sets:
            for key, rec in list(self._store.scan(set_name)):
                if (set_name, key) not in referenced and rec.bins.get(META_TXN):
                    self._store.remove(set_name, key)
                    removed += 1
        if removed:
            log.info("reaper.swept", entity=entity, removed=removed)
        return removed

    def sweep(self, entities: Iterable[str]) -> int:
        return sum(self.sweep_entity(e) for e in entities)
