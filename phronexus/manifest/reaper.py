"""Orphan reaper.

Crashed writes (projections written, manifest never committed) leave invisible
orphan projection records. The reaper scans projection sets and removes records
whose ``_txn`` does not match any committed manifest.

Two safeguards keep it from ever deleting a *live* write:

1. **Grace period** — a record younger than ``orphan_grace_seconds`` (by its
   ``_ts`` write stamp) is never reaped, so an in-flight non-transactional write
   that has written its projections but not yet its manifest is left alone until
   it has had ample time to commit.
2. **Re-check before delete** — the set of manifest-referenced projection keys is
   recomputed immediately before deletion, closing the TOCTOU window between the
   manifest scan and the projection scan (a write that commits mid-sweep is not
   reaped).

It is safe to run continuously: it only ever deletes records that no committed
manifest references *and* that are older than the grace period.
"""

from __future__ import annotations

import time
from typing import Iterable, Optional

import structlog

from phronexus.contracts.registry import ContractRegistry
from phronexus.kv.base import KVStore
from phronexus.manifest.manager import M_PROJECTIONS, M_STATUS, STATUS_COMMITTED
from phronexus.storage.projection import META_TS, META_TXN

log = structlog.get_logger(__name__)


class Reaper:
    def __init__(
        self,
        store: KVStore,
        registry: ContractRegistry,
        grace_seconds: int = 300,
    ):
        self._store = store
        self._registry = registry
        self._grace_seconds = grace_seconds

    def _committed_keys(self, manifest_set: str) -> set[tuple[str, str]]:
        live: set[tuple[str, str]] = set()
        for _key, rec in self._store.scan(manifest_set):
            if rec.bins.get(M_STATUS) == STATUS_COMMITTED:
                for p in rec.bins.get(M_PROJECTIONS, []):
                    live.add((p["set"], p["key"]))
        return live

    def sweep_entity(self, entity: str, *, now: Optional[float] = None) -> int:
        """Reap orphan projections for one entity. Returns count removed.

        ``now`` (wall-clock seconds) is injectable for deterministic tests;
        defaults to ``time.time()``.
        """
        now = time.time() if now is None else now
        sc = self._registry.active_storage(entity)
        referenced = self._committed_keys(sc.manifest_set)
        sets = {p.set for p in sc.projections}

        # Phase 1: collect candidates — unreferenced, carry a _txn, and older than
        # the grace period. A record without a _ts stamp (written before this
        # release) has unknown age; the delete-time re-check is its only guard.
        candidates: list[tuple[str, str]] = []
        for set_name in sets:
            for key, rec in list(self._store.scan(set_name)):
                if (set_name, key) in referenced or not rec.bins.get(META_TXN):
                    continue
                ts = rec.bins.get(META_TS)
                if ts is not None and (now - ts) < self._grace_seconds:
                    continue  # too young — may be an in-flight write
                candidates.append((set_name, key))
        if not candidates:
            return 0

        # Phase 2: re-check references right before deleting, so a write that
        # committed during phase 1 is not reaped (closes the scan TOCTOU).
        referenced = self._committed_keys(sc.manifest_set)
        removed = 0
        for set_name, key in candidates:
            if (set_name, key) in referenced:
                continue
            self._store.remove(set_name, key)
            removed += 1
        if removed:
            log.info("reaper.swept", entity=entity, removed=removed)
        return removed

    def sweep(self, entities: Iterable[str], *, now: Optional[float] = None) -> int:
        return sum(self.sweep_entity(e, now=now) for e in entities)
