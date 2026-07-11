"""Supported access to native Aerospike features.

Phronexus's :class:`~phronexus.kv.base.KVStore` API is intentionally small. When
you need a native capability it doesn't wrap — Aerospike **expressions/filters**,
**CDT list/map operations**, ``operate()``, **batch** ops, **secondary-index
queries**, or **UDFs** — use :class:`NativeAerospike`. It hands you the connected
client plus namespaced helpers, and *guards Phronexus-managed sets*: mutating a
managed set (the manifest, projections, inverted index, outboxes, dedup markers,
schedules, journals) directly would bypass the manifest-last invariant, the
in-transaction index, and the change feed — so those writes must go through
``px.put()`` / the state machine.

Reads and queries are never guarded; the guard only blocks *writes* to managed
sets. For your own sets you get the full native client with no restrictions.

    nx = px.native_aerospike()
    # native CDT + expressions on your own set — fully supported
    from aerospike_helpers.operations import list_operations as lo
    nx.operate("positions", "acct-1", [lo.list_append("legs", leg)])
    # a secondary-index query you manage yourself
    q = nx.query("positions"); q.where(predicates.equals("book", "IRD-1"))
    rows = q.results()
    # raw client for anything else
    nx.client.job_info(job_id, aerospike.JOB_SCAN)
"""

from __future__ import annotations

from typing import Any, Optional

from phronexus.errors import ConfigError, StorageError


def managed_sets(px) -> set[str]:
    """Every set Phronexus owns — framework sets + per-contract manifest/projections.

    Writes here must go through the framework; this powers the guard in
    :class:`NativeAerospike`.
    """
    s = px.settings
    names: set[str] = {
        s.aerospike.contracts_set, s.aerospike.index_set, s.aerospike.outbox_set,
        s.aerospike.changefeed_outbox_set,
        s.statemachine.outbox_set, s.statemachine.dedup_set,
        s.scheduler.schedules_set, s.scheduler.state_set, s.scheduler.outbox_set,
        s.journal.messages_set, s.journal.interactions_set,
    }
    try:
        for c in px.registry.storage_contracts():
            names.add(c.manifest_set)
            for p in c.projections:
                names.add(p.set)
    except Exception:  # noqa: BLE001 - registry may be empty; framework sets still apply
        pass
    return {n for n in names if n}


class NativeAerospike:
    """Namespaced, managed-set-guarded access to the native Aerospike client."""

    def __init__(self, px):
        from phronexus.kv.aerospike import AerospikeKV

        if not isinstance(px.store, AerospikeKV):
            raise ConfigError(
                "native_aerospike() requires backend='aerospike' "
                f"(current backend: {px.settings.backend!r})"
            )
        self._px = px
        self.client = px.store.native_client()   # the connected aerospike.Client
        self.namespace: str = px.store.namespace
        self._managed = managed_sets(px)

    # --- managed-set guard ---------------------------------------------

    def refresh(self) -> None:
        """Recompute the managed-set list (call after publishing new contracts)."""
        self._managed = managed_sets(self._px)

    def managed_sets(self) -> set[str]:
        return set(self._managed)

    def is_managed(self, set_name: str) -> bool:
        return set_name in self._managed

    def assert_writable(self, set_name: str) -> None:
        if set_name in self._managed:
            raise StorageError(
                f"set {set_name!r} is managed by Phronexus — write it through "
                "px.put()/the state machine, not the native client (a direct write "
                "bypasses the manifest, index, and change feed)"
            )

    def key(self, set_name: str, key: str) -> tuple:
        """Build a native ``(namespace, set, key)`` tuple."""
        return (self.namespace, set_name, key)

    # --- guarded mutations (your own sets only) ------------------------

    def put(self, set_name: str, key: str, bins: dict, *, meta=None, policy=None):
        self.assert_writable(set_name)
        return self.client.put(self.key(set_name, key), bins, meta=meta, policy=policy)

    def operate(self, set_name: str, key: str, ops: list, *, meta=None, policy=None):
        """Atomic multi-op on one record (CDT list/map ops, expressions, etc.)."""
        self.assert_writable(set_name)
        return self.client.operate(self.key(set_name, key), ops, meta, policy)

    def remove(self, set_name: str, key: str, *, meta=None, policy=None):
        self.assert_writable(set_name)
        return self.client.remove(self.key(set_name, key), meta=meta, policy=policy)

    def apply(self, set_name: str, key: str, module: str, function: str,
              args: list, *, policy=None):
        """Apply a record UDF."""
        self.assert_writable(set_name)
        return self.client.apply(self.key(set_name, key), module, function, args, policy)

    # --- unguarded reads / queries -------------------------------------

    def get(self, set_name: str, key: str, *, policy=None):
        return self.client.get(self.key(set_name, key), policy=policy)

    def select(self, set_name: str, key: str, bins: list, *, policy=None):
        return self.client.select(self.key(set_name, key), bins, policy=policy)

    def query(self, set_name: str):
        """Native secondary-index query builder over ``set_name`` in this namespace."""
        return self.client.query(self.namespace, set_name)

    def scan(self, set_name: str):
        """Full-set scan (query with no predicate)."""
        return self.client.query(self.namespace, set_name)

    def create_index(self, set_name: str, bin_name: str, index_name: str,
                     *, index_type: Optional[Any] = None, policy=None):
        """Create a native secondary index (DDL)."""
        import aerospike

        dt = index_type if index_type is not None else aerospike.INDEX_STRING
        return self.client.index_create(
            self.namespace, set_name, bin_name, index_name, {"index_type": dt}, policy
        )
