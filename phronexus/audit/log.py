"""Durable, per-document audit trail — the data behind the trace / debug view.

Records are keyed by ``(entity, doc_id)`` and correlated by ``txn_id``. The
:class:`AuditLog` is just the store read/write primitive; it is *populated* by the
audit worker (:mod:`phronexus.audit.worker`) — a change-feed consumer that runs
as its own process in production and inline for the no-services dev mode. Nothing
on the hot write path writes here, so auditing is fully decoupled from writes.

Reading a trace is a single scan filtered to one document; records are sorted by
a nanosecond stamp so the timeline is stable even when several share a
wall-clock ts.
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Optional

from phronexus.kv.base import KVStore, Transaction

# Bin names are kept short (Aerospike caps bin names at 15 chars) and flat, so a
# trace is queryable without unpacking a blob.
B_ENTITY = "entity"
B_DOC_ID = "doc_id"
B_KIND = "kind"
B_NS = "ns"       # time.time_ns() — stable sort key
B_TS = "ts"       # wall-clock seconds (for display)


class AuditLog:
    """Append-only audit records in one KV set.

    ``kind`` is one of ``commit`` | ``delete``. Extra scalar fields (``txn_id``,
    ``version``, ``cver``, ``state``, ...) are stored as their own bins so the
    trace reads without decoding.
    """

    def __init__(
        self, store: KVStore, set_name: str = "_audit", ttl: int = 0, enabled: bool = True
    ) -> None:
        self._store = store
        self._set = set_name
        self._ttl = ttl
        self.enabled = enabled

    def stage(
        self,
        txn: Optional[Transaction],
        *,
        entity: str,
        doc_id: str,
        kind: str,
        ts: Optional[float] = None,
        **fields: Any,
    ) -> None:
        """Append one record. ``txn`` is accepted for symmetry with the store API
        but the worker writes standalone (``txn=None``)."""
        if not self.enabled:
            return
        ns = time.time_ns()
        bins: dict[str, Any] = {
            B_ENTITY: entity,
            B_DOC_ID: doc_id,
            B_KIND: kind,
            B_NS: ns,
            B_TS: ts if ts is not None else ns / 1e9,
        }
        # Drop Nones so absent fields (e.g. state=None) don't clutter bins.
        bins.update({k: v for k, v in fields.items() if v is not None})
        key = f"{entity}:{doc_id}:{ns}:{uuid.uuid4().hex[:6]}"
        self._store.put(self._set, key, bins, ttl=self._ttl, txn=txn)

    def record(self, *, entity: str, doc_id: str, kind: str, **fields: Any) -> None:
        """Standalone append (no surrounding transaction)."""
        self.stage(None, entity=entity, doc_id=doc_id, kind=kind, **fields)

    def trace(self, entity: str, doc_id: str) -> list[dict[str, Any]]:
        """Return every audit record for one document, oldest first, with derived
        transition markers (``from``/``to`` set whenever the state field moves)."""
        rows = [
            dict(rec.bins)
            for _key, rec in self._store.scan(self._set)
            if rec.bins.get(B_ENTITY) == entity and rec.bins.get(B_DOC_ID) == doc_id
        ]
        rows.sort(key=lambda r: r.get(B_NS, 0))
        # Derive transitions at read time: a commit whose state differs from the
        # previous committed state is a transition. Keeps the worker stateless.
        prev_state: Any = None
        for r in rows:
            if r.get(B_KIND) == "commit" and "state" in r:
                if r["state"] != prev_state:
                    r["from"] = prev_state
                    r["to"] = r["state"]
                prev_state = r["state"]
        return rows
