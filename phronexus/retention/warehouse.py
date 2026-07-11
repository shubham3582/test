"""Warehouse abstraction: the insert-only retention log.

Retention is modelled as an **append-only, idempotent event log**. Every commit
becomes one immutable row tagged with its ``_op`` (upsert/delete), a monotonic
``_version`` (the manifest generation) and its ``_txn`` (idempotency key); the
full document is also kept as a msgpack blob in ``_raw`` for byte-faithful
retrieval. Nothing is ever mutated in place.

Because rows are immutable and keyed by ``_txn``, a replay after a crash is a
no-op (same key -> same row). "Current state" is a *derived view*:
:func:`reconcile` keeps the max-``_version`` row per ``_doc_id`` and drops
tombstones. That view is insensitive to duplicate appends, so the pipeline only
needs at-least-once delivery to be correct — exactly-once falls out of the read.

``InMemoryWarehouse`` backs tests/demos; ``IcebergWarehouse`` appends to real
Iceberg tables via pyiceberg.
"""

from __future__ import annotations

import abc
from typing import Any

import structlog

from phronexus import codec
from phronexus.config import IcebergSettings

log = structlog.get_logger(__name__)


def reconcile(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse an append log to current state: latest ``_version`` per doc, no tombstones."""
    latest: dict[str, dict[str, Any]] = {}
    for r in rows:
        doc_id = r.get("_doc_id")
        if doc_id is None:
            continue
        cur = latest.get(doc_id)
        if cur is None or r.get("_version", 0) >= cur.get("_version", 0):
            latest[doc_id] = r
    return [r for r in latest.values() if r.get("_op") != "delete"]


def decode(row: dict[str, Any]) -> Any:
    """Recover the exact original document from a row's msgpack ``_raw`` blob."""
    raw = row.get("_raw")
    return codec.unpack(raw) if raw is not None else None


class Warehouse(abc.ABC):
    @abc.abstractmethod
    def append(self, table: str, idem_key: str, row: dict[str, Any]) -> None:
        """Insert one immutable event row, idempotent by ``idem_key``."""

    @abc.abstractmethod
    def scan(self, table: str) -> list[dict[str, Any]]:
        """Every row in the append log (full history)."""

    def latest_state(self, table: str) -> list[dict[str, Any]]:
        """Reconciled current state — latest row per doc id, tombstones removed."""
        return reconcile(self.scan(table))

    @abc.abstractmethod
    def rewrite(self, table: str, rows: list[dict[str, Any]]) -> None:
        """Replace the table's contents with ``rows`` (used by compaction)."""

    def compact(self, table: str, *, now: float = 0.0, keep_history: bool = True) -> dict[str, Any]:
        """Coalesce the append log: dedup replay rows by ``_txn``, drop fully
        tombstoned docs and rows past ``_expire_at``. With ``keep_history=False``
        also collapse to the latest version per doc. Returns before/after counts."""
        rows = self.scan(table)
        before = len(rows)
        # 1) Dedup replay artifacts (same doc + txn appended more than once).
        by_txn = {(r.get("_doc_id"), r.get("_txn")): r for r in rows}
        deduped = list(by_txn.values())
        # 2) Find docs whose latest version is a delete tombstone.
        latest: dict[Any, dict[str, Any]] = {}
        for r in deduped:
            did = r.get("_doc_id")
            if did is None:
                continue
            cur = latest.get(did)
            if cur is None or r.get("_version", 0) >= cur.get("_version", 0):
                latest[did] = r
        dead = {d for d, r in latest.items() if r.get("_op") == "delete"}
        # 3) Drop tombstoned docs + expired rows (+ old versions if collapsing).
        kept = []
        for r in deduped:
            did = r.get("_doc_id")
            if did in dead:
                continue
            if now and r.get("_expire_at") and r["_expire_at"] <= now:
                continue
            if not keep_history and r is not latest.get(did):
                continue
            kept.append(r)
        self.rewrite(table, kept)
        return {"table": table, "before": before, "after": len(kept), "removed": before - len(kept)}

    def count(self, table: str) -> int:
        return len(self.scan(table))

    def expire(self, table: str, now: float) -> int:  # pragma: no cover - overridden
        return 0

    def close(self) -> None:  # pragma: no cover - trivial default
        pass


class InMemoryWarehouse(Warehouse):
    def __init__(self) -> None:
        # table -> idem_key -> row. Keying by idem_key makes append idempotent.
        self._log: dict[str, dict[str, dict[str, Any]]] = {}

    def append(self, table: str, idem_key: str, row: dict[str, Any]) -> None:
        self._log.setdefault(table, {})[idem_key] = dict(row)

    def scan(self, table: str) -> list[dict[str, Any]]:
        return list(self._log.get(table, {}).values())

    def rewrite(self, table: str, rows: list[dict[str, Any]]) -> None:
        # Re-key by (doc_id, txn) so idempotent append semantics are preserved.
        self._log[table] = {f"{r.get('_doc_id')}:{r.get('_txn')}": dict(r) for r in rows}

    def expire(self, table: str, now: float) -> int:
        rows = self._log.get(table, {})
        doomed = [k for k, r in rows.items() if r.get("_expire_at") and r["_expire_at"] <= now]
        for k in doomed:
            del rows[k]
        if doomed:
            log.info("retention.expired", table=table, rows=len(doomed))
        return len(doomed)

    def tables(self) -> list[str]:
        return list(self._log)


class IcebergWarehouse(Warehouse):  # pragma: no cover - needs catalog + pyarrow
    """Append-only writer to Iceberg via pyiceberg.

    Each commit is one row; the current-state view is produced by :func:`reconcile`
    (or the equivalent ``row_number() OVER (PARTITION BY _doc_id ORDER BY _version
    DESC)`` SQL). A scheduled compaction can dedup by ``_txn`` and drop tombstoned
    / expired rows so the log does not grow without bound.
    """

    def __init__(self, cfg: IcebergSettings):
        try:
            import pyarrow  # noqa: F401
            from pyiceberg.catalog import load_catalog
        except ImportError as exc:
            raise RuntimeError(
                "IcebergWarehouse requires pyiceberg + pyarrow: pip install 'phronexus-core[iceberg]'"
            ) from exc
        self._cfg = cfg
        # catalog_properties() carries the REST-catalog token/TLS + S3 credentials.
        self._catalog = load_catalog(cfg.catalog_name, **cfg.catalog_properties())
        # table -> idem_key -> row (dedup within a flush batch).
        self._buffers: dict[str, dict[str, dict[str, Any]]] = {}

    def append(self, table: str, idem_key: str, row: dict[str, Any]) -> None:
        self._buffers.setdefault(table, {})[idem_key] = row

    def flush(self) -> None:
        import pyarrow as pa

        for table, rows in self._buffers.items():
            if not rows:
                continue
            arrow = pa.Table.from_pylist(list(rows.values()))
            tbl = self._ensure_table(table, arrow.schema)
            # Conform the batch to the table's schema (add missing columns as
            # nulls, order to match) so appends survive per-batch key drift.
            tbl.append(self._conform(arrow, tbl.schema().as_arrow()))
        self._buffers.clear()

    def _ensure_table(self, table: str, arrow_schema):
        """Load the table, creating the namespace + table on first use.

        The retention worker owns its tables, so bootstrapping here keeps the
        stack self-serving: no manual DDL before rows can land.
        """
        from pyiceberg.exceptions import (
            NamespaceAlreadyExistsError,
            NoSuchTableError,
            TableAlreadyExistsError,
        )

        ident = tuple(table.split("."))
        if len(ident) > 1:
            try:
                self._catalog.create_namespace(ident[:-1])
            except NamespaceAlreadyExistsError:
                pass
        try:
            return self._catalog.load_table(table)
        except NoSuchTableError:
            try:
                return self._catalog.create_table(table, schema=arrow_schema)
            except TableAlreadyExistsError:  # a peer worker won the race
                return self._catalog.load_table(table)

    @staticmethod
    def _conform(arrow, target_schema):
        """Return ``arrow`` aligned to ``target_schema`` (missing cols -> nulls)."""
        import pyarrow as pa

        cols = {name: arrow.column(name) for name in arrow.schema.names}
        out = []
        for field in target_schema:
            col = cols.get(field.name)
            if col is None:
                col = pa.nulls(arrow.num_rows, type=field.type)
            out.append(col)
        return pa.table(out, schema=target_schema)

    def scan(self, table: str) -> list[dict[str, Any]]:
        tbl = self._catalog.load_table(table)
        return tbl.scan().to_arrow().to_pylist()

    def rewrite(self, table: str, rows: list[dict[str, Any]]) -> None:
        # Atomic table replace: one Iceberg snapshot holding the compacted rows
        # (also coalesces small files). Time-travel to prior snapshots still works.
        import pyarrow as pa

        tbl = self._catalog.load_table(table)
        arrow = pa.Table.from_pylist(rows, schema=tbl.schema().as_arrow()) if rows \
            else tbl.scan().to_arrow().schema.empty_table()
        tbl.overwrite(arrow)

    def expire(self, table: str, now: float) -> int:
        # Physical expiry on Iceberg is a compaction/DELETE concern; the scheduled
        # maintenance job handles it. No-op here beyond the reconciled view.
        return 0

    def close(self) -> None:
        self.flush()


def build_warehouse(cfg: IcebergSettings) -> Warehouse:
    if cfg.backend == "iceberg":
        return IcebergWarehouse(cfg)
    return InMemoryWarehouse()
