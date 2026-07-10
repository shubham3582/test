"""Warehouse abstraction: where retained rows land.

``InMemoryWarehouse`` keeps rows per table keyed by doc id (upserts overwrite,
so replays are idempotent) and can expire rows past their retention horizon.
``IcebergWarehouse`` writes to real Iceberg tables via pyiceberg.
"""

from __future__ import annotations

import abc
from typing import Any, Optional

import structlog

from phronexus.config import IcebergSettings

log = structlog.get_logger(__name__)


class Warehouse(abc.ABC):
    @abc.abstractmethod
    def upsert(self, table: str, doc_id: str, row: dict[str, Any]) -> None: ...

    @abc.abstractmethod
    def delete(self, table: str, doc_id: str) -> None: ...

    @abc.abstractmethod
    def scan(self, table: str) -> list[dict[str, Any]]: ...

    def count(self, table: str) -> int:
        return len(self.scan(table))

    def expire(self, table: str, now: float) -> int:  # pragma: no cover - overridden
        return 0

    def close(self) -> None:  # pragma: no cover - trivial default
        pass


class InMemoryWarehouse(Warehouse):
    def __init__(self) -> None:
        self._tables: dict[str, dict[str, dict[str, Any]]] = {}

    def upsert(self, table: str, doc_id: str, row: dict[str, Any]) -> None:
        self._tables.setdefault(table, {})[doc_id] = dict(row)

    def delete(self, table: str, doc_id: str) -> None:
        self._tables.get(table, {}).pop(doc_id, None)

    def scan(self, table: str) -> list[dict[str, Any]]:
        return list(self._tables.get(table, {}).values())

    def expire(self, table: str, now: float) -> int:
        rows = self._tables.get(table, {})
        doomed = [k for k, r in rows.items() if r.get("_expire_at") and r["_expire_at"] <= now]
        for k in doomed:
            del rows[k]
        if doomed:
            log.info("retention.expired", table=table, rows=len(doomed))
        return len(doomed)

    def tables(self) -> list[str]:
        return list(self._tables)


class IcebergWarehouse(Warehouse):  # pragma: no cover - needs catalog + pyarrow
    """Row-per-commit writer to Iceberg via pyiceberg.

    Uses an ``_op`` column so a downstream merge/compaction resolves upserts and
    deletes; this keeps the hot write path append-only and cheap.
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
        self._catalog = load_catalog(cfg.catalog_name, uri=cfg.catalog_uri, warehouse=cfg.warehouse)
        self._buffers: dict[str, list[dict[str, Any]]] = {}

    def upsert(self, table: str, doc_id: str, row: dict[str, Any]) -> None:
        self._buffers.setdefault(table, []).append(row | {"_op": "upsert"})

    def delete(self, table: str, doc_id: str) -> None:
        self._buffers.setdefault(table, []).append({"_doc_id": doc_id, "_op": "delete"})

    def flush(self) -> None:
        import pyarrow as pa

        for table, rows in self._buffers.items():
            if not rows:
                continue
            tbl = self._catalog.load_table(table)
            tbl.append(pa.Table.from_pylist(rows))
        self._buffers.clear()

    def scan(self, table: str) -> list[dict[str, Any]]:
        tbl = self._catalog.load_table(table)
        return tbl.scan().to_arrow().to_pylist()

    def close(self) -> None:
        self.flush()


def build_warehouse(cfg: IcebergSettings) -> Warehouse:
    if cfg.backend == "iceberg":
        return IcebergWarehouse(cfg)
    return InMemoryWarehouse()
