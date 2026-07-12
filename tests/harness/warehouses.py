"""Warehouse test doubles for retention chaos tests."""

from __future__ import annotations

from typing import Any

from phronexus.retention.warehouse import Warehouse, _row_key, dedup_new


class PhysicalWarehouse(Warehouse):
    """A warehouse whose rows live in a flat physical log — like Iceberg appends,
    and unlike :class:`InMemoryWarehouse` (which is idem-keyed and therefore
    idempotent for free).

    ``append`` buffers by ``idem_key`` (within-batch dedup); ``flush`` anti-joins
    on ``(_doc_id,_txn)`` against the existing physical rows before extending the
    log — mirroring the fixed ``IcebergWarehouse.flush``. Set ``dedup=False`` to
    reproduce the pre-fix behaviour (raw append) for red/green contrast.
    """

    def __init__(self, *, dedup: bool = True) -> None:
        self._log: dict[str, list[dict[str, Any]]] = {}
        self._buffers: dict[str, dict[str, dict[str, Any]]] = {}
        self._dedup = dedup

    def append(self, table: str, idem_key: str, row: dict[str, Any]) -> None:
        self._buffers.setdefault(table, {})[idem_key] = dict(row)

    def flush(self) -> None:
        for table, buffered in self._buffers.items():
            rows = list(buffered.values())
            log = self._log.setdefault(table, [])
            if self._dedup:
                rows = dedup_new(rows, {_row_key(r) for r in log})
            log.extend(rows)
        self._buffers.clear()

    def scan(self, table: str) -> list[dict[str, Any]]:
        return list(self._log.get(table, []))

    def rewrite(self, table: str, rows: list[dict[str, Any]]) -> None:
        self._log[table] = [dict(r) for r in rows]
