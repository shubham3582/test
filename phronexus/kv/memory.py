"""In-process KV backend.

Faithful enough to exercise the full framework in tests and demos: it enforces
generation-based CAS, honours TTL lazily, and implements real transaction
buffering (staged writes applied atomically under a lock, validated against the
committed store at commit time — so concurrent CAS conflicts surface exactly as
they would on a real cluster).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional

from phronexus.errors import GenerationConflict, StorageError
from phronexus.kv.base import KVStore, Record, Transaction, TransactionContext


@dataclass
class _Cell:
    bins: dict[str, Any]
    generation: int
    expire_at: float | None  # monotonic deadline, or None


_TOMBSTONE = object()


@dataclass
class _MemTxn:
    store: "InMemoryKV"
    # staged ops keyed by (set, key): value is bins-dict or _TOMBSTONE
    ops: dict[tuple[str, str], Any] = field(default_factory=dict)
    expected: dict[tuple[str, str], int] = field(default_factory=dict)
    ttls: dict[tuple[str, str], int] = field(default_factory=dict)
    done: bool = False

    def stage_put(self, set_name, key, bins, ttl, expected_generation):
        k = (set_name, key)
        self.ops[k] = dict(bins)
        self.ttls[k] = ttl
        if expected_generation is not None:
            self.expected[k] = expected_generation

    def stage_remove(self, set_name, key, expected_generation):
        k = (set_name, key)
        self.ops[k] = _TOMBSTONE
        if expected_generation is not None:
            self.expected[k] = expected_generation

    def staged(self, set_name, key):
        return self.ops.get((set_name, key), None)

    def commit(self) -> None:
        if self.done:
            return
        with self.store._lock:  # noqa: SLF001 - same module
            # Validate all CAS expectations against the committed store first.
            for (set_name, key), exp in self.expected.items():
                cur = self.store._live_generation(set_name, key)
                if cur != exp:
                    raise GenerationConflict(
                        f"generation conflict on {set_name}/{key}: "
                        f"expected {exp}, found {cur}"
                    )
            # Apply atomically.
            for (set_name, key), val in self.ops.items():
                if val is _TOMBSTONE:
                    self.store._raw_remove(set_name, key)
                else:
                    self.store._raw_put(set_name, key, val, self.ttls[(set_name, key)])
        self.done = True

    def abort(self) -> None:
        self.done = True
        self.ops.clear()


class InMemoryKV(KVStore):
    def __init__(self) -> None:
        self._data: dict[tuple[str, str], _Cell] = {}
        self._lock = threading.RLock()

    # --- internal helpers (call with lock held) -------------------------

    def _live(self, set_name: str, key: str) -> Optional[_Cell]:
        cell = self._data.get((set_name, key))
        if cell is None:
            return None
        if cell.expire_at is not None and cell.expire_at <= time.monotonic():
            del self._data[(set_name, key)]
            return None
        return cell

    def _live_generation(self, set_name: str, key: str) -> int:
        cell = self._live(set_name, key)
        return cell.generation if cell else 0

    def _raw_put(self, set_name, key, bins, ttl) -> int:
        cell = self._live(set_name, key)
        gen = (cell.generation if cell else 0) + 1
        expire = time.monotonic() + ttl if ttl and ttl > 0 else None
        self._data[(set_name, key)] = _Cell(bins=dict(bins), generation=gen, expire_at=expire)
        return gen

    def _raw_remove(self, set_name, key) -> None:
        self._data.pop((set_name, key), None)

    # --- KVStore API ----------------------------------------------------

    def get(self, set_name, key, *, txn: Optional[Transaction] = None) -> Optional[Record]:
        if txn is not None:
            staged = txn.staged(set_name, key)  # type: ignore[attr-defined]
            if staged is _TOMBSTONE:
                return None
            if staged is not None:
                return Record(bins=dict(staged), generation=-1, ttl=0)
        with self._lock:
            cell = self._live(set_name, key)
            if cell is None:
                return None
            return Record(bins=dict(cell.bins), generation=cell.generation, ttl=0)

    def put(
        self,
        set_name,
        key,
        bins,
        *,
        ttl=0,
        expected_generation=None,
        txn: Optional[Transaction] = None,
    ) -> int:
        if txn is not None:
            txn.stage_put(set_name, key, bins, ttl, expected_generation)  # type: ignore[attr-defined]
            return -1
        with self._lock:
            if expected_generation is not None:
                cur = self._live_generation(set_name, key)
                if cur != expected_generation:
                    raise GenerationConflict(
                        f"generation conflict on {set_name}/{key}: "
                        f"expected {expected_generation}, found {cur}"
                    )
            return self._raw_put(set_name, key, bins, ttl)

    def remove(self, set_name, key, *, expected_generation=None, txn: Optional[Transaction] = None):
        if txn is not None:
            txn.stage_remove(set_name, key, expected_generation)  # type: ignore[attr-defined]
            return
        with self._lock:
            if expected_generation is not None:
                cur = self._live_generation(set_name, key)
                if cur != expected_generation:
                    raise GenerationConflict(
                        f"generation conflict on {set_name}/{key}: "
                        f"expected {expected_generation}, found {cur}"
                    )
            self._raw_remove(set_name, key)

    def scan(self, set_name) -> Iterator[tuple[str, Record]]:
        with self._lock:
            keys = [k for (s, k) in self._data if s == set_name]
        for key in keys:
            with self._lock:
                cell = self._live(set_name, key)
            if cell is not None:
                yield key, Record(bins=dict(cell.bins), generation=cell.generation, ttl=0)

    def transaction(self) -> TransactionContext:
        return TransactionContext(txn=_MemTxn(store=self))

    def flush(self) -> None:
        """Test helper: drop everything."""
        with self._lock:
            self._data.clear()
