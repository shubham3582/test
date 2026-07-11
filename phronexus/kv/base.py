"""KVStore protocol shared by every backend.

The abstraction is deliberately small — just enough to express the manifest
write pattern, the inverted index, and consistent reads — so that the same core
logic runs unchanged on the in-memory backend (tests/demos) and on Aerospike.

Records carry a ``generation`` (monotonic per-record version) which powers
optimistic concurrency: pass ``expected_generation`` to ``put``/``remove`` and a
mismatch raises :class:`phronexus.errors.GenerationConflict`.

Multi-record atomicity is expressed through :meth:`KVStore.transaction`. On
Aerospike 8.0+ this maps to a native multi-record transaction; on the in-memory
backend it is a buffered, all-or-nothing apply. Callers thread the returned
:class:`Transaction` handle into ``put``/``get``/``remove``.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional, Protocol


@dataclass(frozen=True)
class Record:
    """A stored record plus its metadata."""

    bins: dict[str, Any]
    generation: int
    ttl: int = 0  # seconds; 0 = no expiry


class Transaction(Protocol):
    """Opaque handle representing an in-flight multi-record transaction."""

    def commit(self) -> None: ...
    def abort(self) -> None: ...


# Comparison operators understood by the index/query layers.
OPS = frozenset({"eq", "ne", "in", "gt", "gte", "lt", "lte"})


class KVStore(abc.ABC):
    """Backend-agnostic key-value store."""

    # --- single record --------------------------------------------------

    @abc.abstractmethod
    def get(
        self, set_name: str, key: str, *, txn: Optional[Transaction] = None
    ) -> Optional[Record]:
        ...

    @abc.abstractmethod
    def put(
        self,
        set_name: str,
        key: str,
        bins: dict[str, Any],
        *,
        ttl: int = 0,
        expected_generation: Optional[int] = None,
        txn: Optional[Transaction] = None,
    ) -> int:
        """Write ``bins``; returns the new generation.

        ``expected_generation``: if not None, the current generation must match
        (0 means "must not already exist") or :class:`GenerationConflict` is
        raised.
        """

    @abc.abstractmethod
    def remove(
        self,
        set_name: str,
        key: str,
        *,
        expected_generation: Optional[int] = None,
        txn: Optional[Transaction] = None,
    ) -> None:
        ...

    # --- bulk -----------------------------------------------------------

    def batch_get(
        self, set_name: str, keys: list[str], *, txn: Optional[Transaction] = None
    ) -> dict[str, Record]:
        out: dict[str, Record] = {}
        for k in keys:
            rec = self.get(set_name, k, txn=txn)
            if rec is not None:
                out[k] = rec
        return out

    @abc.abstractmethod
    def scan(self, set_name: str) -> Iterator[tuple[str, Record]]:
        """Iterate every live record in a set. Used by the reaper / rebuilds."""

    # --- transactions ---------------------------------------------------

    @abc.abstractmethod
    def transaction(self) -> "TransactionContext":
        """Return a context manager yielding a :class:`Transaction`."""

    def native_client(self) -> Any:
        """Return the backend's native client handle, if it has one.

        Backends without a native driver (e.g. the in-memory store) raise
        :class:`NotImplementedError`. Aerospike returns its connected client.
        """
        raise NotImplementedError(
            f"{type(self).__name__} has no native client (backend is not Aerospike)"
        )

    def close(self) -> None:  # pragma: no cover - trivial default
        pass


@dataclass
class TransactionContext:
    """Context manager wrapper: commits on clean exit, aborts on exception."""

    txn: Transaction
    _entered: bool = field(default=False, repr=False)

    def __enter__(self) -> Transaction:
        self._entered = True
        return self.txn

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            self.txn.commit()
        else:
            self.txn.abort()
        return False  # never suppress
