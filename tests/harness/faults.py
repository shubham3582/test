"""Fault-injecting KV wrappers for deterministic crash/partial-write testing."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterator, Optional

from phronexus.kv.base import KVStore, Record, Transaction, TransactionContext
from phronexus.kv.memory import InMemoryKV


class SimulatedCrash(Exception):
    """Raised by :class:`FaultKV` at an armed crash point. Distinct from real
    errors so tests can assert the fault fired (not a genuine bug)."""


@dataclass
class FaultRule:
    """Deterministically fail one operation.

    Matches ``op`` (``"get"``, ``"put"``, ``"remove"``, ``"scan"``,
    ``"batch_get"``, ``"commit"``) optionally scoped to ``set_name``, and fires
    on the ``at_call``-th match (1-based). ``error`` defaults to
    :class:`SimulatedCrash`. Because the write path issues its puts in a fixed
    order (projections → change-feed outbox → manifest), a rule like
    ``FaultRule("put", set_name="trade_manifest")`` is a precise "crash right
    before the commit point" — no production instrumentation required.
    """

    op: str
    set_name: Optional[str] = None
    at_call: int = 1
    error: Optional[Exception] = None
    repeat: bool = False  # if True, fire on EVERY matching call at or after at_call

    def matches(self, op: str, set_name: Optional[str]) -> bool:
        return self.op == op and (self.set_name is None or self.set_name == set_name)

    def fires_at(self, count: int) -> bool:
        return count == self.at_call or (self.repeat and count > self.at_call)


class FaultKV(KVStore):
    """Wrap a :class:`KVStore` and inject deterministic faults.

    Delegates everything to ``inner``; the only behaviour it adds is raising a
    rule's error on the matching call. The transaction handle and ``txn=`` kwarg
    are passed straight through, so faults fire on the *staged* puts too — on an
    atomic backend that aborts the whole transaction (proving atomicity); on
    :class:`SequentialKV` it leaves a torn write (which reads must still hide and
    the reaper must clean).
    """

    def __init__(self, inner: KVStore, rules: Optional[list[FaultRule]] = None) -> None:
        self._inner = inner
        self._rules = list(rules or [])
        self._counts: dict[int, int] = defaultdict(int)

    # --- fault decision -------------------------------------------------

    def arm(self, *rules: FaultRule) -> "FaultKV":
        """Add rules after construction (chainable)."""
        self._rules.extend(rules)
        return self

    def disarm(self) -> None:
        self._rules.clear()
        self._counts.clear()

    def _maybe_fail(self, op: str, set_name: Optional[str] = None) -> None:
        for i, rule in enumerate(self._rules):
            if not rule.matches(op, set_name):
                continue
            self._counts[i] += 1
            if rule.fires_at(self._counts[i]):
                raise rule.error or SimulatedCrash(
                    f"{op} {set_name or ''} (call #{self._counts[i]})".strip()
                )

    # --- KVStore passthrough with fault checks --------------------------

    def get(self, set_name, key, *, txn: Optional[Transaction] = None) -> Optional[Record]:
        self._maybe_fail("get", set_name)
        return self._inner.get(set_name, key, txn=txn)

    def put(self, set_name, key, bins, *, ttl=0, expected_generation=None,
            txn: Optional[Transaction] = None) -> int:
        self._maybe_fail("put", set_name)
        return self._inner.put(set_name, key, bins, ttl=ttl,
                               expected_generation=expected_generation, txn=txn)

    def remove(self, set_name, key, *, expected_generation=None,
               txn: Optional[Transaction] = None) -> None:
        self._maybe_fail("remove", set_name)
        return self._inner.remove(set_name, key,
                                  expected_generation=expected_generation, txn=txn)

    def batch_get(self, set_name, keys, *, txn: Optional[Transaction] = None):
        self._maybe_fail("batch_get", set_name)
        return self._inner.batch_get(set_name, keys, txn=txn)

    def scan(self, set_name) -> Iterator[tuple[str, Record]]:
        self._maybe_fail("scan", set_name)
        yield from self._inner.scan(set_name)

    def transaction(self, *, native: Optional[bool] = None) -> TransactionContext:
        inner_ctx = self._inner.transaction(native=native)
        # Wrap the transaction so "commit" is also a fault point (models a crash
        # at the exact commit call).
        return TransactionContext(txn=_FaultTxn(inner_ctx.txn, self))

    def native_client(self):
        return self._inner.native_client()

    def close(self) -> None:
        self._inner.close()


class _FaultTxn:
    """Wraps the inner transaction to make ``commit`` a fault point. Everything
    else (``stage_put``/``stage_remove``/``staged`` — the backend's staging and
    read-your-writes hooks) is forwarded transparently via ``__getattr__``."""

    def __init__(self, inner: Transaction, kv: "FaultKV") -> None:
        self.inner = inner
        self.kv = kv

    def commit(self) -> None:
        self.kv._maybe_fail("commit")  # noqa: SLF001 - same package
        self.inner.commit()

    def abort(self) -> None:
        self.inner.abort()

    def __getattr__(self, name):  # forward stage_put / stage_remove / staged / ...
        return getattr(self.inner, name)


# ---------------------------------------------------------------------------
# Non-atomic store — models Aerospike Community Edition (no native txn).
# ---------------------------------------------------------------------------


@dataclass
class _SeqTxn:
    """A transaction that cannot isolate or roll back: puts have already been
    applied to the live store, so commit/abort are no-ops. Modelling CE, where
    ``use_native_txn`` is off and each op executes immediately."""

    ops: list = field(default_factory=list)

    def commit(self) -> None:  # already applied
        return None

    def abort(self) -> None:  # cannot undo — this is the whole point
        return None

    def staged(self, set_name, key):
        return None  # no buffering; reads hit the live store


class SequentialKV(InMemoryKV):
    """In-memory store WITHOUT native multi-record transactions.

    Each ``put``/``remove`` inside a ``transaction()`` applies immediately to the
    live store (CAS still enforced), and commit/abort do nothing. A crash
    (injected via :class:`FaultKV`) between two ops therefore leaves a partial,
    torn write — exactly the window the manifest's "write projections first,
    manifest last" ordering is designed to survive, and which the always-atomic
    :class:`InMemoryKV` can never exercise.
    """

    def supports_atomic_txn(self) -> bool:
        return False  # the whole point: no native multi-record transactions

    def transaction(self, *, native: Optional[bool] = None) -> TransactionContext:
        return TransactionContext(txn=_SeqTxn())

    def get(self, set_name, key, *, txn: Optional[Transaction] = None) -> Optional[Record]:
        return super().get(set_name, key, txn=None)  # ignore txn -> live store

    def put(self, set_name, key, bins, *, ttl=0, expected_generation=None,
            txn: Optional[Transaction] = None) -> int:
        return super().put(set_name, key, bins, ttl=ttl,
                           expected_generation=expected_generation, txn=None)

    def remove(self, set_name, key, *, expected_generation=None,
               txn: Optional[Transaction] = None) -> None:
        return super().remove(set_name, key, expected_generation=expected_generation, txn=None)
