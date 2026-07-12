"""Deterministic fault-injection & chaos harness (test-only).

Inserted at the framework's clean ABC + factory seams (KVStore, EventSink,
EventSource, Warehouse, Kafka factories) so faults can be injected without any
change to business logic. Nothing here is imported by the production package.

Building blocks:
- :class:`FaultKV` — wrap any :class:`~phronexus.kv.base.KVStore` and fail a
  chosen operation deterministically (fail-on-Nth-op, per set, or a named crash
  point raising :class:`SimulatedCrash`).
- :class:`SequentialKV` — an in-memory store WITHOUT native multi-record
  transactions, modelling Aerospike Community Edition: each op applies
  immediately and commit/abort cannot roll back. Combined with FaultKV this
  reproduces torn (partial) multi-record writes that the always-atomic
  :class:`~phronexus.kv.memory.InMemoryKV` cannot.
"""

from __future__ import annotations

from tests.harness.build import phronexus_with
from tests.harness.faults import (
    FaultKV,
    FaultRule,
    SequentialKV,
    SimulatedCrash,
)

__all__ = [
    "FaultKV",
    "FaultRule",
    "SequentialKV",
    "SimulatedCrash",
    "phronexus_with",
]
