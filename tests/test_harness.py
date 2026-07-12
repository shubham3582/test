"""Self-tests for the fault-injection harness itself.

These also demonstrate the central G1 contrast: the same injected crash right
before the manifest commit point leaves NOTHING on an atomic backend (clean
abort) but a hidden orphan on a non-transactional (CE) backend — which reads
must never surface and the reaper must later clean.
"""

from __future__ import annotations

import pytest

from phronexus.kv.memory import InMemoryKV
from tests.harness import FaultKV, FaultRule, SequentialKV, SimulatedCrash, phronexus_with

TRADE = {
    "trade_id": "T-HARNESS-1",
    "counterparty": "GS",
    "notional": 1_000_000.0,
    "ccy": "USD",
    "trade_date": 20250115,
    "book": "RATES-1",
}


def test_faultkv_fires_on_nth_matching_put():
    kv = FaultKV(InMemoryKV())
    kv.arm(FaultRule("put", set_name="s", at_call=2))
    kv.put("s", "a", {"x": 1})  # 1st matching call — fine
    with pytest.raises(SimulatedCrash):
        kv.put("s", "b", {"x": 2})  # 2nd — boom
    # a different set is unaffected
    kv.put("other", "c", {"x": 3})


def test_faultkv_scoped_to_set():
    kv = FaultKV(InMemoryKV())
    kv.arm(FaultRule("put", set_name="manifest"))
    kv.put("projection", "k", {"x": 1})  # different set, no fault
    with pytest.raises(SimulatedCrash):
        kv.put("manifest", "k", {"x": 1})


def test_atomic_backend_crash_before_manifest_leaves_nothing():
    """InMemoryKV is atomic: a crash at the commit point aborts the whole txn."""
    fault = FaultKV(InMemoryKV())
    px = phronexus_with(fault)
    sc = px.registry.active_storage("trade")
    fault.arm(FaultRule("put", set_name=sc.manifest_set))  # crash right before manifest

    with pytest.raises(SimulatedCrash):
        px.put("trade", TRADE)

    doc_id = px.manifest._proj.compute_doc_id(sc, TRADE)  # noqa: SLF001
    assert px.get("trade", doc_id) is None                      # invisible
    # atomic abort => not even an orphan projection survives
    canonical_set = sc.canonical_projection.set
    assert list(fault.scan(canonical_set)) == []
    px.close()


def test_sequential_backend_crash_leaves_hidden_orphan():
    """SequentialKV models Aerospike CE: the same crash leaves a torn write —
    projections written, manifest absent. The document must stay invisible."""
    fault = FaultKV(SequentialKV())
    px = phronexus_with(fault)
    sc = px.registry.active_storage("trade")
    doc_id = px.manifest._proj.compute_doc_id(sc, TRADE)  # noqa: SLF001
    fault.arm(FaultRule("put", set_name=sc.manifest_set))  # crash right before manifest

    with pytest.raises(SimulatedCrash):
        px.put("trade", TRADE)

    # torn write: canonical projection physically present ...
    canonical_set = sc.canonical_projection.set
    assert any(True for _ in fault.scan(canonical_set)), "expected an orphan projection"
    # ... but the document is NOT visible (no committed manifest)
    assert px.get("trade", doc_id) is None
    px.close()
