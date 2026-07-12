"""G2 — the reaper never deletes live data.

It removes a projection record only when no committed manifest references it AND
it is older than the grace period. A young, still-in-flight write (projections
written, manifest not yet committed) must be spared; a genuinely orphaned one
(crash long ago) must be swept; a committed document must never be touched.
"""

from __future__ import annotations

import pytest

from phronexus.storage.projection import META_TS
from tests.harness import FaultKV, FaultRule, SequentialKV, SimulatedCrash, phronexus_with

TRADE = {
    "trade_id": "T-REAP-1",
    "counterparty": "GS",
    "notional": 1_000_000.0,
    "ccy": "USD",
    "trade_date": 20250115,
    "book": "RATES-1",
}


@pytest.mark.chaos
def test_reaper_spares_inflight_write_then_sweeps_real_orphan():
    fault = FaultKV(SequentialKV())  # CE-like: no native txn -> torn writes possible
    px = phronexus_with(fault)
    sc = px.registry.active_storage("trade")
    canon = sc.canonical_projection.set
    grace = px.settings.reaper.orphan_grace_seconds

    # A torn write: projections land, the manifest commit point never does.
    fault.arm(FaultRule("put", set_name=sc.manifest_set))
    with pytest.raises(SimulatedCrash):
        px.put("trade", TRADE)

    orphans = list(fault.scan(canon))
    assert orphans, "torn write should leave an orphan projection"
    ts = orphans[0][1].bins[META_TS]
    n_orphans = sum(len(list(fault.scan(p.set))) for p in sc.projections)

    # Within the grace period this looks exactly like an in-flight write — spare it.
    assert px.reaper.sweep_entity("trade", now=ts + grace - 1) == 0
    assert list(fault.scan(canon)), "must NOT reap a within-grace record"

    # Past the grace period the orphaned projections (one per projection) are swept.
    assert px.reaper.sweep_entity("trade", now=ts + grace + 1) == n_orphans
    assert all(list(fault.scan(p.set)) == [] for p in sc.projections)
    px.close()


@pytest.mark.chaos
def test_reaper_never_touches_committed_document():
    px = phronexus_with(SequentialKV())
    doc_id = px.put("trade", TRADE)  # cleanly committed

    # Even arbitrarily far past the grace period, referenced projections survive.
    assert px.reaper.sweep_entity("trade", now=1e12) == 0
    got = px.get("trade", doc_id)
    assert got is not None and got["trade_id"] == "T-REAP-1"
    px.close()
