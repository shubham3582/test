"""G5 — hard-delete then re-insert leaves the document visible again.

The bug: a hard delete removed the manifest, so a re-insert restarted at
version 1 while the delete tombstone carried a higher version. The retention
view keeps the max-version row per doc, so the tombstone SHADOWED the re-insert
and the document stayed invisible in Iceberg. F3 keeps a manifest tombstone so
versions stay monotonic (delete < re-insert).
"""

from __future__ import annotations

import pytest

from phronexus.retention.warehouse import reconcile

FX = {"deal_id": "FX-G5", "pair": "EURUSD", "notional": 1e6, "rate": 1.08, "trade_date": 20250115}


def _versions_by_op(px, doc_id):
    out: dict[str, list[int]] = {}
    for e in px.sink.events:
        if e.doc_id == doc_id:
            out.setdefault(e.op, []).append(e.version)
    return out


@pytest.mark.chaos
def test_hard_delete_then_reinsert_visible_and_monotonic(px):
    px.put("fx_spot", FX)                          # v1 upsert
    px.delete("fx_spot", "FX-G5")                  # tombstone
    px.put("fx_spot", {**FX, "rate": 1.10})        # re-insert

    # Hot tier: the re-inserted document is visible again.
    got = px.get("fx_spot", "FX-G5")
    assert got is not None and got["rate"] == 1.10

    # The re-insert's version strictly exceeds the tombstone's — the property the
    # retention reconcile relies on (max version per doc wins).
    v = _versions_by_op(px, "FX-G5")
    del_v, reins_v = max(v["delete"]), max(v["upsert"])
    assert reins_v > del_v, f"re-insert v{reins_v} must beat tombstone v{del_v}"

    # End-to-end: with those versions the reconciled retention view shows the
    # re-inserted document, not the tombstone.
    rows = [
        {"_doc_id": "FX-G5", "_txn": "a", "_version": min(v["upsert"]), "_op": "upsert", "rate": 1.08},
        {"_doc_id": "FX-G5", "_txn": "b", "_version": del_v, "_op": "delete"},
        {"_doc_id": "FX-G5", "_txn": "c", "_version": reins_v, "_op": "upsert", "rate": 1.10},
    ]
    state = reconcile(rows)
    assert len(state) == 1 and state[0]["rate"] == 1.10
