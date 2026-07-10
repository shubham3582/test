from __future__ import annotations

from phronexus.storage.projection import DOC_BIN, META_TXN


def test_reaper_removes_orphans_not_committed_docs(px, sample_trade):
    px.put("trade", sample_trade)  # committed — must survive
    # Orphan projection from a crashed write (no manifest references it).
    px.store.put("trade_main", "ORPHAN", {DOC_BIN: {"x": 1}, META_TXN: "dead"})

    removed = px.reaper.sweep_entity("trade")
    assert removed == 1
    assert px.store.get("trade_main", "ORPHAN") is None
    assert px.get("trade", "T-1001") == sample_trade
