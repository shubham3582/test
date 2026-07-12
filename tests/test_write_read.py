from __future__ import annotations

import pytest

from phronexus.errors import DocumentAlreadyExists, DocumentNotFound
from phronexus.manifest.manager import M_STATUS, STATUS_COMMITTED
from phronexus.storage.projection import DOC_BIN, META_TXN


def test_write_then_read_roundtrip(px, sample_trade):
    doc_id = px.put("trade", sample_trade)
    assert doc_id == "T-1001"
    got = px.get("trade", "T-1001")
    assert got == sample_trade


def test_projections_are_written_across_sets(px, sample_trade):
    px.put("trade", sample_trade)
    main = px.store.get("trade_main", "T-1001")
    by_cpty = px.store.get("trade_by_cpty", "GS:T-1001")
    assert main is not None and by_cpty is not None
    # Partial projection only carries the contracted field subset.
    assert set(by_cpty.bins[DOC_BIN]) == {"trade_id", "counterparty", "notional", "ccy", "trade_date"}
    assert "book" not in by_cpty.bins[DOC_BIN]


def test_document_invisible_without_committed_manifest(px, sample_trade):
    # Simulate a crashed write: projection present, no manifest.
    px.store.put("trade_main", "T-9", {DOC_BIN: sample_trade, META_TXN: "orphan"})
    assert px.get("trade", "T-9") is None


def test_manifest_marks_committed(px, sample_trade):
    px.put("trade", sample_trade)
    manifest = px.store.get("trade_manifest", "T-1001")
    assert manifest.bins[M_STATUS] == STATUS_COMMITTED


def test_update_supersedes_old_projection(px, sample_trade):
    px.put("trade", sample_trade)
    updated = dict(sample_trade, counterparty="JPM")
    px.put("trade", updated)
    assert px.get("trade", "T-1001")["counterparty"] == "JPM"
    # Old counterparty-keyed projection reaped, new one present.
    assert px.store.get("trade_by_cpty", "GS:T-1001") is None
    assert px.store.get("trade_by_cpty", "JPM:T-1001") is not None


def test_insert_only_policy_rejects_overwrite(px):
    repo = {
        "repo_id": "R-1", "counterparty": "MS", "principal": 5_000_000,
        "rate": 0.031, "maturity_date": 20250601,
    }
    px.put("repo", repo)
    with pytest.raises(DocumentAlreadyExists):
        px.put("repo", dict(repo, principal=6_000_000))


def test_soft_delete_hides_document(px, sample_trade):
    px.put("trade", sample_trade)
    assert px.delete("trade", "T-1001") is True
    assert px.get("trade", "T-1001") is None
    # Soft delete keeps a tombstone manifest.
    assert px.store.get("trade_manifest", "T-1001") is not None


def test_hard_delete_removes_records(px):
    fx = {"deal_id": "FX-1", "pair": "EURUSD", "notional": 1e6, "rate": 1.08, "trade_date": 20250115}
    px.put("fx_spot", fx)
    assert px.delete("fx_spot", "FX-1") is True
    # The document DATA is physically gone ...
    assert px.store.get("fx_main", "FX-1") is None
    assert px.get("fx_spot", "FX-1") is None
    # ... but a minimal manifest tombstone remains (delete marker) so versions
    # stay monotonic across a re-insert and the change feed keeps a delete row.
    tomb = px.store.get("fx_manifest", "FX-1")
    assert tomb is not None and tomb.bins["status"] == "deleted"
    assert tomb.bins["projections"] == []


def test_delete_missing_raises(px):
    with pytest.raises(DocumentNotFound):
        px.delete("trade", "nope")


def test_commit_event_emitted(px, sample_trade):
    px.put("trade", sample_trade)
    events = px.sink.events
    assert events and events[-1].entity == "trade" and events[-1].op == "upsert"
