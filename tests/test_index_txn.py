from __future__ import annotations

from phronexus import Phronexus, Settings

TRADES = [
    {"trade_id": "T1", "counterparty": "GS", "notional": 100.0, "ccy": "USD", "trade_date": 20250101},
    {"trade_id": "T2", "counterparty": "GS", "notional": 200.0, "ccy": "EUR", "trade_date": 20250115},
]


def _px(in_txn: bool) -> Phronexus:
    s = Settings(backend="memory")
    s.observability.log_level = "ERROR"
    s.index.in_txn = in_txn
    p = Phronexus(s)
    p.load_contract_dir("contracts_examples")
    return p


def _query_gs(px):
    return {r["trade_id"] for r in px.query(
        {"entity": "trade", "where": [{"field": "counterparty", "op": "eq", "value": "GS"}]})}


def test_index_written_in_txn_survives_crash_after_commit(monkeypatch):
    """A crash right after commit (post_write never runs) must NOT lose the index."""
    px = _px(in_txn=True)
    # Simulate the crash window: post-commit side effects don't run.
    monkeypatch.setattr(px.manifest, "post_write", lambda *a, **k: None)
    px.put("trade", TRADES[0])
    # Committed AND searchable, because the index was written inside the txn.
    assert px.get("trade", "T1") == TRADES[0]
    assert _query_gs(px) == {"T1"}
    px.close()


def test_post_commit_mode_would_miss_on_crash(monkeypatch):
    """Contrast: with in_txn=False the same crash window loses the index entry."""
    px = _px(in_txn=False)
    monkeypatch.setattr(px.manifest, "post_write", lambda *a, **k: None)
    px.put("trade", TRADES[0])
    assert px.get("trade", "T1") == TRADES[0]     # still committed / readable by PK
    assert _query_gs(px) == set()                 # but missing from search — the bug
    px.close()


def test_in_txn_normal_query_and_update_and_delete():
    px = _px(in_txn=True)
    for t in TRADES:
        px.put("trade", t)
    assert _query_gs(px) == {"T1", "T2"}
    # update moves T1 off GS -> old posting removed atomically
    px.put("trade", {**TRADES[0], "counterparty": "JPM"})
    assert _query_gs(px) == {"T2"}
    # delete removes remaining posting
    px.delete("trade", "T2")
    assert _query_gs(px) == set()
    px.close()


def test_range_index_terms_staged_in_txn():
    px = _px(in_txn=True)
    for t in TRADES:
        px.put("trade", t)
    res = px.query({"entity": "trade", "where": [{"field": "trade_date", "op": "gte", "value": 20250110}]})
    assert {r["trade_id"] for r in res} == {"T2"}
    px.close()
