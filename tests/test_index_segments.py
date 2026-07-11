"""Segmented posting lists: rollover correctness + bounded-append behaviour."""

from __future__ import annotations

import pytest

from phronexus import Phronexus, Settings


@pytest.fixture()
def px() -> Phronexus:
    settings = Settings(backend="memory")
    settings.observability.log_level = "WARNING"
    settings.index.segment_size = 4          # tiny cap → forces rollover quickly
    p = Phronexus(settings)
    p.load_contract_dir("contracts_examples")
    yield p
    p.close()


def _trade(i: int, cpty: str = "GS") -> dict:
    return {"trade_id": f"T{i}", "counterparty": cpty, "notional": 1_000_000.0 + i,
            "ccy": "USD", "trade_date": 20250101 + i, "book": "R"}


def test_hot_term_spans_segments_and_lookup_unions_all(px):
    # 10 docs share counterparty=GS with segment_size=4 → head + 2 sealed segments.
    for i in range(10):
        px.put("trade", _trade(i, "GS"))
    ids = px.index.lookup_eq("trade", "counterparty", "GS")
    assert ids == {f"T{i}" for i in range(10)}          # nothing lost across segments

    # The head record proves rollover happened (segs > 0).
    from phronexus.query.inverted import _posting_key
    head = px.store.get(px.settings.aerospike.index_set, _posting_key("trade", "counterparty", "GS"))
    assert head.bins["segs"] >= 2 and len(head.bins["docs"]) <= 4


def test_remove_finds_doc_in_a_sealed_segment(px):
    for i in range(10):
        px.put("trade", _trade(i, "GS"))
    # T0 lives in the first sealed segment; delete must still remove it from search.
    px.delete("trade", "T0")
    ids = px.index.lookup_eq("trade", "counterparty", "GS")
    assert "T0" not in ids and len(ids) == 9


def test_query_returns_all_across_segments(px):
    for i in range(10):
        px.put("trade", _trade(i, "GS"))
    rows = px.query({"entity": "trade",
                     "where": [{"field": "counterparty", "op": "eq", "value": "GS"}],
                     "limit": 100})
    assert len(rows) == 10


def test_numeric_term_dictionary_spans_segments_for_range(px):
    # trade_date is a numeric searchable field; 10 distinct values with cap=4
    # → the term dictionary rolls over into segments too. Range must see them all.
    for i in range(10):
        px.put("trade", _trade(i, "GS"))
    terms = px.index.all_terms("trade", "trade_date")
    assert len(terms) == 10                              # every distinct value retained
    rows = px.query({"entity": "trade",
                     "where": [{"field": "trade_date", "op": "gte", "value": 20250105}],
                     "limit": 100})
    assert {r["trade_id"] for r in rows} == {f"T{i}" for i in range(4, 10)}


def test_put_many_matches_put(px):
    ids = px.put_many("trade", [_trade(i, "MS") for i in range(10)])
    assert len(ids) == 10
    assert px.index.lookup_eq("trade", "counterparty", "MS") == {f"T{i}" for i in range(10)}
    assert px.get("trade", "T3")["counterparty"] == "MS"
