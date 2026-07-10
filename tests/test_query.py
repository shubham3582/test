from __future__ import annotations

import pytest

from phronexus.errors import QueryError


@pytest.fixture()
def loaded(px):
    trades = [
        {"trade_id": "T1", "counterparty": "GS", "notional": 100.0, "ccy": "USD", "trade_date": 20250101},
        {"trade_id": "T2", "counterparty": "GS", "notional": 200.0, "ccy": "EUR", "trade_date": 20250115},
        {"trade_id": "T3", "counterparty": "JPM", "notional": 300.0, "ccy": "USD", "trade_date": 20250201},
    ]
    for t in trades:
        px.put("trade", t)
    return px


def test_query_eq(loaded):
    res = loaded.query({"entity": "trade", "where": [{"field": "counterparty", "op": "eq", "value": "GS"}]})
    assert {r["trade_id"] for r in res} == {"T1", "T2"}


def test_query_and_of_predicates(loaded):
    res = loaded.query({
        "entity": "trade",
        "where": [
            {"field": "counterparty", "op": "eq", "value": "GS"},
            {"field": "ccy", "op": "eq", "value": "USD"},
        ],
    })
    assert {r["trade_id"] for r in res} == {"T1"}


def test_query_numeric_range(loaded):
    res = loaded.query({"entity": "trade", "where": [{"field": "trade_date", "op": "gte", "value": 20250115}]})
    assert {r["trade_id"] for r in res} == {"T2", "T3"}


def test_query_in(loaded):
    res = loaded.query({"entity": "trade", "where": [{"field": "ccy", "op": "in", "value": ["EUR"]}]})
    assert {r["trade_id"] for r in res} == {"T2"}


def test_ne_needs_a_positive_seed(loaded):
    with pytest.raises(QueryError):
        loaded.query({"entity": "trade", "where": [{"field": "ccy", "op": "ne", "value": "USD"}]})


def test_range_on_string_field_rejected(loaded):
    with pytest.raises(QueryError):
        loaded.query({"entity": "trade", "where": [{"field": "counterparty", "op": "gte", "value": "A"}]})


def test_non_searchable_field_rejected(loaded):
    with pytest.raises(QueryError):
        loaded.query({"entity": "trade", "where": [{"field": "book", "op": "eq", "value": "X"}]})


def test_named_pattern_with_params(loaded):
    res = loaded.query_pattern("trade", "cpty_since", cpty="GS", since=20250110)
    assert {r["trade_id"] for r in res} == {"T2"}


def test_index_updates_on_change(loaded):
    # Move T1 from GS to JPM; it should leave the GS posting list.
    loaded.put("trade", {"trade_id": "T1", "counterparty": "JPM", "notional": 100.0, "ccy": "USD", "trade_date": 20250101})
    gs = loaded.query({"entity": "trade", "where": [{"field": "counterparty", "op": "eq", "value": "GS"}]})
    assert {r["trade_id"] for r in gs} == {"T2"}


def test_deleted_doc_leaves_index(loaded):
    loaded.delete("trade", "T2")
    res = loaded.query({"entity": "trade", "where": [{"field": "counterparty", "op": "eq", "value": "GS"}]})
    assert {r["trade_id"] for r in res} == {"T1"}
