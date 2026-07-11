from __future__ import annotations

import pytest

from phronexus.errors import QueryError


@pytest.fixture()
def loaded(px):
    for i, notional in enumerate([500.0, 100.0, 400.0, 200.0, 300.0]):
        px.put("trade", {"trade_id": f"T{i}", "counterparty": "GS",
                         "notional": notional, "ccy": "USD", "trade_date": 20250100 + i})
    return px


def _q(loaded, **kw):
    base = {"entity": "trade", "where": [{"field": "counterparty", "op": "eq", "value": "GS"}]}
    return loaded.query_page({**base, **kw})


def test_pagination_walks_all(loaded):
    p1 = _q(loaded, limit=2, offset=0, sort=[{"field": "notional", "order": "asc"}])
    assert p1["count"] == 2 and p1["has_more"] is True
    assert [d["notional"] for d in p1["documents"]] == [100.0, 200.0]

    p2 = _q(loaded, limit=2, offset=2, sort=[{"field": "notional", "order": "asc"}])
    assert [d["notional"] for d in p2["documents"]] == [300.0, 400.0]

    p3 = _q(loaded, limit=2, offset=4, sort=[{"field": "notional", "order": "asc"}])
    assert p3["count"] == 1 and p3["has_more"] is False
    assert p3["documents"][0]["notional"] == 500.0


def test_sort_desc(loaded):
    p = _q(loaded, limit=3, sort=[{"field": "notional", "order": "desc"}])
    assert [d["notional"] for d in p["documents"]] == [500.0, 400.0, 300.0]


def test_sort_on_non_searchable_field_rejected(loaded):
    with pytest.raises(QueryError):
        _q(loaded, sort=[{"field": "book", "order": "asc"}])


def test_default_query_still_returns_list(loaded):
    docs = loaded.query({"entity": "trade", "where": [{"field": "counterparty", "op": "eq", "value": "GS"}]})
    assert isinstance(docs, list) and len(docs) == 5
