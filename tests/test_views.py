from __future__ import annotations


def test_risk_view_projects_and_transforms(px, sample_trade):
    px.put("trade", sample_trade)
    v = px.view("trade", "risk_consumer", "T-1001")
    assert set(v) == {"trade_id", "counterparty", "notional", "ccy", "trade_date"}
    assert "book" not in v                    # not in the view allow-list
    assert v["notional"] == 1_000_000.01       # round2 transform applied


def test_public_view_masks_counterparty(px, sample_trade):
    px.put("trade", sample_trade)
    v = px.view("trade", "public", "T-1001")
    assert v["counterparty"] == "****"
    assert v["trade_id"] == "T-1001"


def test_query_view_combines(px):
    for t in [
        {"trade_id": "A", "counterparty": "GS", "notional": 1.239, "ccy": "USD", "trade_date": 20250101},
        {"trade_id": "B", "counterparty": "GS", "notional": 2.5, "ccy": "USD", "trade_date": 20250102},
    ]:
        px.put("trade", t)
    rows = px.query_view("trade", "public", {"entity": "trade", "where": [{"field": "counterparty", "op": "eq", "value": "GS"}]})
    assert len(rows) == 2
    assert all(r["counterparty"] == "****" for r in rows)


def test_view_missing_doc_returns_none(px):
    assert px.view("trade", "public", "missing") is None
