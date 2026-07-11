from __future__ import annotations

import pytest

from phronexus.errors import ValidationError

TRADE = {"trade_id": "T-1", "counterparty": "GS", "notional": 1e6, "ccy": "USD", "trade_date": 20250115, "book": "R"}


def _fx(deal_id, **extra):
    return {"deal_id": deal_id, "pair": "EURUSD", "notional": 1e6, "rate": 1.08,
            "trade_date": 20250115, **extra}


def test_referential_check(px):
    px.publish_contract({
        "kind": "validation", "entity": "fx_spot", "version": 1, "mode": "enforce",
        "dq_checks": [{"name": "trade_ref_exists", "field": "trade_ref", "references": "trade"}],
    })
    px.put("trade", TRADE)                              # referenced doc exists
    px.put("fx_spot", _fx("F1", trade_ref="T-1"))       # ok
    with pytest.raises(ValidationError) as e:
        px.put("fx_spot", _fx("F2", trade_ref="NOPE"))  # dangling reference
    assert "references no existing trade" in str(e.value)
    assert px.get("fx_spot", "F2") is None


def test_uniqueness_check_scan_path(px):
    # ext_id is not searchable -> uniqueness uses the scan fallback.
    px.publish_contract({
        "kind": "validation", "entity": "fx_spot", "version": 1, "mode": "enforce",
        "dq_checks": [{"name": "ext_unique", "field": "ext_id", "unique": True}],
    })
    px.put("fx_spot", _fx("F1", ext_id="X1"))
    with pytest.raises(ValidationError) as e:
        px.put("fx_spot", _fx("F2", ext_id="X1"))       # duplicate ext_id
    assert "must be unique" in str(e.value)
    # Re-writing the SAME document keeps its own value (self excluded).
    px.put("fx_spot", _fx("F1", ext_id="X1", notional=2e6))
    assert px.get("fx_spot", "F1")["notional"] == 2e6


def test_uniqueness_check_index_path(px):
    # Make ext_id searchable so uniqueness uses the inverted-index fast path.
    px.publish_contract({
        "kind": "query", "entity": "fx_spot", "version": 2,
        "searchable": [{"field": "pair", "index": "string"}, {"field": "ext_id", "index": "string"}],
    })
    px.publish_contract({
        "kind": "validation", "entity": "fx_spot", "version": 1, "mode": "enforce",
        "dq_checks": [{"name": "ext_unique", "field": "ext_id", "unique": True}],
    })
    px.put("fx_spot", _fx("F1", ext_id="Y1"))
    with pytest.raises(ValidationError):
        px.put("fx_spot", _fx("F2", ext_id="Y1"))


def test_uniqueness_allows_distinct_values(px):
    px.publish_contract({
        "kind": "validation", "entity": "fx_spot", "version": 1, "mode": "enforce",
        "dq_checks": [{"name": "ext_unique", "field": "ext_id", "unique": True}],
    })
    px.put("fx_spot", _fx("F1", ext_id="A"))
    px.put("fx_spot", _fx("F2", ext_id="B"))            # distinct -> ok
    assert px.get("fx_spot", "F2")["ext_id"] == "B"
