"""CCR5 — cube projection: point-per-tenor (hot sorted/clipped) + transposed spread."""

from __future__ import annotations

import pytest

from ccrsupport import build_ccr
from phronexus.query.models import QueryDoc, SortKey

COB = 20260711


@pytest.mark.ccr
def test_point_cube_hot_sorted_clipped_query():
    px = build_ccr()
    for t in (365, 1, 90, 30, 7):  # unsorted inserts
        px.put("value_cube", {"trade_id": "T", "scenario_id": "BASE", "tenor": t,
                              "value": 1000.0 + t, "currency": "USD", "as_of": COB})
    page = px.query_page(QueryDoc(
        entity="value_cube",
        where=[{"field": "trade_id", "op": "eq", "value": "T"},
               {"field": "scenario_id", "op": "eq", "value": "BASE"}],
        sort=[SortKey(field="tenor", order="asc")], limit=3))
    assert [d["tenor"] for d in page["documents"]] == [1, 7, 30]
    assert page["has_more"] is True
    px.close()


@pytest.mark.ccr
def test_transposed_cube_spreads_dates_to_bins():
    px = build_ccr()
    curve = {"20260712": 1.00, "20260718": 1.05, "20260731": 1.12}
    px.put("fvcube", {"trade_id": "T", "scenario_id": "BASE", "as_of": COB,
                      "currency": "USD", "curve": curve})

    # The wide projection stores each future date as its own bin (d<YYYYMMDD>).
    wide = px.store.get("fvc_wide", "T:BASE")
    assert wide is not None
    for date, val in curve.items():
        assert wide.bins[f"d{date}"] == val

    # The canonical (msgpack) reconstructs the clean nested document (doc id is
    # the primary key joined by "|").
    doc = px.get("fvcube", "T|BASE")
    assert doc["curve"] == curve and doc["as_of"] == COB
    px.close()
