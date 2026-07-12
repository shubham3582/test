"""Reusable CCR operations layered on the Phronexus library.

These are the thin "serve/aggregate/orchestrate" pieces that sit around the risk
engine — deliberately NOT risk maths. The netting aggregation is a plain sum of
member-trade exposures; the quant engine owns real netting/XVA/PFE. Phronexus
stores, governs, and SERVES the operational data.
"""

from __future__ import annotations

from typing import Optional


def onboard_counterparty(sm, cp: dict) -> str:
    """Drive a counterparty through its onboarding saga to ``active``. ``cp`` must
    carry counterparty_id/name/jurisdiction/rating. Returns the final status."""
    from phronexus.statemachine import InputEvent

    cid = cp["counterparty_id"]
    sm.process(InputEvent(entity="counterparty", event_type="CounterpartyReceived",
                          key=cid, payload=cp, event_id=f"cp-recv-{cid}"))
    r = sm.process(InputEvent(entity="counterparty", event_type="ReviewApproved",
                              key=cid, payload={}, event_id=f"cp-appr-{cid}"))
    return r.to_state


def aggregate_netting_set(px, netting_set_id: str, cob: int, *,
                          source_event_id: Optional[str] = None) -> dict:
    """Served netting-set aggregation: sum member-trade exposures (as-of ``cob``)
    into a bitemporal ``ns_exposure``. Members are found via the ``idx_ns``
    inverted index. Returns the aggregate written."""
    total, count, currency = 0.0, 0, None
    for trade in px.find("ccr_trade", "netting_set_id", netting_set_id):
        er = px.get("exposure_result", trade["trade_id"], as_of=cob)
        if er is None:
            continue
        total += float(er.get("exposure", 0.0))
        count += 1
        currency = currency or er.get("currency")
    agg = {
        "netting_set_id": netting_set_id, "cob": cob, "exposure": total,
        "trade_count": count, "currency": currency or "USD",
    }
    if source_event_id:
        agg["source_event_id"] = source_event_id
    px.put("ns_exposure", agg)
    return agg
