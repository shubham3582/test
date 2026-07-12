"""Lineage: stitch a trade's source event → saga steps → cube → exposure result.

Correlates the operational trail of one trade across the entities and journals
the framework already keeps — the audit trace (per-document commit/transition
history), the request/response interactions (the saga request-reply chain, keyed
by the input event id), the value-cube points, and the (bitemporal) exposure
result — into one ordered, correlated view. Read-only: it assembles what is
already recorded, it does not compute anything.
"""

from __future__ import annotations

from typing import Any, Optional

from phronexus import codec
from phronexus.errors import ContractNotFound


def _interactions_for(px, trade_id: str) -> list[dict[str, Any]]:
    """Saga request/response interactions whose input event keyed this trade."""
    jcfg = px.settings.journal
    if not (jcfg.enabled and jcfg.journal_requests):
        return []
    out: list[dict[str, Any]] = []
    for event_id, rec in px.store.scan(jcfg.interactions_set):
        b = rec.bins
        try:
            req = codec.unpack(b["req"]) if b.get("req") else {}
        except Exception:  # noqa: BLE001
            req = {}
        if req.get("key") != trade_id:
            continue
        out.append({"event_id": event_id, "type": b.get("type"),
                    "status": b.get("status"), "ts": b.get("ts")})
    out.sort(key=lambda x: x.get("ts") or 0)
    return out


def _find(px, entity: str, field: str, value: str) -> list[dict[str, Any]]:
    try:
        return px.find(entity, field, value)
    except Exception:  # noqa: BLE001 - no query contract / index
        return []


def _version(px, entity: str) -> Optional[int]:
    try:
        return px.registry.active_storage(entity).version
    except ContractNotFound:
        return None


def build_lineage(px, trade_id: str, *, entity: str = "ccr_trade",
                  as_of: Optional[int] = None) -> dict[str, Any]:
    """Assemble the end-to-end lineage for ``trade_id``. ``as_of`` selects the
    bitemporal exposure view (defaults to the environment COB)."""
    trade = px.get(entity, trade_id)
    ns_id = trade.get("netting_set_id") if trade else None
    exposure = px.get("exposure_result", trade_id, as_of=as_of)
    ns_exposure = px.get("ns_exposure", ns_id, as_of=as_of) if ns_id else None
    cube_points = _find(px, "value_cube", "trade_id", trade_id)
    audit = px.trace(entity, trade_id)
    interactions = _interactions_for(px, trade_id)

    return {
        "trade_id": trade_id,
        "as_of": as_of if as_of is not None else px.governance.get_cob(),
        "source": {"entity": entity, "trade": trade, "netting_set_id": ns_id},
        # ordered saga steps (commit/transition history of the trade doc)
        "saga": audit,
        # request/reply interactions correlated to this trade (if journaling on)
        "interactions": interactions,
        # cube inputs that fed the calc
        "cube": {"point_count": len(cube_points),
                 "scenarios": sorted({p.get("scenario_id") for p in cube_points})},
        # the result artifacts
        "exposure_result": exposure,
        "ns_exposure": ns_exposure,
        # provenance: which contract versions produced these
        "contract_versions": {
            e: _version(px, e) for e in
            (entity, "value_cube", "exposure_result", "ns_exposure", "netting_set")
        },
    }
