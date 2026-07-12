"""Versioned, insert-only trade store — latest + point-in-time (as-of) reads.

    python examples/versioned_trade/run_versioned.py                              # in-memory
    PHRONEXUS_BACKEND=aerospike python examples/versioned_trade/run_versioned.py  # live stack

A trade is amended over time; every amendment is a new IMMUTABLE version. We keep
the whole history (insert-only) but usually read only the latest — and can still
ask "which version was current as of time T?".
"""

from __future__ import annotations

import os
from pathlib import Path

from phronexus import Phronexus, Settings
from phronexus.errors import DocumentAlreadyExists
from phronexus.query.models import QueryDoc, SortKey

HERE = Path(__file__).parent
TID = "OTC-9001"


def banner(t: str) -> None:
    print(f"\n=== {t} ===")


def latest(px: Phronexus, trade_id: str):
    """The highest-version record for a trade — sort by version desc, clip to 1."""
    page = px.query_page(QueryDoc(
        entity="vtrade",
        where=[{"field": "trade_id", "op": "eq", "value": trade_id}],
        sort=[SortKey(field="trade_version", order="desc")], limit=1))
    return page["documents"][0] if page["documents"] else None


def as_of(px: Phronexus, trade_id: str, t: int):
    """The version that was current AS OF time t — greatest valid_from <= t."""
    page = px.query_page(QueryDoc(
        entity="vtrade",
        where=[{"field": "trade_id", "op": "eq", "value": trade_id},
               {"field": "valid_from", "op": "lte", "value": t}],
        sort=[SortKey(field="valid_from", order="desc")], limit=1))
    return page["documents"][0] if page["documents"] else None


def main() -> None:
    settings = Settings(backend=os.environ.get("PHRONEXUS_BACKEND", "memory"))
    settings.observability.log_level = "ERROR"
    px = Phronexus(settings)
    px.load_contract_dir(str(HERE / "contracts"))

    # --- amendments arrive over time; each is a new immutable version ----------
    banner("store 3 immutable versions (insert-only)")
    versions = [
        {"trade_id": TID, "trade_version": 1, "valid_from": 20260701, "counterparty": "CP-GS", "notional": 25_000_000, "status": "booked"},
        {"trade_id": TID, "trade_version": 2, "valid_from": 20260705, "counterparty": "CP-GS", "notional": 30_000_000, "status": "amended"},
        {"trade_id": TID, "trade_version": 3, "valid_from": 20260710, "counterparty": "CP-GS", "notional": 28_000_000, "status": "amended"},
    ]
    for v in versions:
        px.put("vtrade", v)
        print(f"  v{v['trade_version']}  valid_from={v['valid_from']}  notional={v['notional']:,}")

    # --- the common case: we only care about the latest ------------------------
    banner("latest version")
    L = latest(px, TID)
    print(f"  -> v{L['trade_version']}  notional={L['notional']:,}  (valid_from {L['valid_from']})")

    # --- point-in-time: which version was current as of a past date? -----------
    for t in (20260703, 20260707, 20260712):
        A = as_of(px, TID, t)
        print(f"  as of {t}: v{A['trade_version']}  notional={A['notional']:,}  (valid_from {A['valid_from']})")

    # --- re-delivering a version is a no-op (idempotent) -----------------------
    banner("idempotent: re-insert v3")
    try:
        px.put("vtrade", versions[2])
        print("  overwrote (unexpected for insert_only)")
    except DocumentAlreadyExists:
        print("  duplicate ignored — insert-only, so re-delivery can't mutate history")

    px.close()


if __name__ == "__main__":
    main()
