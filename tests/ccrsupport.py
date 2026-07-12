"""Shared setup for the Stage-3 CCR proofs (not a test module)."""

from __future__ import annotations

import sys
from pathlib import Path

from phronexus import Phronexus, Settings

CCR = Path(__file__).resolve().parent.parent / "examples" / "ccr"
if str(CCR) not in sys.path:
    sys.path.insert(0, str(CCR))  # make examples/ccr/ccr_ops.py importable


def seed_reference_data(px: Phronexus) -> None:
    px.put("counterparty", {"counterparty_id": "GS", "name": "Goldman Sachs",
                            "jurisdiction": "US", "rating": "A", "status": "active"})
    px.put("counterparty", {"counterparty_id": "DB", "name": "Deutsche Bank",
                            "jurisdiction": "DE", "rating": "BBB", "status": "active"})
    px.put("currency", {"code": "USD", "usd_rate": 1.0})
    px.put("currency", {"code": "EUR", "usd_rate": 1.08})
    px.put("netting_set", {"netting_set_id": "NS-GS-USD", "counterparty": "GS",
                           "csa_id": "CSA-GS-1", "status": "active"})


def build_ccr(*, seed: bool = True, journal: bool = False, **governance) -> Phronexus:
    s = Settings(backend="memory")
    s.observability.log_level = "WARNING"
    if journal:
        s.journal.enabled = True
        s.journal.journal_requests = True
    for k, v in governance.items():
        setattr(s.governance, k, v)
    px = Phronexus(s)
    px.load_contract_dir(str(CCR / "contracts"))
    if seed:
        seed_reference_data(px)
    return px


def a_trade(trade_id="CCR-T-1", **over) -> dict:
    return {"trade_id": trade_id, "counterparty": "GS", "netting_set_id": "NS-GS-USD",
            "book": "IRD-1", "product_type": "IRS", "notional": 25_000_000.0,
            "currency": "USD", "trade_date": 20260711, "maturity_date": 20360711, **over}
