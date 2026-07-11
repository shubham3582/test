"""Transposed future-value cube — dates as Aerospike bins, by config.

    python examples/fvcube/run_fvcube.py                              # in-memory
    PHRONEXUS_BACKEND=aerospike python examples/fvcube/run_fvcube.py  # live stack

A future-value cube arrives as points (one value per tenor). We ETL it into a
per-scenario document whose `curve` is a {date: value} map, then the STORAGE
CONTRACT does the transpose: a `spread` projection explodes `curve` so each date
is its own bin. No cube-specific Python in the framework — the pivot is config.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

from phronexus import Phronexus, Settings

HERE = Path(__file__).parent


def as_of_plus(base: int, days: int) -> str:
    d = date(base // 10000, base // 100 % 100, base % 100) + timedelta(days=days)
    return d.strftime("%Y%m%d")


def main() -> None:
    settings = Settings(backend=os.environ.get("PHRONEXUS_BACKEND", "memory"))
    settings.observability.log_level = "ERROR"
    px = Phronexus(settings)
    px.load_contract_dir(str(HERE / "contracts"))

    # --- raw cube points (per tenor) arrive; ETL transposes tenor -> date map --
    as_of = 20260711
    tenors = [1, 7, 30, 90, 180, 365, 730]
    base_notional = 25_000_000.0
    curve = {
        as_of_plus(as_of, t): round(base_notional * (1 + t / 3650), 2)
        for t in tenors
    }
    doc = {"trade_id": "CCR-T-001", "scenario_id": "BASE", "as_of": as_of,
           "currency": "USD", "curve": curve}
    print("logical document (curve is a {date: value} map):")
    print(f"  curve = {curve}")

    px.put("fvcube", doc)

    # --- the transposed physical record: one bin per date ----------------------
    print("\ntransposed record in fvc_wide (each date is its own bin):")
    rec = px.store.get("fvc_wide", "CCR-T-001:BASE")
    date_bins = {k: v for k, v in rec.bins.items() if k.startswith("d")}
    for bn in sorted(date_bins):
        print(f"  {bn} = {date_bins[bn]:,}")
    other = {k: v for k, v in rec.bins.items() if not k.startswith(("d", "_"))}
    print(f"  (+ scalar bins: {other})")

    # --- reads reconstruct the clean nested doc from the canonical -------------
    print("\nread back (from canonical fvc_doc, msgpack):")
    got = px.get("fvcube", "CCR-T-001|BASE")
    print(f"  curve has {len(got['curve'])} dated points; currency={got['currency']}")

    px.close()


if __name__ == "__main__":
    main()
