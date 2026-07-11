"""Future-value cube at scale — parts as separate bins, max per date, by config.

    python examples/fv_paths/run_fv_paths.py                              # in-memory
    PHRONEXUS_BACKEND=aerospike python examples/fv_paths/run_fv_paths.py  # live stack

Each (trade, scenario, date) is one small record. Each date has 3 parts of ~2000
numbers (e.g. simulation paths); each part is its own bin (p1/p2/p3), and the
per-date max is computed at ingest and stored in its own `max` bin.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

from phronexus import Phronexus, Settings

HERE = Path(__file__).parent
N_PATHS = 2000


def gen_part(seed: int, base: float, n: int = N_PATHS) -> list[float]:
    """Deterministic ~2000-number path vector (stands in for a Monte-Carlo run)."""
    return [round(base * (1 + ((seed * 7919 + j * 104729) % 1000) / 1000.0), 2)
            for j in range(n)]


def etl_point(trade_id: str, scenario: str, as_of: int, day: int, ccy: str) -> dict:
    """Build one per-date point: 3 parts + the max across all of them."""
    d = date(as_of // 10000, as_of // 100 % 100, as_of % 100) + timedelta(days=day)
    val_date = int(d.strftime("%Y%m%d"))
    base = 1_000_000.0 * (1 + day / 365)          # value grows with the horizon
    parts = {f"part{p}": gen_part(val_date + p, base * (1 + 0.1 * p)) for p in (1, 2, 3)}
    max_value = max(v for part in parts.values() for v in part)   # max per date
    return {"trade_id": trade_id, "scenario_id": scenario, "val_date": val_date,
            "currency": ccy, "max_value": max_value, **parts}


def main() -> None:
    settings = Settings(backend=os.environ.get("PHRONEXUS_BACKEND", "memory"))
    settings.observability.log_level = "ERROR"
    px = Phronexus(settings)
    px.load_contract_dir(str(HERE / "contracts"))

    as_of, ccy = 20260711, "USD"
    tenors = [1, 7, 30, 90, 365]      # 5 future dates, each a separate record
    print(f"ingesting {len(tenors)} dates x 3 parts x {N_PATHS} numbers "
          f"for CCR-T-001 / BASE ...")
    # Bulk write: all per-date points in one call (each independently committed,
    # the change feed relayed once at the end).
    points = [etl_point("CCR-T-001", "BASE", as_of, t, ccy) for t in tenors]
    ids = px.put_many("fv_point", points)
    print(f"  px.put_many wrote {len(ids)} points in one call")

    # --- one date's physical record: parts as separate bins + a max bin --------
    one = etl_point("CCR-T-001", "BASE", as_of, 1, ccy)["val_date"]
    rec = px.store.get("fvp_wide", f"CCR-T-001:BASE:{one}")
    print(f"\nrecord fvp_wide[CCR-T-001:BASE:{one}] — one bin per part + max:")
    for b in ("p1", "p2", "p3"):
        vals = rec.bins[b]
        print(f"  {b:3} → list of {len(vals)} numbers (e.g. {vals[0]:,} … {vals[-1]:,})")
    print(f"  max → {rec.bins['max']:,}   (max across all 3 parts for this date)")
    print(f"  scalar bins: val_date={rec.bins['val_date']}, currency={rec.bins['currency']}")

    # --- each date is its own record; query them by trade ----------------------
    pts = px.query({"entity": "fv_point",
                    "where": [{"field": "trade_id", "op": "eq", "value": "CCR-T-001"}]})
    print(f"\n{len(pts)} per-date records for CCR-T-001 (max per date):")
    for p in sorted(pts, key=lambda r: r["val_date"]):
        print(f"  {p['val_date']}  max={p['max_value']:,}")

    px.close()


if __name__ == "__main__":
    main()
