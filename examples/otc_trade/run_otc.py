"""OTC trade ingestion end to end — validation, ETL, dual-shape storage, indexes.

    python examples/otc_trade/run_otc.py                              # in-memory, no services
    PHRONEXUS_BACKEND=aerospike python examples/otc_trade/run_otc.py  # live stack

All shape/behaviour is config under contracts/ — there is no otc-specific Python
in the framework. The flow:

  1. Seed reference data (counterparties, currencies) into Aerospike.
  2. A raw trade is received and validated:
       - JSON Schema (syntax), then
       - DQ checks — including that counterparty_id and currency EXIST in the
         reference data (an Aerospike lookup, not just a shape check).
  3. A small ETL step enriches/normalises the message.
  4. It's stored in two shapes from one write:
       - t_doc  — the full message as a msgpack binary blob (canonical)
       - t_base — a few hot elements as a queryable map
     and two inverted indexes are maintained: idx_cp (counterparty_id) and
     idx_ns (netting_set_id).
  5. Scan-free lookups via those indexes.
  6. A malformed trade is rejected (unknown counterparty + currency).
"""

from __future__ import annotations

import os
from pathlib import Path

from phronexus import Phronexus, Settings, codec

HERE = Path(__file__).parent


def banner(t: str) -> None:
    print(f"\n=== {t} ===")


def etl(raw: dict, fx: dict) -> dict:
    """Normalise + enrich the raw message before it's stored."""
    d = dict(raw)
    d["currency"] = str(d["currency"]).upper()
    d.setdefault("product", "IRS")
    # Derive the netting set if the source didn't supply one.
    if not d.get("netting_set_id"):
        d["netting_set_id"] = f"NS-{d['counterparty_id']}-{d['currency']}"
    # Convert notional to USD using the reference-data FX rate.
    rate = fx.get(d["currency"], 1.0)
    d["notional_usd"] = round(float(d["notional"]) * rate, 2)
    d["ingested_at"] = 20260711
    return d


def main() -> None:
    settings = Settings(backend=os.environ.get("PHRONEXUS_BACKEND", "memory"))
    settings.observability.log_level = "ERROR"
    px = Phronexus(settings)
    px.load_contract_dir(str(HERE / "contracts"))

    # --- 1) seed reference data -------------------------------------------
    banner("1) seed reference data (Aerospike)")
    for cp in [
        {"counterparty_id": "CP-GS", "name": "Goldman Sachs", "lei": "784F5XWPLTWKTBV3E584"},
        {"counterparty_id": "CP-JPM", "name": "JPMorgan", "lei": "8I5DZWZKVSZI1NUHU748"},
        {"counterparty_id": "CP-DB", "name": "Deutsche Bank", "lei": "7LTWFZYICNSX8D621K86"},
    ]:
        px.put("counterparty", cp)
    fx = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27}
    for code, rate in fx.items():
        px.put("currency", {"code": code, "usd_rate": rate})
    print(f"seeded {3} counterparties and {len(fx)} currencies")

    # --- 2) receive + validate --------------------------------------------
    banner("2) trade received → validate (JSON Schema, then DQ + reference checks)")
    raw = {
        "trade_id": "OTC-1001", "counterparty_id": "CP-GS", "currency": "USD",
        "notional": 25_000_000, "trade_date": 20260711, "book": "RATES-1",
        # no netting_set_id on the wire — the ETL step derives it below
        # nested JSON elements — stored as-is (a list CDT and a map CDT):
        "legs": [
            {"leg": "pay", "ccy": "USD", "rate": 3.85},
            {"leg": "receive", "ccy": "USD", "index": "SOFR"},
        ],
        "additional_terms": {"csa": True, "threshold_usd": 0, "day_count": "ACT/360"},
    }
    print("raw message:", raw)
    rep = px.validate("otc_trade", raw)
    print(f"validate → ok={rep.ok}  errors={rep.errors}  warnings={rep.warnings}")

    # --- 3) ETL ------------------------------------------------------------
    banner("3) ETL: normalise currency, derive netting set, add notional_usd")
    doc = etl(raw, fx)
    print("enriched:", doc)

    # --- 4) store (dual shape + indexes) ----------------------------------
    banner("4) store → t_doc (msgpack blob) + t_base (per-element bins) + idx_cp / idx_ns")
    doc_id = px.put("otc_trade", doc)
    print("stored doc_id:", doc_id)

    # Peek the raw physical records to prove the two encodings.
    doc_rec = px.store.get("t_doc", "OTC-1001")
    base_rec = px.store.get("t_base", "CP-GS:OTC-1001")
    blob = doc_rec.bins["doc"]
    print(f"  t_doc['doc'] → {type(blob).__name__}, {len(blob)} bytes (msgpack blob); "
          f"unpacks to {len(codec.unpack(blob))} fields")
    # t_base has NO 'doc' bin — each element is its own bin (cp_id / ns_id /
    # add_terms renamed). Nested elements land as native map/list bins.
    base_bins = {k: v for k, v in base_rec.bins.items() if not k.startswith("_")}
    print(f"  t_base bins  → {len(base_bins)} individual bins: {sorted(base_bins)}")
    print(f"    legs        → {type(base_bins['legs']).__name__} (list CDT): {base_bins['legs']}")
    print(f"    add_terms   → {type(base_bins['add_terms']).__name__} (map CDT): {base_bins['add_terms']}")

    # Read back through the manifest (decodes the msgpack canonical automatically).
    got = px.get("otc_trade", "OTC-1001")
    print("  read back (via manifest, decoded):", got)

    # --- 5) index lookups --------------------------------------------------
    banner("5) scan-free lookups via the named inverted indexes")
    # Add a second trade in the SAME counterparty + netting set so both posting
    # lists hold more than one primary key.
    px.put("otc_trade", etl({"trade_id": "OTC-1002", "counterparty_id": "CP-GS",
        "currency": "USD", "notional": 5_000_000, "trade_date": 20260712,
        "netting_set_id": "NS-CP-GS-USD"}, fx))
    # An inverted index is a posting list: value -> [trade primary keys]. Looking
    # up counterparty_id=CP-GS returns every trade PK booked to that counterparty.
    cp_pks = px.index.lookup_eq("otc_trade", "counterparty_id", "CP-GS")
    print(f"idx_cp  counterparty_id=CP-GS → posting list of trade PKs: {sorted(cp_pks)}")
    # Pull every trade for the counterparty: inverted index -> PKs -> one batch
    # read (px.find), instead of a read per trade.
    by_cp = px.find("otc_trade", "counterparty_id", "CP-GS")
    print(f"        px.find (index + batch read) → {len(by_cp)} trades: "
          f"{sorted(t['trade_id'] for t in by_cp)}")
    ns_pks = px.index.lookup_eq("otc_trade", "netting_set_id", "NS-CP-GS-USD")
    print(f"idx_ns  netting_set_id=NS-CP-GS-USD → posting list of trade PKs: {sorted(ns_pks)}")

    # --- 6) rejection ------------------------------------------------------
    banner("6) reference-data DQ rejects an unknown counterparty + currency")
    bad = {"trade_id": "OTC-9999", "counterparty_id": "CP-UNKNOWN", "currency": "ZZZ",
           "notional": 1_000_000, "trade_date": 20260711, "netting_set_id": "NS-X"}
    rep = px.validate("otc_trade", bad)
    print(f"validate → ok={rep.ok}")
    for e in rep.errors:
        print("   ✗", e)

    px.close()


if __name__ == "__main__":
    main()
