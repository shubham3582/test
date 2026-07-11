# OTC trade ingestion — validation, ETL, dual-shape storage, indexes

Everything is config; there is no `otc_trade`-specific Python in the framework.

```bash
python examples/otc_trade/run_otc.py                              # in-memory, no services
PHRONEXUS_BACKEND=aerospike python examples/otc_trade/run_otc.py  # live stack
```

## The pipeline

1. **Receive + validate** — the raw message is checked against a **JSON Schema**
   (syntax), then **data-quality** rules run. Two of the DQ checks are
   *reference-data existence* checks: `counterparty_id` and `currency` must
   resolve to a seeded document in the `counterparty` / `currency` entities
   (a real Aerospike lookup, not just a shape check).
2. **ETL** — normalise currency, derive `netting_set_id` when absent, add
   `notional_usd` from the reference FX rate.
3. **Store, two shapes from one write:**
   - `t_doc` — the **full message as a msgpack binary blob** (canonical; reads
     reconstruct from here and decode automatically).
   - `t_base` — a **few hot elements, each as its own Aerospike bin**
     (`encoding: bins`, with `bin_map` shortening `counterparty_id → cp_id`,
     `netting_set_id → ns_id`, `additional_terms → add_terms` because the JSON
     name is 16 chars > Aerospike's 15-char limit), keyed by
     `counterparty_id:trade_id`. Nested elements are stored **as-is**: `legs`
     lands as a list bin and `additional_terms` as a map bin (native CDTs).
4. **Inverted indexes** — `idx_cp` on `counterparty_id` and `idx_ns` on
   `netting_set_id`. Each is a **posting list `value → [trade primary keys]`**, so
   a lookup by counterparty returns every trade booked to it — scan-free.
   `px.find("otc_trade", "counterparty_id", "CP-GS")` does exactly this: index
   lookup → PKs → **one batch read** (Aerospike `batch_read`), not a read per
   trade. `px.query(...)` uses the same batch path internally.

## Contracts

| File | Kind | What it declares |
|---|---|---|
| `contracts/otc_trade.storage.yaml` | storage | `t_doc` (msgpack, canonical) + `t_base` (per-element `bins`) projections |
| `contracts/otc_trade.query.yaml` | query | searchable `counterparty_id` (`idx_cp`), `netting_set_id` (`idx_ns`) |
| `contracts/otc_trade.validation.yaml` | validation | JSON Schema + DQ, incl. `references` checks |
| `contracts/counterparty.storage.yaml` | storage | counterparty reference data |
| `contracts/currency.storage.yaml` | storage | currency reference data (carries FX rate) |
