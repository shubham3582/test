# Future-value cube at scale — parts as bins, max per date

When each date carries **3 parts of ~2000 numbers** (e.g. Monte-Carlo paths),
transposing every date into one record would blow Aerospike's record-size limit.
The model here instead:

- **one record per `(trade, scenario, date)`** — `val_date` is in the primary
  key, so each date is its own ~48 KB record (`3 × 2000` doubles);
- **each part its own bin** — `encoding: bins` with `bin_map` → `p1` / `p2` /
  `p3`, each a 2000-element list, independently read/updated;
- **max per date** — computed at ingest (ETL) and stored in its own `max` bin.

```bash
python examples/fv_paths/run_fv_paths.py                              # in-memory
PHRONEXUS_BACKEND=aerospike python examples/fv_paths/run_fv_paths.py  # live stack
```

No new framework feature is needed — it's `encoding: bins` + `bin_map` applied at
the right storage grain. The `max` is an aggregate, so it's derived in ETL (a
write hook could do it instead); the storage contract owns the physical layout.
The per-date points are ingested with **`px.put_many`** — one bulk call, each
point independently committed, the change feed relayed once.

If a single part is itself too large or needs its own lifecycle, split further:
make the part the grain (`primary_key: [trade_id, scenario_id, val_date, part]`),
one record per part.
