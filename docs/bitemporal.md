# Bitemporal storage & COB reproducibility

Some data must be reproducible **as-of a date**: what was the exposure for COB 2026-07-11,
as it was known that evening — not as corrected since? Phronexus has a first-class
bitemporal storage mode for exactly this. It tracks two time axes:

- **valid time** (`_valid_from`) — the business/effective date the fact applies to (e.g. the
  COB). Sourced from the document's `valid_time_field`, else an explicit arg, else today.
- **transaction time** (`_tx_from`) — the system time the fact was recorded.

A read as-of `(valid_time, tx_time)` returns the version that was **known by** `tx_time` to be
**effective at** `valid_time` — the latest correction wins. Later writes never change a past
as-of result, so a COB view is reproducible.

---

## Opting in — the storage contract

```yaml
kind: storage
entity: exposure_result
version: 1
primary_key: [trade_id]
manifest_set: exposure_manifest
temporal: bitemporal          # append-only temporal versions (default: "none")
valid_time_field: cob         # document field carrying the effective date (int YYYYMMDD)
projections:
  - {name: main, set: exposure_main, key: "{trade_id}", fields: ["*"], canonical: true}
```

- `temporal: bitemporal` makes every write an **immutable version** (append-only) rather than
  latest-wins. The logical document (its primary key) accumulates an ordered version index; a
  late or corrected event is just another append.
- `valid_time_field` names the field that carries the effective date. When absent, the write's
  valid-time defaults to the environment **COB** (`px.governance.get_cob()`), else today.
- Bitemporal entities keep the full document per version in the canonical set; secondary
  projections / inverted indexes are not applied to them.

---

## Reading as-of

```python
px.put("exposure_result", {"trade_id": "T", "cob": 20260711, "exposure": 100.0, "currency": "USD"})
px.put("exposure_result", {"trade_id": "T", "cob": 20260712, "exposure": 150.0, "currency": "USD"})

px.get("exposure_result", "T", as_of=20260711)     # 100.0 — the 20260712 version isn't effective yet
px.get("exposure_result", "T", as_of=20260712)     # 150.0
px.get("exposure_result", "T")                     # defaults as_of to the environment COB
```

Both `put` and `get` default the valid-time to the **environment COB** when you don't pass one,
so the whole plane reads "as of today's processing date" by default. Set the COB via the
control plane (see [governance.md](governance.md) §7):

```python
px.governance.set_cob("ops", 20260712)
```

---

## Late & corrected events

A correction is a normal write; the tx-time axis makes it non-destructive.

```python
import time
px.put("exposure_result", {"trade_id": "T2", "cob": 20260711, "exposure": 100.0, "currency": "USD"})
tt = time.time()                                   # snapshot: "as known now"
# ... later, a correction to the SAME cob lands ...
px.put("exposure_result", {"trade_id": "T2", "cob": 20260711, "exposure": 120.0, "currency": "USD"})

px.get("exposure_result", "T2", as_of=20260711)                 # 120.0  (current view)
px.get("exposure_result", "T2", as_of=20260711, tx_as_of=tt)    # 100.0  (reproducible: as known at tt)
```

A **backdated** correction (effective earlier, recorded now) is only visible at or after its
tx-time: a read as-of a tx-time *before* the correction was recorded does not see it. This is
what makes end-of-day reporting reproducible and audit-defensible.

---

## Semantics (precise)

For a read `(as_of=vt, tx_as_of=tt)` (defaults: `vt = env COB`, `tt = now`), among all versions
of the logical document:

1. keep versions with `_tx_from ≤ tt` **and** `_valid_from ≤ vt`;
2. return the one with the greatest `_tx_from` (the latest correction known by `tt`), tie-broken
   by greatest `_valid_from`;
3. if none qualify, the document is not yet effective at `vt` as known at `tt` → `None`.

---

## API

- Library: `px.put(entity, doc, valid_from=None)`, `px.get(entity, id, as_of=None, tx_as_of=None)`.
- REST: `GET /entities/{entity}/documents/{id}?as_of=&tx_as_of=`.

## Notes & limits

- Bitemporal is **per-entity opt-in**; leave `temporal: none` for current-state entities.
- The **state machine writes via the transactional manifest path, not the bitemporal path** —
  so a saga's system-of-record entity stays current-state (upsert). Model the reproducible
  *artifact* (e.g. `exposure_result`) as the bitemporal entity, written via `px.put`. This is
  the split used by the CCR reference.
- Proven by `pytest -m governance` (GOV5) and `pytest -m ccr` (CCR6/CCR7).
