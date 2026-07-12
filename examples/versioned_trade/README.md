# Versioned, insert-only trade store (latest + as-of)

A trade is amended over time and you keep the whole history, but usually read only
the latest — and sometimes need "which version was current as of time T?". This is
an **append-only / bitemporal** store, expressed as config.

```bash
python examples/versioned_trade/run_versioned.py                              # in-memory
PHRONEXUS_BACKEND=aerospike python examples/versioned_trade/run_versioned.py  # live stack
```

## How to store it

Put the version in the **primary key** and make the entity **insert-only**:

```yaml
primary_key: [trade_id, trade_version]   # each version is its own immutable record
update_policy: insert_only               # nothing is ever overwritten
```

Each amendment writes a new `(trade_id, trade_version)` with a `valid_from`
(effective time). Because it's insert-only, **re-delivering an existing version is
a no-op** (`DocumentAlreadyExists`) — history can't be mutated, so ingestion is
idempotent.

## How to read it

Index `trade_version` and `valid_from` as numeric, then sort + clip to 1:

| Read | Query |
|---|---|
| **Latest** | `where trade_id == X`, `sort trade_version desc`, `limit 1` |
| **As of time T** | `where trade_id == X AND valid_from <= T`, `sort valid_from desc`, `limit 1` |

```
latest            -> v3   (notional 28,000,000)
as of 2026-07-07  -> v2   (valid_from 2026-07-05, the one current then)
```

## Scaling the "latest" read (optional)

The `latest` query reads every version of a trade to find the max — fine for a
handful of versions. For a hot path with deep histories, keep a small **current
pointer**: a companion `upsert` record keyed by `trade_id` that holds (or points
to) the latest version, updated on each amendment. The insert-only history stays
the source of truth; the pointer is a derived, O(1) "head". (Do the two writes in
one transaction via the state machine if you need them atomic.)

## Same pattern, other entities

This is exactly how to make the CCR **value cube** immutable: put `as_of` in the
key (`primary_key: [trade_id, scenario_id, as_of, tenor]`, `update_policy:
insert_only`) so each day's cube is its own immutable set, re-delivery is a no-op,
and you can read any day's cube by its `as_of`.
