# Storage layouts

One logical document, many physical shapes — each **projection** in a storage
contract is a record written for a specific read path. This page covers the
physical options (how a projection is laid out and committed) and how to read
them back. Field-level details are in
[contracts-reference.md](contracts-reference.md#storage).

## The rule that never changes

Exactly one projection is **canonical** (`canonical: true`, `fields: ["*"]`).
Reads always reconstruct the document from the canonical projection, so all the
layout choices below are about *physical* shape — they never change what `get` /
`query` return.

## Payload encoding — how a projection stores its data

Set `encoding:` per projection:

| `encoding` | Layout | Use it for |
|---|---|---|
| `map` (default) | one `doc` bin holding an Aerospike map | general purpose |
| `msgpack` | one `doc` bin holding a compact binary blob | the canonical "whole document" store — small, byte-faithful, cross-language |
| `bins` | **each element becomes its own Aerospike bin** | native access: secondary indexes, expressions, partial reads |

Under `encoding: bins`:
- **`bin_map: {field: bin}`** renames a field's bin — needed because Aerospike caps
  bin names at 15 chars (`counterparty_id → cp_id`). Publish fails if a bin name is
  too long / reserved / duplicated.
- A nested **map or list** value is stored **as-is** as a native Aerospike CDT bin.
- **`spread: [{field, prefix}]`** transposes a map-valued field into one bin per
  entry — data-driven bin names: `curve: {"20260712": v, …}` → bins `d20260712, …`.
  Not allowed on the canonical projection.

## Write transactionality — `native_txn`

Per entity, override the backend's `aerospike.use_native_txn`:

- `native_txn: true` — wrap the multi-record write in a native Aerospike MRT (8.0+ EE)
- `native_txn: false` — ordered puts, manifest written last as the commit point (works on CE)
- omit — inherit the backend default

The manifest is the visibility commit point either way; this only affects
cross-record durability under failure.

## Reading it back

- `get` / `query` reconstruct from the canonical projection (decoding msgpack or
  gathering bins automatically).
- **Batch**: `get_many(entity, [ids])` reads many by id in one round-trip;
  `find(entity, field, value)` does inverted-index lookup → PKs → batch read
  (e.g. all trades for a counterparty). See [api-reference.md](api-reference.md).
- `bins`/`spread` projections are write-optimised physical views for native
  Aerospike access; the framework's reads use the canonical.

## Worked layouts (runnable)

| Want | Layout | Example |
|---|---|---|
| Full doc as a blob + a few hot elements as bins, named indexes | `t_doc` msgpack canonical + `t_base` bins (`idx_cp`/`idx_ns`) | [`examples/otc_trade/`](../examples/otc_trade) |
| A cube stored transposed — each date its own bin | one record per (trade, scenario) + `spread` on `curve` | [`examples/fvcube/`](../examples/fvcube) |
| A cube at scale — 3 parts × ~2000 numbers per date | one record **per date** (date in PK), parts as bins, `max` per date | [`examples/fv_paths/`](../examples/fv_paths) |

## Versioning / immutability (insert-only)

To keep an immutable history (amendments, dated snapshots), put the version (or
`as_of`) in the primary key and set `update_policy: insert_only`. Nothing is ever
overwritten, so re-delivering a version is an idempotent no-op. Read the **latest**
with `sort <version> desc, limit 1`, and the version **current as of time T** with
`where valid_from <= T, sort valid_from desc, limit 1`. Worked example:
[`examples/versioned_trade/`](../examples/versioned_trade).

## Choosing

- **Reads reconstruct the whole doc** → canonical `msgpack` (compact) or `map`.
- **Native per-field access / secondary indexes / expressions** → a `bins`
  read-projection alongside the canonical.
- **Wide, sparse, data-driven keys** (a curve of dates) → `spread`.
- **Big per-record payloads** (arrays of thousands) → push volume into the
  primary key so each record stays small, rather than one giant record.
