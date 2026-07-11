# Contract Reference

Every entity concern is described by a versioned contract stored in the
`_contracts` set. Contracts are JSON/YAML, validated on publish. This page lists
every field for the six kinds.

Common to all: `kind`, `entity`, `version` (integer). Publishing a version and
flipping its active pointer is what evolves schema without a redeploy; documents
record the version that produced them, so old data stays interpretable.

---

## `storage`

Defines persistence: primary key, projections across Aerospike sets, policies,
TTL, and Iceberg retention.

| Field | Type | Notes |
|---|---|---|
| `primary_key` | `[string]` | fields forming the document id |
| `manifest_set` | string | Aerospike set holding the manifest (commit point) |
| `update_policy` | `upsert` \| `insert_only` | `insert_only` rejects overwrites |
| `delete_policy` | `soft` \| `hard` | soft = tombstone manifest; hard = purge |
| `projections` | `[Projection]` | one physical record shape per read path |
| `iceberg` | object | `{enabled, table, partition_by:[…], retention_days}` |

**Projection**

| Field | Type | Notes |
|---|---|---|
| `name` | string | unique within the contract |
| `set` | string | Aerospike set the record lives in |
| `key` | string | template, e.g. `"{issuer}:{isin}"` (tokens from doc fields) |
| `fields` | `[string]` | `["*"]` = whole document; else a subset |
| `encoding` | string | how the payload is laid out: `map` (default — one `doc` map bin), `msgpack` (one `doc` binary blob), or `bins` (each element becomes its own Aerospike bin — natively addressable for secondary indexes / expressions / partial reads). Under `bins`, a nested **map or list** element is stored as-is as a native Aerospike CDT bin. |
| `bin_map` | `{field: bin}` | `encoding: bins` only — rename/shorten a field's bin (e.g. `counterparty_id → cp_id`). Unlisted fields keep their name. Bin names ≤ 15 chars, no reserved/duplicate names. |
| `spread` | `[{field, prefix}]` | `encoding: bins` only — **transpose** a map-valued field into one bin per entry (data-driven bin names): `{field: {key: value}}` → bins `prefix+key → value`. E.g. a cube's `curve: {date: value}` → bins `d20260712, …`. Not allowed on the canonical projection; keys' bin-name length is checked at write time. |
| `ttl` | int | seconds; `0` = never expire |
| `canonical` | bool | exactly one true; must be `fields:["*"]` and key uses only PK fields |

---

## `query`

Defines searchable fields (backed by the inverted index) and named, parameterised
query patterns.

| Field | Type | Notes |
|---|---|---|
| `searchable` | `[{field, index, name?}]` | `index`: `string` (eq/in) or `numeric` (adds range). `name`: optional label for the index (e.g. `idx_cp`); the index is a posting list `value → [doc ids]`, maintained per `(entity, field)`. |
| `patterns` | `[QueryPattern]` | reusable named queries |

**Predicate** (`where` item): `{field, op, value}` where `op` ∈
`eq, ne, in, gt, gte, lt, lte`. Queries also support `sort`, `limit`, and
`offset` (pagination); sort fields must be searchable:

```json
{"entity": "bond",
 "where": [{"field": "coupon", "op": "gte", "value": 4.0}],
 "sort": [{"field": "coupon", "order": "desc"}],
 "limit": 50, "offset": 0}
```

`query_page()` (and `POST /entities/{entity}/query`) return
`{documents, count, offset, limit, has_more}`; `query()` returns just the page
list.

**QueryPattern**: `{name, where:[Predicate], limit}` — `value` may use
`${param}` placeholders bound at call time (`query_pattern(entity, name, **params)`).
A range op (`gt/gte/lt/lte`) requires a `numeric` index; queries need at least
one positive predicate (`eq/in/range`) to seed candidates.

### Worked example — one entity: `t_doc`, `t_base`, `idx_cp`, `idx_ns`

A single `trade` entity showing the full storage + query surface together: the
whole document as one msgpack blob (`t_doc`), a few hot elements each as their own
bin (`t_base`), and two named inverted indexes.

```yaml
kind: storage
entity: trade
version: 1
primary_key: [trade_id]
manifest_set: trade_manifest
projections:
  - {name: doc, set: t_doc, key: "{trade_id}", fields: ["*"], encoding: msgpack, canonical: true}
  - name: base                         # a few hot elements, each its own bin
    set: t_base
    key: "{counterparty_id}:{trade_id}"
    fields: [trade_id, counterparty_id, netting_set_id, notional, currency, trade_date]
    encoding: bins
    bin_map: {counterparty_id: cp_id, netting_set_id: ns_id}   # 15-char bin names
```

```yaml
kind: query
entity: trade
version: 1
searchable:
  - {field: counterparty_id, index: string, name: idx_cp}   # inverted index idx_cp
  - {field: netting_set_id,  index: string, name: idx_ns}   # inverted index idx_ns
patterns:
  - {name: by_counterparty, where: [{field: counterparty_id, op: eq, value: "${cpty}"}]}
```

Pull all trades for a counterparty via `idx_cp` + a single batch read:
`px.find("trade", "counterparty_id", "CP-GS")`. Runnable end-to-end (with
reference-data DQ) in [`examples/otc_trade/`](../examples/otc_trade).

---

## `view`

Defines a consumer-specific output projection.

| Field | Type | Notes |
|---|---|---|
| `view` | string | view name (unique per entity) |
| `fields` | `[string]` | allow-list projected into the output |
| `mask` | `[string]` | fields redacted to `****` (must be in `fields`) |
| `transform` | `{field: name}` | transform applied per field |

Built-in transforms: `round2`, `upper`, `lower`, `abs`. Register more with
`phronexus.views.register_transform(name, fn)`.

---

## `validation`

Defines ingestion validation: JSON Schema (structure) + data-quality checks
(semantics). Enforced at the write boundary for `put()`, the state machine, and
backfill.

| Field | Type | Notes |
|---|---|---|
| `mode` | `enforce` \| `warn_only` \| `off` | `warn_only` downgrades errors to warnings |
| `json_schema` | object | JSON Schema Draft 2020-12; checked at publish time |
| `dq_checks` | `[DQCheck]` | declarative quality rules |

**DQCheck** — set either a cross-field `expr` **or** a `field` with constraints:

| Field | Type | Notes |
|---|---|---|
| `name` | string | identifies the check in error/warning messages |
| `severity` | `error` \| `warn` | errors block (in `enforce`); warns never block |
| `message` | string | optional custom message |
| `expr` | string | sandboxed boolean expression over the document |
| `field` | string | field to constrain |
| `required` | bool | field must be present and non-null |
| `type` | `string`\|`number`\|`integer`\|`boolean` | type check |
| `in` | `[value]` | value allow-list |
| `min` / `max` | number | numeric bounds |
| `min_len` / `max_len` | int | length bounds |
| `regex` | string | pattern the value must match |
| `unique` | bool | value must be unique across the entity (store lookup) |
| `references` | string | value must be an existing doc id of the named entity (store lookup) |

Expressions support comparisons, `and`/`or`/`not`, names (document fields), and
literals — no function calls, imports, or attribute access.

**Context-aware checks.** `unique` and `references` do a store lookup at the
write boundary. `unique` uses the inverted index when the field is searchable
(fast) and falls back to a scan otherwise — index unique fields in the query
contract. Both are evaluated pre-commit; under high concurrency, strict
uniqueness also needs an Aerospike strong-consistency namespace (two concurrent
inserts of the same value can otherwise both pass). Example:

```yaml
dq_checks:
  - {name: ext_id_unique, field: ext_id, unique: true}
  - {name: cpty_exists,   field: counterparty_id, references: counterparty}
```

---

## `transition`

Defines the entity lifecycle as a state machine.

| Field | Type | Notes |
|---|---|---|
| `state_field` | string | document field holding the current state (default `status`) |
| `inputs` | `[string]` | informational: input topics the runner consumes |
| `transitions` | `[Transition]` | the transition table |

**Transition**

| Field | Type | Notes |
|---|---|---|
| `event` | string | input event type that triggers it (named `event`, not `on`) |
| `from` | string \| `null` \| `"*"` | source state; `null` = creation, `"*"` = any |
| `to` | string | target state written into `state_field` |
| `guard` | string | sandboxed boolean expression; must pass to proceed |
| `emit` | `[{topic, type}]` | output events; `topic` is a URI (see routing) |

**Emit routing** by `topic` scheme: `kafka://…` → Kafka/MSK, `http(s)://…` →
HTTP POST (TLS/mTLS), `null://` or empty `emit` → consumer-only (no output).

---

## `stream`

Registers a **JSON Schema per outbound event type** — a schema-registry
substitute that keeps events as JSON on the wire and validates them at **produce
time** (before commit), so malformed events are never published.

| Field | Type | Notes |
|---|---|---|
| `mode` | `enforce` \| `warn_only` \| `off` | enforce rejects the transition; warn logs but publishes |
| `events` | `[{type, json_schema}]` | `type` matches the transition's emitted `type` |

```yaml
kind: stream
entity: bond
version: 1
mode: enforce
events:
  - type: BondActivated
    json_schema:
      type: object
      required: [isin, issuer, status]
      properties: {status: {const: active}}
```

An emitted event whose payload fails its schema rejects the transition (nothing
committed, nothing published) under `enforce`; `px.validate_event(entity, type,
payload)` runs the same check standalone.

---

## Publishing

```python
px.load_contract_file("path/to/contract.yaml")     # load + activate one
px.load_contract_dir("examples/bond")              # load + activate a directory
px.publish_contract(dict_or_model, activate=True)  # programmatic
```

Or over REST: `POST /contracts` (requires an admin principal). Publishing
refreshes the in-process cache immediately for the publishing process; other
processes pick it up on their next refresh (`contracts.refresh_seconds`,
default 300).
