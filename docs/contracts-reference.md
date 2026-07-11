# Contract Reference

Every entity concern is described by a versioned contract stored in the
`_contracts` set. Contracts are JSON/YAML, validated on publish. This page lists
every field for the five kinds.

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
| `ttl` | int | seconds; `0` = never expire |
| `canonical` | bool | exactly one true; must be `fields:["*"]` and key uses only PK fields |

---

## `query`

Defines searchable fields (backed by the inverted index) and named, parameterised
query patterns.

| Field | Type | Notes |
|---|---|---|
| `searchable` | `[{field, index}]` | `index`: `string` (eq/in) or `numeric` (adds range) |
| `patterns` | `[QueryPattern]` | reusable named queries |

**Predicate** (`where` item): `{field, op, value}` where `op` ∈
`eq, ne, in, gt, gte, lt, lte`. Ad-hoc queries use the same shape:

```json
{"entity": "bond", "where": [{"field": "coupon", "op": "gte", "value": 4.0}], "limit": 100}
```

**QueryPattern**: `{name, where:[Predicate], limit}` — `value` may use
`${param}` placeholders bound at call time (`query_pattern(entity, name, **params)`).
A range op (`gt/gte/lt/lte`) requires a `numeric` index; queries need at least
one positive predicate (`eq/in/range`) to seed candidates.

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
