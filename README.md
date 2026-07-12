# Phronexus Core

A **contract-driven data projection framework**. One logical document is
projected into many physical records — each shaped for a specific read path —
and made visible atomically via a **manifest**. What to store, what's
searchable, and what each consumer sees are all described by **versioned
contracts** that live in the datastore and hot-reload, so new business entities
(trades, FX, repos, …) are onboarded by config, not code.

- **Operational store:** Aerospike (hot path) — with a supported escape hatch to native features
- **Retention store:** Apache Iceberg (optional, async, long-term) — an insert-only, idempotent log
- **Change feed:** Kafka / Redpanda
- **Interfaces:** Python SDK + REST + a self-contained web console
- **Data plane:** a transactional state machine, a distributed exactly-once scheduler, **bitemporal** as-of reads, binary (msgpack) journals, a per-document **trace / debug view**, and **lineage** (source event → result)
- **Control plane:** a governance layer — config-driven **RBAC**, a change→approve→publish workflow (N-of-M approvals + separation of duties), immutable **hash-chained history**, environment **promotion**, per-environment **COB**, backfill control, and tamper-evident **evidence export**

> **Status:** a hardened, guarantee-backed data plane plus a full governance
> control plane. The data plane covers contracts, manifest write/read, inverted-
> index search, consumer views, a transactional state machine, an exactly-once
> scheduler, insert-only Iceberg retention, a decoupled audit/trace worker,
> bitemporal storage, and supported native-Aerospike access — with Stage-1
> crash-consistency guarantees (fault-injection tested). The control plane adds
> config-driven RBAC, contract approval/publication governance, promotion,
> evidence, and lineage. Runs on a built-in in-memory backend (no services) and
> on Aerospike. A **production-depth Counterparty-Credit-Risk reference** —
> onboarding, netting-set lifecycle, cube projection, intraday recalc, COB
> reproducibility, hot/cold tiering, lineage, and recovery — is built on it in
> [`examples/ccr/`](examples/ccr).

---

## Documentation

Full docs are in [`docs/`](docs/):

- **[architecture.md](docs/architecture.md)** — mental model, components, write path, state machine, control plane (with diagrams).
- **[building-on-phronexus.md](docs/building-on-phronexus.md)** — developer guide: onboard an entity by config end-to-end, hooks, REST/SDK, evolving contracts.
- **[governance.md](docs/governance.md)** — the control plane: RBAC, change→approve→publish, hash-chained history, rollback, promotion, COB, backfill control, evidence export.
- **[bitemporal.md](docs/bitemporal.md)** — bitemporal storage: valid-time + transaction-time, as-of reads, COB defaulting, late/corrected events.
- **[api-reference.md](docs/api-reference.md)** — the verbs at a glance: Python, REST (incl. governance + lineage), and the remote SDK.
- **[contracts-reference.md](docs/contracts-reference.md)** — every field of the six contract kinds (incl. `temporal`/`valid_time_field`).
- **[storage-layouts.md](docs/storage-layouts.md)** — physical storage options: `msgpack`/`bins`/`spread` encodings, `bin_map`, `native_txn`, batch reads.
- **[state-machine.md](docs/state-machine.md)** — the transactional state machine in depth.
- **[ccr-reference.md](docs/ccr-reference.md)** — production-depth Counterparty-Credit-Risk reference: the ten proofs (onboarding → recovery), all as config.
- **[retention-and-journals.md](docs/retention-and-journals.md)** — insert-only, self-reconciling retention log and the binary msgpack journals.
- **[deployment.md](docs/deployment.md)** — production: Aerospike / MSK / S3 Tables, native-client options, TLS/mTLS, auth, RBAC, observability, ops.

## Try it (no services)

Every example runs on the built-in in-memory backend — nothing to install but the
package. Prefix any with `PHRONEXUS_BACKEND=aerospike` to run the same code
against a live stack (`cd deploy && ./setup.sh` brings one up).

```bash
python examples/bond/run_bond.py        # onboard an entity by config, end to end
```

| Example | Shows |
|---|---|
| [`bond`](examples/bond) | onboard an entity by config (storage/query/view/validation/transition) |
| [`ccr`](examples/ccr) | **production-depth CCR**: onboarding + reference DQ, netting-set lifecycle + served aggregation, cube projection, intraday recalc, bitemporal COB, hot/cold tiering, lineage, recovery (ten proofs, `pytest -m ccr`) |
| [`otc_trade`](examples/otc_trade) | validation → ETL → dual-shape storage (`t_doc` msgpack + `t_base` bins), reference-data DQ, named indexes `idx_cp`/`idx_ns`, batch `find` |
| [`fvcube`](examples/fvcube) | a future-value cube stored **transposed** — each date its own bin (`spread`) |
| [`fv_paths`](examples/fv_paths) | the cube **at scale** — 3 parts × ~2000 numbers/date: one record per date, parts as bins, max per date |
| [`library_embed`](examples/library_embed) | the **whole API embedded as a library** — store + state-machine node + journal + retention→Iceberg, no services |
| [`reject_handling`](examples/reject_handling) | what to do when validation fails — drop / Kafka reject topic / HTTP webhook / a specific message via a hook |
| [`versioned_trade`](examples/versioned_trade) | append-only **insert-only versioning** — keep every version, read the latest, and find which version was current *as of* a past time |
All default to the always-available in-memory
backend (no services); prefix with `PHRONEXUS_BACKEND=aerospike` to run the same
code against a live Aerospike + Kafka stack.

**Management UI:** the API serves a self-contained web console at `/ui` — browse/edit
contracts with their version history, validate documents, drive state machines,
browse data, and **trace** any document's lifecycle. It also fronts the **control
plane** — permission-gated **Change Requests** (draft → approve → publish),
**Fleet** (active versions, drift, COB), hash-chained **History**, **Backfill**
controls, **Promotion**, **Evidence** export, and **Lineage** — with tabs and
controls gated by the caller's permissions. Behind JWT login (fixed users now;
Microsoft Entra / Azure AD via the OIDC provider seam). See
[docs/deployment.md](docs/deployment.md#management-ui).

## Why manifests

Aerospike gives strong single-record operations but no free cross-record
atomicity. Phronexus writes **all projection records first, then the manifest
last**. The manifest is the single commit point:

- Reads go **through the manifest** → a half-written document has no committed
  manifest and is invisible.
- On backends with native multi-record transactions (Aerospike 8.0+ and the
  in-memory backend) the whole write is one atomic transaction; the manifest is
  still the authoritative "is this committed?" record.
- The manifest is written with a **generation CAS**, giving optimistic
  concurrency across competing writers.
- Crashed writes leave only invisible orphans, swept by the **reaper**.

## The six contract kinds

| Contract | Defines | Example file |
|----------|---------|--------------|
| **storage** | primary key, projections across sets, update/delete policy, TTL, Iceberg retention | `contracts_examples/trade.storage.yaml` |
| **query** | searchable fields, index types, named query patterns | `contracts_examples/trade.query.yaml` |
| **view** | consumer output: field allow-list, masking, transforms | `contracts_examples/trade.view.*.yaml` |
| **transition** | state-machine lifecycle: states, guards, emitted events | `contracts_examples/trade.transition.yaml` |
| **validation** | JSON Schema (syntax) + data-quality checks | `contracts_examples/trade.validation.yaml` |
| **stream** | JSON Schema on published events (no external registry) | `examples/bond/bond.stream.yaml` |

Contracts are versioned and stored in the `_contracts` set. An in-process cache
refreshes on a configurable cadence (default **300s**), so publishing a new
version and flipping the active pointer evolves schema **without a redeploy**.
Each document records the contract version that produced it, so older documents
stay interpretable after the active version moves on.

## Search: a self-maintained inverted index

Instead of Aerospike secondary indexes, Phronexus maintains its own inverted
index in the KV store (portable across backends, no cluster index-build step).
It's a **derived hint** — queries load every candidate through the manifest and
re-check predicates, so a stale posting is validated away and the document is
always addressable by primary key.

- Equality / `in` → posting-list lookups and intersections
- Numeric ranges (`gt`/`gte`/`lt`/`lte`) → per-field ordered term dictionary

Candidates resolved from a posting list are loaded in **one batch read**
(Aerospike `batch_read`), not a read per id — so pulling all trades for a
counterparty is two round-trips, not N. `px.find(entity, field, value)` is the
shortcut (index → PKs → batch); `px.get_many(entity, ids)` batch-reads by id;
`px.query(...)` uses the same batch path.

## Quickstart

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
pytest                     # full suite, no services needed
```

```python
from phronexus import Phronexus, Settings

px = Phronexus(Settings(backend="memory"))
px.load_contract_dir("contracts_examples")

px.put("trade", {
    "trade_id": "T-1", "counterparty": "GS",
    "notional": 1_000_000.005, "ccy": "USD",
    "trade_date": 20250115, "book": "RATES",
})

px.get("trade", "T-1")
# {'trade_id': 'T-1', 'counterparty': 'GS', ...}

px.query({"entity": "trade",
          "where": [{"field": "counterparty", "op": "eq", "value": "GS"}]})

px.query_pattern("trade", "cpty_since", cpty="GS", since=20250101)

px.view("trade", "public", "T-1")
# {'trade_id': 'T-1', 'counterparty': '****', 'ccy': 'USD', 'trade_date': 20250115}
```

### Bulk ingestion

For loading many documents, `put_many` commits each as its own durable manifest
write but relays the change-feed **once** at the end:

```python
px.put_many("trade", trades)                 # e.g. 500K trades
```

Also over REST (`POST /entities/{entity}/documents/batch`) and the remote SDK
(`client.put_many(entity, docs)`). Each document is committed independently (its
own manifest CAS), so a bad document can't fail the whole batch.

Posting lists are **segmented** (bounded-size head, sealed on fill), so ingesting
documents that share an indexed value stays **O(N)**, not O(N²) — 500K trades
ingest in ~2.5 min single-threaded on the in-memory backend, and scale out
horizontally by Kafka partition (key = doc_id) on Aerospike. Tune the segment
size with `index.segment_size` (default 512).

### Onboard a new entity — by config only

Drop a `storage` (and optional `query`/`view`) contract into a directory and
`load_contract_dir`. See `fx_spot.*.yaml` and `repo.storage.yaml` — no code
changes to store, search, or serve a brand-new entity.

## REST API & remote SDK

The same core is exposed over HTTP. `Phronexus` is the in-process SDK;
`PhronexusClient` is the remote one — application code reads the same either way.

```bash
pip install -e '.[api,client]'
PHRONEXUS_BACKEND=memory python -m phronexus.api.server   # serves on :8080
```

| Method & path | Purpose |
|---|---|
| `PUT /entities/{entity}/documents` | write (manifest commit) |
| `GET /entities/{entity}/documents/{id}` | read by primary key |
| `DELETE /entities/{entity}/documents/{id}` | delete (per contract policy) |
| `POST /entities/{entity}/query?view=` | JSON query (optionally through a view) |
| `POST /entities/{entity}/events` | **submit a domain event (sync state-machine accept/reject)** |
| `POST /entities/{entity}/patterns/{name}` | named parameterised query |
| `GET /entities/{entity}/views/{view}/documents/{id}` | read through a view |
| `GET /entities/{entity}/documents/{id}/trace` · `/lineage` | audit trace · source→result lineage |
| `POST /contracts` · `POST /contracts/refresh` | contract admin (admin principal) |
| `/governance/*` | control plane: changes, approval, fleet, history, promotion, evidence (permission-gated) |
| `GET/PUT/DELETE /schedules` · `POST /schedules/tick` | scheduler admin (admin principal) |
| `GET /healthz` · `GET /readyz` | liveness / readiness |

```python
from phronexus.sdk import PhronexusClient

c = PhronexusClient("https://phronexus.internal:8080", api_key="…")
c.put("trade", {...})
c.query("trade", [{"field": "counterparty", "op": "eq", "value": "GS"}], view="public")
```

**Auth** is pluggable and configured centrally (`PHRONEXUS_API__AUTH__*`): any subset of
`none` / `api_key` / `bearer` / `jwt` / `mtls` (+ OIDC login for the console), tried
in order. mTLS reads the client-cert CN from a trusted proxy header (or the TLS
transport). **Authorization is config-driven RBAC** — `auth.roles` maps each role
to permission strings (from JWT/OIDC claims), enforced per-endpoint (contract-admin
still requires admin; governance actions require their specific permission — see
[docs/governance.md](docs/governance.md)). Every request gets a bound `request_id`
in the structured logs and an `X-Request-ID` response header; OTel FastAPI
instrumentation attaches when `OTEL_ENABLED=true`.

## Iceberg retention — an insert-only, idempotent log

Every committed document emits a `CommitEvent` onto the Kafka/Redpanda change
feed. A separate **retention worker** consumes it and appends to the Iceberg
table declared by each storage contract's `iceberg` block — only for entities
that opt in. Aerospike stays the source of truth for the hot path; Iceberg is
the analytical, long-term tail.

Retention is modelled as an **append-only event log** — nothing is mutated in
place — so it **self-reconciles on replay**:

- Each commit is one immutable row with `_txn` (idempotency key), `_version`
  (the manifest generation → "latest wins" order), `_op` (upsert / **delete
  tombstone**), and `_raw` (the exact document as a **msgpack** blob).
- **Current state is a derived view** — `reconcile()` keeps the max-`_version`
  row per doc and drops tombstones — so duplicate appends are harmless. The
  pipeline only needs at-least-once delivery to be correct.
- The Kafka consumer uses **manual offset commit** (advance only after the batch
  is flushed), so a crash replays rather than drops.

See [docs/retention-and-journals.md](docs/retention-and-journals.md).

```bash
# production: its own process next to the app
pip install -e '.[kafka,iceberg]'
PHRONEXUS_BACKEND=aerospike PHRONEXUS_KAFKA__ENABLED=true \
  PHRONEXUS_ICEBERG__ENABLED=true PHRONEXUS_ICEBERG__BACKEND=iceberg \
  python -m phronexus.retention.main
```

Validate the whole path locally (MinIO + Iceberg REST catalog + worker):
`cd deploy && docker compose --profile iceberg up -d` then
`docker compose --profile iceberg run --rm iceberg-validate`.

## Trace / debug view (decoupled audit worker)

"What happened to this document?" — every commit, delete, and derived state
transition, oldest first. A second **change-feed consumer** (the same decoupled
pattern as retention) builds a per-document trace; it runs as **its own process**,
off the hot write path, so it scales and restarts independently and adds zero
write latency. Correlated by `txn_id`; state transitions are derived from
committed document versions.

Read it in the console's **Trace** tab, or over REST:

```bash
GET /entities/{entity}/documents/{doc_id}/trace
# -> {"events":[{"kind":"commit","version":2,"from":"cube_requested","to":"calc_requested","txn_id":"…"}, …]}
```

```bash
# production: its own process next to the app (part of the default deploy stack)
PHRONEXUS_BACKEND=aerospike PHRONEXUS_KAFKA__ENABLED=true python -m phronexus.audit.main
```

On the in-memory backend (no Kafka) the consumer runs **inline**, so a trace
exists with zero services. Disable with `PHRONEXUS_AUDIT__ENABLED=false` (e.g.
for peak-write benchmarks), or set `PHRONEXUS_AUDIT__TTL_SECONDS` to auto-expire
old trace records.

## Ingestion validation (JSON Schema + data quality)

A **validation contract** validates every incoming document at the write boundary
— so `put()`, the state machine, and backfill all enforce it uniformly:

- **JSON Schema** (Draft 2020-12) for structural/syntax validation (required
  fields, types, shapes).
- **Data-quality checks** — declarative field rules (`required`, `type`, `in`,
  `min`/`max`, `min_len`/`max_len`, `regex`) and cross-field **expressions**
  (sandboxed), each with `severity: error | warn`.
- `mode: enforce | warn_only | off`. Errors abort the write (HTTP `422`);
  warnings are logged/metered but don't block.

```bash
POST /entities/{entity}/validate     # dry-run: {ok, errors[], warnings[]}
```

```python
report = px.validate("trade", doc)   # -> ValidationReport(ok, errors, warnings)
```

**On failure**, choose what happens by config: drop, route the whole message to a
Kafka reject topic or an HTTP webhook (`statemachine.dlq_topic:` `kafka://…` /
`http://…` / `null://`), or emit a specific message via a reject hook — see
[`examples/reject_handling/`](examples/reject_handling).

## Transactional state machine (optional face)

Phronexus can also run as an **autonomous, event-driven state machine**: consume a
domain event, load the entity's current state, evaluate a metadata-driven
**transition contract**, and *atomically* persist the new state, enqueue output
events, and record a dedup marker — in one Aerospike transaction — then relay the
outbox to Kafka. The guarantee is **atomic durable transition + effectively-once
emission** (transactional outbox + idempotent writes + input dedup), the strongest
this class of system can give without literal 2PC. The atomic block needs native
transactions, so it **fails fast** on a store that can't provide them (opt into
best-effort at-least-once with `statemachine.require_atomic: false` for CE); write
conflicts are retried, and rejected/poison events are **durably** dead-lettered.

```python
sm = px.state_machine(output=publisher)
sm.process(InputEvent(entity="trade", event_type="TradeConfirmed",
                      key="T-1", payload={}, event_id="evt-123"))
```

Three faces, one engine: an autonomous Kafka service
(`python -m phronexus.statemachine.runner`), a **synchronous REST endpoint**
(`POST /entities/{entity}/events` — returns accept/reject: `200` applied/duplicate,
`409` invalid transition, `422` guard failure, while output events still fan out
via the outbox), and an embedded DishtaYantra `CalculationNode`
(`PhronexusStateMachineNode`). See
[`docs/state-machine.md`](docs/state-machine.md) and the DAG integration in
[`docs/integration-dishtayantra.md`](docs/integration-dishtayantra.md).

## Distributed scheduler (exactly-once)

Schedule work (EOD jobs, periodic refreshes) with state stored in Aerospike, so
**duplicate events can't be generated** no matter how many replicas run. Each
occurrence is claimed with a **generation-CAS lease**; the winner writes the
claim and a trigger outbox row in one transaction and relays it to a topic —
losers get a conflict and skip. Pattern: *kafka topic → the service → it pulls
what it needs via Phronexus*.

```python
from phronexus.scheduler import ScheduleSpec
sch = px.scheduler(output=publisher)
sch.upsert_schedule(ScheduleSpec(name="ccr-eod", topic="kafka://ccr.eod",
                                 daily_at="18:30", timezone="America/New_York"))
```

Run standalone (`python -m phronexus.scheduler.runner`, scale to N replicas),
manage over REST (`PUT/GET/DELETE /schedules`, admin), or embed `px.scheduler()`.
See [docs/ccr-reference.md](docs/ccr-reference.md#the-scheduler--eod-exactly-once-across-replicas).

## Governance control plane

Above the data plane, a governance layer turns "publish a contract" into a
governed, audited change — the difference between a framework and an enterprise
product. It **wraps, never bypasses**, the registry primitives.

- **Config-driven RBAC** — roles map to permissions in config (`auth.roles`);
  `require_permission(...)` gates every action (403 otherwise). Consumed by JWT/OIDC.
- **Approval workflow** — a change goes `draft → submit → approve → publish` with a
  **configurable N-of-M** policy (per environment × contract kind), **distinct**
  approvers, and separation of duties (the author can't self-approve).
- **Compatibility explanations** — a structured report of what changed and why it
  is/isn't breaking, plus a field-level version `diff`.
- **Immutable history** — every action is appended to a SHA-256 **hash-chained**
  log; any edit/reorder/delete is detectable (`verify()`).
- **Governed rollback** — flip the active pointer to a prior version, audited and
  reversible; version history is never mutated.
- **Environment promotion** — a signed, provenance-stamped bundle export/import
  (air-gapped/cross-cluster) plus a direct cross-store path.
- **Evidence export** — a windowed, tamper-evident bundle (audit trail +
  interactions + governance log) with a content hash and log-head hash.
- **Fleet + backfill + COB** — one view of active versions, drift, and COB;
  cooperative pause/resume/cancel of backfills; a per-environment processing date.

```python
cr = px.governance.draft("alice", contract_v2)      # author drafts
px.governance.submit("alice", cr["id"])             # compat check + required approvals
px.governance.approve("bob", cr["id"])              # a DIFFERENT approver (SoD)
px.governance.publish("bob", cr["id"])              # only when approved
px.governance.log.verify()                          # {"ok": true, ...}
```

Over REST at `/governance/*` (permission-gated) and in the console's governance
tabs. See [docs/governance.md](docs/governance.md).

## Bitemporal storage (as-of / COB reproducibility)

A storage entity can opt into `temporal: bitemporal` to track two time axes —
**valid time** (the business/effective date, e.g. COB) and **transaction time**
(when it was recorded). A read as-of `(valid_time, tx_time)` returns what was
*known by* `tx_time` to be *effective at* `valid_time`, so a past COB view is
**reproducible**: later writes and backdated corrections never change it.

```python
px.get("exposure_result", "T", as_of=20260711)                # value effective at that COB
px.get("exposure_result", "T", as_of=20260711, tx_as_of=t0)   # ... as it was known at t0
```

`as_of`/`valid_from` default to the environment COB. See
[docs/bitemporal.md](docs/bitemporal.md).

## Binary journals (msgpack)

Persist full messages and request/response pairs as compact **msgpack** blobs —
a few typed metadata bins for lookup plus one bin holding the whole payload,
decoded byte-faithfully on read:

- **Message journal** — every raw inbound Kafka message (full envelope).
- **Request/response journal** — per request, the request and its response as
  `req` / `resp` blobs; the state machine journals **every `process()`**
  automatically when enabled.

```python
rj = px.request_journal()
rj.read("evt-1")   # {"meta": {...}, "request": <decoded>, "response": <decoded>}
```

Enable with `journal.enabled` + `journal_messages` / `journal_requests`. See
[docs/retention-and-journals.md](docs/retention-and-journals.md#binary-journals).

## Native Aerospike access (supported)

The `KVStore` API is deliberately small; for native features it doesn't wrap —
**expressions, CDT list/map ops, `operate()`, batch, secondary-index queries,
UDFs** — use the supported accessor. Native client policies (timeouts,
consistency, rack, pools) pass through via `aerospike.policies` /
`aerospike.client_config`.

```python
nx = px.native_aerospike()                       # requires backend='aerospike'
nx.operate("positions", "acct-1", [lo.list_append("legs", leg)])
nx.client.batch_read(...)                        # raw connected client
```

It injects the namespace and **guards Phronexus-managed sets** — a write to the
manifest, projections, index, outboxes, dedup, schedules, or journals raises, so
the manifest-last invariant, in-txn index, and change feed can't be bypassed.
Reads/queries and your own sets are unrestricted. See
[docs/deployment.md](docs/deployment.md#using-native-aerospike-features-directly-supported-api).

## Operations

**Contract backfill** — after a contract evolves (new projection / searchable
field), re-project existing documents onto the active version. Idempotent:

```bash
phronexus --contracts-dir contracts_examples backfill trade
```

**Admin CLI** (`phronexus …`): `publish-contract`, `put`, `get`, `delete`,
`query [--view]`, `backfill [--dry-run]`, `reap`. Backend/auth from `PHRONEXUS_*`.

**Load harness** (in-memory backend, exercises writes → reads → search →
retention end to end):

```bash
python scripts/loadtest.py --docs 20000 --queries 5000
```

## Configuration

Config is layered (highest precedence first): constructor kwargs → environment
variables (`PHRONEXUS_*`, nested with `__`) → `.env` → **per-subsystem YAML files**
→ file secrets. Production deployments keep readable, version-controlled files per
subsystem under [`config/`](config/) and override individual values with env vars:

```bash
export PHRONEXUS_CONFIG_DIR=./config      # opt-in; unset keeps the in-memory dev default
python -m phronexus.api.server
```

| File | Subsystem | Security |
|------|-----------|----------|
| `config/aerospike.yaml` | operational store | TLS/mTLS, user/pass, auth mode |
| `config/kafka.yaml` | change-feed / state-machine transport | **Amazon MSK IAM** (default), SASL/SCRAM, or mTLS |
| `config/iceberg.yaml` | long-term retention | **Amazon S3 Tables** (Iceberg REST + SigV4), or self-hosted REST + S3 creds |
| `config/api.yaml` | REST server + auth | server TLS/mTLS, API-key/bearer/mTLS |

### Managed AWS stack

The shipped templates target a managed AWS deployment out of the box:

- **Amazon MSK** — `kafka.msk_iam: true` uses SASL_SSL + OAUTHBEARER with SigV4
  tokens from the instance role (`pip install 'phronexus-core[msk]'`); SASL/SCRAM
  and mTLS are drop-in alternatives.
- **Amazon S3 Tables** — `iceberg.sigv4_enabled: true` with `signing_name:
  s3tables` (or `glue`) and the table-bucket ARN as the warehouse.
- **Aerospike 8.1** — native multi-record transactions require **Enterprise
  Edition + a strong-consistency namespace**; on Community Edition set
  `aerospike.use_native_txn: false` (the manifest pattern still gives all-or-
  nothing visibility, with weaker cross-record durability under failure).

Secrets are never committed — reference them with `${ENV_VAR}` / `${ENV_VAR:-default}`
placeholders that resolve from the environment at load time; certificate/key paths
point at mounted secrets. See [`config/README.md`](config/README.md).

## Running against real infrastructure

Fastest path — a self-contained local stack (Aerospike + Kafka + Phronexus) with
a one-command setup + smoke test:

```bash
cd deploy && ./setup.sh       # builds, starts, ingests contracts, smoke-tests
# then open http://localhost:8080/docs
```

See [`deploy/README.md`](deploy/README.md) for details, the Kafka runner, and
troubleshooting. For a hand-rolled setup:

```bash
cp .env.example .env          # set PHRONEXUS_BACKEND=aerospike, KAFKA__ENABLED=true
pip install -e '.[aerospike,kafka]'
python -m phronexus.api.server
```

TLS/mTLS, auth, and structured JSON logging are configured centrally via
`Settings` (`.env`). OpenTelemetry traces + metrics are emitted natively when
`OBSERVABILITY__OTEL_ENABLED=true`.

## Architecture

```
phronexus/
  config.py            # central Settings (env / .env)
  kv/                  # KVStore abstraction: memory + aerospike backends, txns
  contracts/           # pydantic models, loader, versioned registry + cache
  storage/             # projection engine (document -> projection records)
  manifest/            # write/read/delete via the manifest pattern; reaper
  query/               # inverted index + JSON/YAML query engine
  views/               # consumer view projection (allow-list, mask, transform)
  validation/          # JSON Schema + data-quality validator
  statemachine/        # transactional state machine (hooks, I/O, node, runner)
  scheduler/           # distributed exactly-once scheduler (CAS lease)
  retention/           # insert-only Iceberg log: source, warehouse, worker
  governance/          # control plane: service, hash-chained log, promote, evidence
  lineage.py           # source-event -> result lineage assembler
  events/              # change-feed sinks (memory, kafka)
  codec.py             # msgpack pack/unpack (binary envelopes)
  journal.py           # message + request/response journals
  native.py            # supported native-Aerospike accessor (managed-set guard)
  observability/       # structured logging + OTel telemetry + durability preflight
  api/                 # FastAPI app, routers (incl. governance), auth + RBAC, UI
  sdk/                 # PhronexusClient (remote HTTP SDK)
  admin/               # contract backfill job
  cli.py               # phronexus admin CLI
  core.py              # Phronexus facade (in-process SDK entrypoint)
scripts/loadtest.py    # throughput harness
```

## Roadmap

- ~~**Phase 3** — REST API (FastAPI), auth (API key / bearer / mTLS-CN), request tracing.~~ ✅
- ~~**Phase 4** — Iceberg retention worker consuming the Kafka change feed.~~ ✅
- ~~**Phase 5** — load tests, contract backfill job, admin CLI.~~ ✅
- ~~**Stage 1** — crash-consistency hardening: guarantee-backed write/commit, reaper grace, retention idempotence, consumer recovery, a deterministic fault-injection harness.~~ ✅
- ~~**Stage 2** — governance control plane: config-driven RBAC, contract approval + publication history, compatibility checks on publish, promotion, evidence, bitemporal COB.~~ ✅
- ~~**Stage 3** — production-depth CCR reference: onboarding, netting-set lifecycle, intraday recalc, hot/cold tiering, lineage, recovery.~~ ✅

**Beyond:** row-level Iceberg merge/compaction, index sharding for hot terms,
per-view RBAC scopes, property-based/randomized-seed chaos fuzzing, and horizontal
load testing against a real Aerospike + Kafka cluster.

## Design decisions

- **Manifest + native transactions** for write atomicity (portable fallback to manifest-only).
- **Inverted index** over Aerospike sindex for portability and manifest-consistent reads.
- **JSON/YAML query rules** mirroring contract style, rather than a SQL dialect.
