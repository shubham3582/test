# Phronexus Core

A **contract-driven data projection framework**. One logical document is
projected into many physical records — each shaped for a specific read path —
and made visible atomically via a **manifest**. What to store, what's
searchable, and what each consumer sees are all described by **versioned
contracts** that live in the datastore and hot-reload, so new business entities
(trades, FX, repos, …) are onboarded by config, not code.

- **Operational store:** Aerospike (hot path)
- **Retention store:** Apache Iceberg (optional, async, long-term) — *Phase 4*
- **Change feed:** Kafka / Redpanda
- **Interfaces:** Python SDK + REST (REST is *Phase 3*)

> **Status:** Phases 0–5 complete (feature-complete prototype) — contracts,
> write/read via the manifest pattern, config-driven inverted-index search,
> consumer views, REST API + remote SDK with API-key/bearer/mTLS auth, async
> Iceberg retention off the Kafka change feed, and operational tooling
> (contract backfill, admin CLI, load harness). Runs today on a built-in
> in-memory backend (no services required) and on Aerospike.

---

## Documentation

Full docs are in [`docs/`](docs/):

- **[architecture.md](docs/architecture.md)** — mental model, components, write path, state machine (with diagrams).
- **[building-on-phronexus.md](docs/building-on-phronexus.md)** — developer guide: onboard an entity by config end-to-end, hooks, REST/SDK, evolving contracts.
- **[contracts-reference.md](docs/contracts-reference.md)** — every field of the five contract kinds.
- **[state-machine.md](docs/state-machine.md)** — the transactional state machine in depth.
- **[deployment.md](docs/deployment.md)** — production: Aerospike / MSK / S3 Tables, TLS/mTLS, auth, observability, ops.

Runnable worked example: `python examples/bond/run_bond.py`.

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

## The three contracts

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

## Quickstart

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
pytest                     # 29 tests, no services needed
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
| `POST /contracts` · `POST /contracts/refresh` | contract admin (admin principal) |
| `GET /healthz` · `GET /readyz` | liveness / readiness |

```python
from phronexus.sdk import PhronexusClient

c = PhronexusClient("https://phronexus.internal:8080", api_key="…")
c.put("trade", {...})
c.query("trade", [{"field": "counterparty", "op": "eq", "value": "GS"}], view="public")
```

**Auth** is pluggable and configured centrally (`PHRONEXUS_API__AUTH__*`): any subset of
`none` / `api_key` / `bearer` / `mtls`, tried in order. mTLS reads the client-cert
CN from a trusted proxy header (or the TLS transport). Contract-admin endpoints
additionally require an admin principal. Every request gets a bound `request_id`
in the structured logs and an `X-Request-ID` response header; OTel FastAPI
instrumentation attaches when `OTEL_ENABLED=true`.

## Iceberg retention (async, off the change feed)

Every committed document emits a `CommitEvent` onto the Kafka/Redpanda change
feed. A separate **retention worker** consumes it and lands rows in the Iceberg
table declared by each storage contract's `iceberg` block — only for entities
that opt in. Aerospike stays the source of truth for the hot path; Iceberg is
the analytical, long-term tail.

- Upserts are keyed by doc id → **replays are idempotent**.
- Deletes remove the row; rows carry `_expire_at` from `retention_days` for a
  maintenance pass to drop aged data.
- Pins the exact contract version that produced each document.

```bash
# production: its own process next to the app
pip install -e '.[kafka,iceberg]'
PHRONEXUS_BACKEND=aerospike PHRONEXUS_KAFKA__ENABLED=true \
  PHRONEXUS_ICEBERG__ENABLED=true PHRONEXUS_ICEBERG__BACKEND=iceberg \
  python -m phronexus.retention.main
```

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

## Transactional state machine (optional face)

Phronexus can also run as an **autonomous, event-driven state machine**: consume a
domain event, load the entity's current state, evaluate a metadata-driven
**transition contract**, and *atomically* persist the new state, enqueue output
events, and record a dedup marker — in one Aerospike transaction — then relay the
outbox to Kafka. The guarantee is **atomic durable transition + effectively-once
emission** (transactional outbox + idempotent writes + input dedup), the strongest
this class of system can give without literal 2PC.

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
  events/              # change-feed sinks (memory, kafka)
  observability/       # structured logging + OTel telemetry
  api/                 # FastAPI app, routers, auth, middleware, server
  sdk/                 # PhronexusClient (remote HTTP SDK)
  retention/           # change-feed source, warehouse, Iceberg worker
  statemachine/        # transactional state machine (processor, I/O, node, runner)
  admin/               # contract backfill job
  cli.py               # phronexus admin CLI
  core.py              # Phronexus facade (in-process SDK entrypoint)
scripts/loadtest.py    # throughput harness
```

## Roadmap

- ~~**Phase 3** — REST API (FastAPI), auth (API key / bearer / mTLS-CN), request tracing.~~ ✅
- ~~**Phase 4** — Iceberg retention worker consuming the Kafka change feed.~~ ✅
- ~~**Phase 5** — load tests, contract backfill job, admin CLI.~~ ✅

**Beyond the prototype:** row-level Iceberg merge/compaction, index sharding for
hot terms, contract schema-compatibility checks on publish, RBAC scopes per
view, and horizontal load testing against a real Aerospike + Kafka cluster.

## Design decisions

- **Manifest + native transactions** for write atomicity (portable fallback to manifest-only).
- **Inverted index** over Aerospike sindex for portability and manifest-consistent reads.
- **JSON/YAML query rules** mirroring contract style, rather than a SQL dialect.
