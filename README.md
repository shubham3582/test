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

> **Status:** Phases 0–3 complete — contracts, write/read via the manifest
> pattern, config-driven inverted-index search, consumer views, and a REST API
> + remote SDK with API-key/bearer/mTLS auth. Runs today on a built-in
> in-memory backend (no services required) and on Aerospike.

---

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

## Running against real infrastructure

```bash
docker compose up -d          # Aerospike, Redpanda, MinIO, OTel collector
cp .env.example .env          # set PHRONEXUS_BACKEND=aerospike, KAFKA__ENABLED=true
pip install -e '.[aerospike,kafka]'
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
  events/              # change-feed sinks (memory, kafka)
  observability/       # structured logging + OTel telemetry
  api/                 # FastAPI app, routers, auth, middleware, server
  sdk/                 # PhronexusClient (remote HTTP SDK)
  core.py              # Phronexus facade (in-process SDK entrypoint)
```

## Roadmap

- ~~**Phase 3** — REST API (FastAPI), auth (API key / bearer / mTLS-CN), request tracing.~~ ✅
- **Phase 4** — Iceberg retention worker consuming the Kafka change feed.
- **Phase 5** — load tests, contract backfill jobs, admin CLI.

## Design decisions

- **Manifest + native transactions** for write atomicity (portable fallback to manifest-only).
- **Inverted index** over Aerospike sindex for portability and manifest-consistent reads.
- **JSON/YAML query rules** mirroring contract style, rather than a SQL dialect.
