# API Reference

Two ways to call Phronexus, same core underneath:

- **Embedded** — hold a `Phronexus` object and call the verbs below (in-process;
  its own Aerospike/Kafka connections). This is the library face (e.g. a
  DishtaYantra node); see [`examples/library_embed/`](../examples/library_embed).
- **Remote** — the REST API, or `PhronexusClient` (a thin HTTP wrapper).

```python
from phronexus import Phronexus, Settings
px = Phronexus(Settings())          # backend/kafka/iceberg from env or ./config
```

---

## Python verbs (`Phronexus`)

### Contracts (admin)
| Verb | Does |
|---|---|
| `load_contract_dir(path)` · `load_contract_file(path)` | publish + activate contracts from YAML |
| `publish_contract(contract, *, activate=True, force=False)` | publish/activate one contract (dict or model) |
| `list_contracts()` · `get_contract(identity)` | enumerate / fetch stored contracts |
| `refresh_contracts()` | force a cache refresh from the store |
| `validate_contract(doc)` | dry-run parse + compatibility check |

### Write
| Verb | Does |
|---|---|
| `put(entity, document) -> doc_id` | write one document (manifest commit) |
| `put_many(entity, [documents]) -> [doc_id]` | bulk write; each independently committed, change feed relayed once |
| `delete(entity, doc_id) -> bool` | delete (soft/hard per contract) |

### Read / query
| Verb | Does |
|---|---|
| `get(entity, doc_id, *, validate=False) -> doc?` | read one by id; `validate=True` re-checks against the JSON Schema version the doc was written under |
| `schema(entity, *, version=None) -> {version, mode, json_schema, dq_checks}` | fetch the versioned JSON Schema for an entity (schema-registry surface) |
| `get_many(entity, [doc_ids]) -> {id: doc}` | **batch** read by id (one round-trip per set) |
| `find(entity, field, value) -> [doc]` | inverted-index lookup → PKs → **batch** read (e.g. all trades for a counterparty) |
| `query(querydoc\|dict) -> [doc]` | predicate query (`where`, `sort`, `limit`, `offset`) |
| `query_page(...) -> {documents, count, offset, limit, has_more}` | query with paging metadata |
| `query_pattern(entity, name, **params) -> [doc]` | run a named query pattern from the contract |
| `view(entity, view, doc_id) -> doc?` | read one through a consumer view (allow-list / mask / transform) |
| `query_view(entity, view, querydoc\|dict) -> [doc]` | query, projected through a view |
| `trace(entity, doc_id) -> [event]` | audit/debug: commits, deletes, derived transitions |

### Validate
| Verb | Does |
|---|---|
| `validate(entity, document) -> ValidationReport` | JSON Schema + DQ, no write (`.ok`, `.errors`, `.warnings`) |
| `validate_event(entity, event_type, payload) -> ValidationReport` | validate an **outbound** event vs its `stream` schema |
| `validate_inbound(entity, event_type, payload) -> ValidationReport` | validate an **inbound** message vs its `ingress` schema (via `px.validator`) |

### Engines & journals
| Verb | Does |
|---|---|
| `state_machine(output=None, hooks=None) -> StateMachine` | transactional state machine (`.process(InputEvent)`) |
| `scheduler(output=None) -> Scheduler` | distributed exactly-once scheduler |
| `request_journal()` · `message_journal()` | msgpack journals — `.read(id)` for request/response or raw messages |
| `native_aerospike()` | supported native-Aerospike accessor (managed-set guard) |
| `durability_report()` | preflight the no-loss posture (producer acks · DLQ · Aerospike SC/persistence/replication · atomic-txn availability). Also the `phronexus doctor` CLI (non-zero exit if the config can lose messages). |
| `lineage(trade_id, entity="ccr_trade", as_of=None)` | end-to-end lineage: source event → saga → cube → exposure, correlated + ordered ([ccr-reference.md](ccr-reference.md)) |
| `close()` | release connections |

### Admin jobs (`phronexus.admin`)
| Job | Does |
|---|---|
| `BackfillJob(px).run(entity, dry_run=False)` | re-project committed docs under the active contract (new projections / searchable fields) |
| `ResyncJob(px).run(entity, direction, date_from=None, date_to=None, overwrite=False, dry_run=False)` | date-scoped resync between the hot (Aerospike) and cold (Iceberg) tiers — `cold-to-hot` rehydrates from Iceberg (silent, coords-preserving), `hot-to-cold` re-lands into Iceberg (idempotent). Bitemporal entities window on **valid-time**, others on **commit-time**. Checkpointed/restartable; honours governance pause/cancel. |

### Governance (`px.governance`)

The control plane — full guide in [governance.md](governance.md). Every method takes an
`actor` and is recorded in the hash-chained history.

| Verb | Does |
|---|---|
| `draft(actor, contract)` · `submit(actor, id)` | open / submit a change request (submit runs the compat check) |
| `approve(actor, id)` · `reject(actor, id, reason)` · `withdraw(actor, id)` | N-of-M approval (distinct approvers, SoD) |
| `publish(actor, id, force=False)` | publish an approved change (the exact approved bytes) |
| `rollback(actor, identity, reason)` | governed active-pointer rollback (audited, reversible) |
| `list_changes()` · `get_change(id)` | enumerate / fetch change requests |
| `export_bundle(actor, identity)` · `import_bundle(actor, bundle, sig)` · `promote_direct(actor, identity, target)` | environment promotion (signed bundle / direct) |
| `get_cob()` · `set_cob(actor, cob)` · `advance_cob(actor)` | environment COB / processing date |
| `fleet()` | active version per entity · drift · COB · backfill · history integrity |
| `backfill_status()` · `control_backfill(actor, entity, action)` | fleet-visible backfill + pause/resume/cancel |
| `resync_status()` · `control_resync(actor, run_key, action)` | fleet-visible tier resync (keyed `entity:direction:window`) + pause/resume/cancel |
| `log.entries()` · `log.verify()` | immutable, hash-chained governance history |
| `registry.compat_report(sc)` · `registry.diff(a, b)` | structured compatibility explanation + version diff |

---

## REST endpoints

Auth per `api.auth` (none / api_key / bearer / jwt / mtls). Authoritative, live
list: `/openapi.json` (Swagger UI at `/docs`).

| Method & path | Purpose |
|---|---|
| `PUT /entities/{entity}/documents` | write one |
| `POST /entities/{entity}/documents/batch` | bulk write |
| `GET /entities/{entity}/documents/{id}?as_of=&tx_as_of=` | read (bitemporal as-of, [bitemporal.md](bitemporal.md)) |
| `DELETE /entities/{entity}/documents/{id}` | delete |
| `GET /entities/{entity}/documents/{id}/trace` | audit/debug trace |
| `GET /entities/{entity}/documents/{id}/lineage?as_of=` | source event → saga → cube → exposure lineage |
| `POST /entities/{entity}/validate` | dry-run validation |
| `POST /entities/{entity}/query?view=` | query (optionally via a view) |
| `POST /entities/{entity}/patterns/{pattern}` | run a named query pattern |
| `GET /entities/{entity}/views/{view}/documents/{id}` | read through a view |
| `POST /entities/{entity}/events` | submit a lifecycle event (sync accept/reject) |
| `GET /interactions/{event_id}` | journaled request + response for an event |
| `GET/PUT/DELETE /schedules` · `POST /schedules/tick` | scheduler admin |
| `POST /contracts` · `GET /contracts[/{id}]` · `POST /contracts/refresh` · `POST /contracts/{id}/activate` | contract admin |
| `POST /auth/login` · `GET /auth/me` (roles + **permissions**) · `GET /healthz` · `GET /readyz` | auth + health |

### Governance endpoints (permission-gated — see [governance.md](governance.md))

| Method & path | Permission |
|---|---|
| `POST /governance/changes` · `/{id}/{submit,approve,reject,withdraw,publish}` | `contract:draft/submit/approve/publish` |
| `GET /governance/changes[/{id}]` · `/governance/log` · `/governance/fleet` | `governance:read` |
| `POST /governance/compat` · `GET /governance/diff?a=&b=` | `governance:read` |
| `POST /governance/rollback` | `contract:rollback` |
| `GET/POST /governance/cob` | read / `cob:set` |
| `GET /governance/backfill` · `POST /governance/backfill/{entity}/{action}` | read / `backfill:control` |
| `POST /admin/resync` · `GET /admin/resync/status` · `POST /admin/resync/control` | `resync:control` |
| `POST /governance/promote/{identity}/bundle` · `POST /governance/import` | `contract:promote` |
| `POST /governance/evidence` | `evidence:export` |

---

## Remote SDK (`PhronexusClient`)

```python
from phronexus.sdk import PhronexusClient
c = PhronexusClient("https://phronexus.internal:8443", api_key="…")   # or bearer=, or mTLS cert=
```

Covers the common verbs: `put`, `put_many`, `get`, `delete`, `view`,
`query(entity, where, view=…)`, `query_pattern(entity, name, **params)`,
`publish_contract`, `close`. For verbs not yet on the SDK (`find`, `get_many`,
`trace`, interactions) call the REST paths above directly, or use `query` for the
counterparty-style pulls.

See also: [contracts-reference.md](contracts-reference.md) (what each contract
declares) · [building-on-phronexus.md](building-on-phronexus.md) (the tutorial) ·
[storage-layouts.md](storage-layouts.md) (physical storage options).
