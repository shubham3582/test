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
| `get(entity, doc_id) -> doc?` | read one by id |
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
| `validate_event(entity, event_type, payload) -> ValidationReport` | validate an outbound event vs its stream schema |

### Engines & journals
| Verb | Does |
|---|---|
| `state_machine(output=None, hooks=None) -> StateMachine` | transactional state machine (`.process(InputEvent)`) |
| `scheduler(output=None) -> Scheduler` | distributed exactly-once scheduler |
| `request_journal()` · `message_journal()` | msgpack journals — `.read(id)` for request/response or raw messages |
| `native_aerospike()` | supported native-Aerospike accessor (managed-set guard) |
| `close()` | release connections |

---

## REST endpoints

Auth per `api.auth` (none / api_key / bearer / jwt / mtls). Authoritative, live
list: `/openapi.json` (Swagger UI at `/docs`).

| Method & path | Purpose |
|---|---|
| `PUT /entities/{entity}/documents` | write one |
| `POST /entities/{entity}/documents/batch` | bulk write |
| `GET /entities/{entity}/documents/{id}` | read |
| `DELETE /entities/{entity}/documents/{id}` | delete |
| `GET /entities/{entity}/documents/{id}/trace` | audit/debug trace |
| `POST /entities/{entity}/validate` | dry-run validation |
| `POST /entities/{entity}/query?view=` | query (optionally via a view) |
| `POST /entities/{entity}/patterns/{pattern}` | run a named query pattern |
| `GET /entities/{entity}/views/{view}/documents/{id}` | read through a view |
| `POST /entities/{entity}/events` | submit a lifecycle event (sync accept/reject) |
| `GET /interactions/{event_id}` | journaled request + response for an event |
| `GET/PUT/DELETE /schedules` · `POST /schedules/tick` | scheduler admin |
| `POST /contracts` · `GET /contracts[/{id}]` · `POST /contracts/refresh` | contract admin |
| `POST /auth/login` · `GET /healthz` · `GET /readyz` | auth + health |

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
