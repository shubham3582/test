# Phronexus Core — Documentation

| Doc | What it covers |
|---|---|
| [architecture.md](architecture.md) | The mental model, components, the manifest write path, and the state machine — with diagrams. Start here. |
| [building-on-phronexus.md](building-on-phronexus.md) | Developer guide: onboard a new entity by config end-to-end, extend with hooks, use the REST API/SDK, evolve contracts, test. |
| [api-reference.md](api-reference.md) | The verbs at a glance: Python (`Phronexus`), REST endpoints, and the remote SDK. |
| [contracts-reference.md](contracts-reference.md) | Field-by-field reference for all seven contract kinds (storage, query, view, validation, transition, stream, ingress), incl. `temporal`/`valid_time_field`. |
| [messaging.md](messaging.md) | Creating & validating messages with JSON Schema: outbound `stream` (shaped emit), inbound `ingress`, the produce/validate flow, and fetching schemas. |
| [governance.md](governance.md) | **Control plane**: config-driven RBAC, the change→approve→publish workflow (N-of-M + separation of duties), hash-chained history, rollback, environment promotion, COB, backfill control, and evidence export. |
| [bitemporal.md](bitemporal.md) | Bitemporal storage: `temporal: bitemporal`, valid-time + transaction-time, as-of reads, COB defaulting, and late/corrected events. |
| [storage-layouts.md](storage-layouts.md) | Physical storage options: `map`/`msgpack`/`bins` encoding, `bin_map`, `spread`, `native_txn`, and batch reads — with worked layouts. |
| [state-machine.md](state-machine.md) | The transactional state machine in depth: atomicity model, hooks, output routing, the three faces. |
| [scaling.md](scaling.md) | Scaling the stateless runner to 500K TPS+: partitions × replicas, Aerospike sizing, getting the produce path off the consume loop, key-skew hot partitions, and the config knobs. |
| [ccr-reference.md](ccr-reference.md) | End-to-end reference: Counterparty-Credit-Risk saga, the hot value cube, and the exactly-once scheduler — all as config. |
| [ccr-architecture.md](ccr-architecture.md) | Reference architecture: Phronexus as the hot-tier data plane for a fleet of CCR compute services — boundaries, who-owns-what, and the five disciplines. |
| [retention-and-journals.md](retention-and-journals.md) | Insert-only, idempotent retention log (self-reconciling on replay) and the binary msgpack journals for messages and request/response pairs. |
| [resync.md](resync.md) | Date-scoped store resync: rehydrate the hot store (Aerospike) from the cold tier (Iceberg) and vice versa — silent coords-preserving restore, idempotent, contract-driven format, checkpointed/governed. |
| [deployment.md](deployment.md) | Production: Aerospike / MSK / S3 Tables, TLS/mTLS, auth, observability, operations, scaling. |
| [running-on-amazon-linux.md](running-on-amazon-linux.md) | Run the engine on Amazon Linux with **no Docker, air-gapped**, against an existing Aerospike — offline wheelhouse install, headless (no FastAPI) or REST/UI, systemd, config. |
| [logging.md](logging.md) | The one place to change log format/level/fields for the whole platform (structlog). |
| [packaging.md](packaging.md) | Build the wheel, consume it as a library (extras matrix), and produce an offline/air-gapped install bundle. |
| [integration-dishtayantra.md](integration-dishtayantra.md) | Integrating Phronexus into a DishtaYantra DAG as library components. |
| [dishtayantra-vs-phronexus.md](dishtayantra-vs-phronexus.md) | Responsibilities by use case — the compute/orchestration DAG vs the governed data layer, and the overlap zones (orchestration, scheduling, transport) to resolve. |
| [product-positioning.md](product-positioning.md) | *Internal strategy note* — positioning, the moat vs. commodity, and a moat/keep-thin/expose-native/cut scope table. |

## Quick links

- **Run the worked example:** `python examples/bond/run_bond.py`
- **Run the CCR reference:** `python examples/ccr/run_ccr.py` (in-memory by default; `PHRONEXUS_BACKEND=aerospike` runs it against a live stack)
- **Contracts for the example:** [`examples/bond/`](../examples/bond), [`examples/ccr/`](../examples/ccr)
- **Config templates:** [`config/`](../config)
- **Tests (usage patterns):** [`tests/`](../tests)
